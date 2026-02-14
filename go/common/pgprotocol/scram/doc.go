// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package scram implements SCRAM-SHA-256 authentication for PostgreSQL protocol connections.
//
// # Overview
//
// This package provides SCRAM-SHA-256 authentication for both server and client roles,
// enabling Multigres to verify client credentials and extract SCRAM keys for passthrough
// authentication to backend PostgreSQL servers. This eliminates the need to store plaintext
// passwords while maintaining compatibility with PostgreSQL's native authentication.
//
// # SCRAM-SHA-256 Protocol
//
// SCRAM (Salted Challenge Response Authentication Mechanism) is defined in RFC 5802:
// https://datatracker.ietf.org/doc/html/rfc5802
//
// PostgreSQL's SCRAM-SHA-256 implementation is documented at:
// https://www.postgresql.org/docs/current/sasl-authentication.html
//
// The protocol involves a three-message exchange:
//  1. Client → Server: client-first-message (username, nonce)
//  2. Server → Client: server-first-message (combined nonce, salt, iterations)
//  3. Client → Server: client-final-message (proof)
//  4. Server → Client: server-final-message (server signature for mutual auth)
//
// # Why Not Use an Existing Library?
//
// Several Go SCRAM libraries exist (xdg-go/scram, lib/pq, jackc/pgx), but none support
// our critical requirement: ClientKey extraction for passthrough authentication.
//
// Existing libraries:
//   - xdg-go/scram: Most comprehensive, but lacks ClientKey extraction and context.Context support
//   - lib/pq: Maintenance mode, client-side only
//   - jackc/pgx: Client library, no server-side SCRAM
//
// Our implementation adds:
//   - ExtractAndVerifyClientProof: Recovers ClientKey from client's proof for passthrough auth
//   - context.Context support: Allows timeout/cancellation during credential lookup
//   - Minimum security thresholds: Enforces 4096+ iterations and 8+ byte salts
//
// These features enable Multigres to verify clients and reuse extracted keys to
// authenticate to backend PostgreSQL servers without storing plaintext passwords.
//
// # Architecture
//
// The package is organized into several components:
//
//   - ScramAuthenticator: Stateful server-side authenticator handling the protocol exchange
//   - SCRAMClient: Client-side authenticator supporting password and passthrough modes
//   - PasswordHashProvider: Interface for retrieving SCRAM password hashes from storage
//   - Cryptographic functions: RFC 5802 compliant key derivation and verification
//   - Protocol parsers/generators: Message construction and parsing (unexported)
//
// # Usage Example
//
//	// Server-side authentication
//	provider := NewMyPasswordProvider()
//	auth := scram.NewScramAuthenticator(provider, "mydb")
//
//	// Start SASL negotiation
//	mechanisms := auth.StartAuthentication()
//	// Send AuthenticationSASL with mechanisms...
//
//	// Handle client-first-message
//	serverFirst, err := auth.HandleClientFirst(ctx, clientFirstMsg)
//	// Send AuthenticationSASLContinue with serverFirst...
//
//	// Handle client-final-message
//	serverFinal, err := auth.HandleClientFinal(clientFinalMsg)
//	if auth.IsAuthenticated() {
//	    // Authentication successful
//	    clientKey, serverKey := auth.ExtractedKeys()
//	    // Use keys for passthrough authentication...
//	}
//
// # Key Passthrough Authentication
//
// A critical feature of this implementation is extracting the ClientKey from the client's
// proof during authentication. This enables SCRAM passthrough:
//
//  1. Client authenticates to Multigres multigateway
//  2. Multigateway extracts ClientKey from the authentication proof
//  3. Multigateway uses ClientKey to authenticate to PostgreSQL as that user
//  4. No plaintext password needed at any stage
//
// This is possible because SCRAM proofs reveal the ClientKey through XOR:
//
//	ClientKey = ClientProof XOR ClientSignature
//
// The extracted ClientKey can then be used with the stored ServerKey to perform
// subsequent SCRAM authentications to backend PostgreSQL servers without knowing
// the original password.
//
// # Password Hash Storage
//
// This package expects password hashes in PostgreSQL's SCRAM-SHA-256 format:
//
//	SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>
//
// The PasswordHashProvider interface abstracts the storage mechanism:
//
//	type PasswordHashProvider interface {
//	    GetPasswordHash(ctx context.Context, username, database string) (*ScramHash, error)
//	}
//
// Implementations can:
//   - Query PostgreSQL's pg_authid directly
//   - Use a credential cache with TTL and invalidation
//   - Fetch from a centralized credential service
//   - Combine multiple sources with fallback logic
//
// # Future Directions
//
// Current implementation:
//   - Core SCRAM protocol and cryptography
//   - Server-side authentication (ScramAuthenticator)
//   - Client-side authentication with passthrough support (SCRAMClient)
//   - No credential caching
//   - No integration with multigateway/multipooler
//
// Planned enhancements:
//   - Caching password hashes in multigateway to save a round-trip to multipooler on connect
//   - Integration with multigateway/multipooler for end-to-end passthrough
//
// # Credential Cache Design Considerations
//
// When implementing credential caching:
//
//   - TTL: Balance security (shorter) vs performance (longer)
//
//     PgBouncer: No cache, runs auth_query on every connection
//     See src/client.c start_auth_query()
//     https://github.com/pgbouncer/pgbouncer/tree/master/src
//
//     Supavisor: 24-hour cache with 15-second background refresh + refresh on auth failure
//     See lib/supavisor/secret_cache.ex @default_secrets_ttl, fetch_validation_secrets/3
//     See lib/supavisor/secret_checker.ex @interval, check_secrets/2 (background polling)
//     See lib/supavisor/client_handler/auth.ex check_and_update_secrets/7 (refresh on auth failure)
//     https://github.com/supabase/supavisor/tree/main/lib/supavisor
//
//   - Invalidation: Consider cache busting on password changes
//     Option 1: PostgreSQL triggers + notification channel
//     Option 2: Periodic refresh on access
//     Option 3: External invalidation API
//
// # Security Considerations
//
//   - All password hash comparisons use constant-time algorithms (crypto/subtle)
//   - Nonce validation prevents replay attacks
//   - State machine prevents protocol violations
//   - No plaintext passwords stored or logged
//   - ClientKey extraction requires successful authentication
//
// # Password Normalization (SASLprep)
//
// This implementation includes SASLprep password normalization (RFC 4013) for full
// PostgreSQL compatibility. SASLprep applies NFKC Unicode normalization and character
// mapping to passwords before hashing.
//
// Key behaviors:
//   - Non-ASCII spaces normalized to ASCII space (U+0020)
//   - Soft hyphens and zero-width characters removed
//   - Unicode combining characters normalized
//   - Fallback to raw password on normalization failure (prohibited chars, bidi violations)
//
// This matches PostgreSQL's lenient approach: passwords that fail SASLprep validation
// (invalid UTF-8, prohibited characters, bidirectional check failures) are accepted
// using their raw byte representation.
//
// # Compatibility
//
// This implementation is compatible with:
//   - PostgreSQL 10+ SCRAM-SHA-256 authentication
//   - Standard PostgreSQL client libraries (psql, libpq, pgx, etc.)
//   - PostgreSQL's pg_authid password hash format
//   - PostgreSQL's SASLprep implementation (RFC 4013)
//
// Not currently supported:
//   - SCRAM-SHA-1 (deprecated, not used by PostgreSQL)
//   - Channel binding (SCRAM-SHA-256-PLUS)
//   - Custom iteration counts (uses hash's iteration count)
//
// # References
//
//   - RFC 5802 (SCRAM): https://datatracker.ietf.org/doc/html/rfc5802
//   - PostgreSQL SASL: https://www.postgresql.org/docs/current/sasl-authentication.html
//   - PgBouncer auth: https://www.pgbouncer.org/config.html#authentication-settings
//   - Supavisor: https://github.com/supabase/supavisor
package scram
