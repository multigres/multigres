// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multipooler

import "testing"

// TestPGClientTLS_VerifyFull is a scaffold for the MUL-370 integration test.
//
// Acceptance from MUL-370:
//   - Multipooler connects to PG with each sslmode value; negotiation matches libpq.
//   - verify-full rejects a server cert whose SAN/CN doesn't match the target hostname.
//   - Integration test with a TLS-enabled PG instance.
//
// What's still missing before this can run:
//
//  1. shardsetup.WithMultipoolerPGTLS() option that:
//     a. Generates a CA + server cert (CN=localhost, SAN=localhost) using
//     provisioner/local.GenerateCA / GenerateCert (already used by the
//     multigateway TLS test).
//     b. Writes a postgresql.conf snippet enabling SSL:
//     ssl = on
//     ssl_cert_file = '<server.crt>'
//     ssl_key_file  = '<server.key>'
//     ssl_ca_file   = '<ca.crt>'
//     c. Passes the snippet to pgctld via --pg-initdb-extra-conf so postgres
//     starts with TLS enabled. Cert+key files must be readable by the
//     postgres process (mode 0600 owned by the running user).
//     d. Adds --pg-client-sslmode=verify-full and --pg-client-sslrootcert=<ca>
//     to the multipooler argv.
//
//  2. The Unix-socket short-circuit in client/startup.go means the multipooler
//     still has to dial postgres over TCP for TLS to be exercised. The fixture
//     currently uses Unix sockets — switch this test to TCP via Host/Port on
//     ConnectionConfig.
//
// Once those are in place, this test should:
//
//   - Boot the cluster with WithMultipoolerPGTLS().
//   - Issue a SELECT 1 through the multipooler; assert success.
//   - Inspect pg_stat_ssl on the postgres side to confirm the connection used
//     SSL (ssl=true and the expected cipher).
//   - Cover negative cases per sslmode: verify-full against a cert whose SAN
//     doesn't match the host should fail at handshake; require should
//     succeed; disable should round-trip plaintext.
func TestPGClientTLS_VerifyFull(t *testing.T) {
	t.Skip("MUL-370 follow-up: needs shardsetup WithMultipoolerPGTLS() option (PG-side TLS provisioning)")
}
