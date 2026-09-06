// Copyright 2026 Supabase, Inc.
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

package servenv

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/spf13/pflag"
	"google.golang.org/grpc/metadata"

	"github.com/multigres/multigres/go/tools/retry"
)

// jwtSigningMethods are the only signing algorithms accepted for JWT
// verification. This is hardcoded (not configurable) to prevent
// algorithm-confusion attacks - in particular, allowing a symmetric
// algorithm like HS256 here would let an attacker forge tokens signed with
// the (public) RSA/EC verification key treated as an HMAC secret.
var jwtSigningMethods = []string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512"}

// jwtClockSkewLeeway is the tolerance applied to exp/nbf/iat checks. Not
// configurable, to keep the flag surface minimal; revisit only if a real
// deployment needs it tuned.
const jwtClockSkewLeeway = 60 * time.Second

// jwtJWKSFetchTimeout bounds a single JWKS fetch attempt performed during
// plugin initialization, so one hung request can't stall a retry attempt
// indefinitely.
const jwtJWKSFetchTimeout = 10 * time.Second

// jwtJWKSStartupRetryBudget bounds the total time spent retrying the initial
// JWKS fetch (see jwtAuthPluginInitializer) before giving up and failing
// startup. Rides out a brief blip at the issuer (a restart, a DNS/network
// hiccup) without turning it into a full Multiadmin outage, while still
// eventually failing loudly for a genuinely broken configuration (wrong
// URL, issuer permanently down).
//
// jwtJWKSRetryBaseDelay/jwtJWKSRetryMaxDelay configure the exponential
// backoff (with full jitter) between retries. All three are vars, not
// consts - not flags (deliberately not user-configurable, same reasoning as
// jwtClockSkewLeeway), but tests shrink them so the "unreachable JWKS"
// failure path doesn't take a real 60s to run.
var (
	jwtJWKSStartupRetryBudget = 60 * time.Second
	jwtJWKSRetryBaseDelay     = 1 * time.Second
	jwtJWKSRetryMaxDelay      = 10 * time.Second
)

var (
	// jwtIssuer is the expected `iss` claim. Required when the jwt auth
	// plugin is active.
	jwtIssuer string
	// jwtJWKSURI is the URI of the issuer's JWKS endpoint, used to fetch and
	// cache the public keys used to verify token signatures. Required when
	// the jwt auth plugin is active.
	jwtJWKSURI string
	// jwtAllowedSubs is the list of authorized `sub` claim values, one per
	// --grpc-auth-jwt-allowed-subs occurrence (a repeatable flag, not a
	// delimited string - `sub` claims are frequently URI/ARN-shaped and
	// commonly contain colons themselves, e.g. AWS IAM role ARNs like
	// arn:aws:iam::123456789012:role/x, which a colon-joined flag value
	// could not represent). Optional: unlike mTLS's allowed-substrings list,
	// an empty value here is a legitimate configuration ("any valid token
	// from the trusted issuer is authorized"), since a JWT's core guarantee
	// (signature + issuer + expiry) doesn't share mTLS's "empty substring
	// matches everything" failure mode.
	jwtAllowedSubs []string
	// jwtAudience is the expected `aud` claim. Optional: only enforced if
	// set. Without this, any valid token from a shared issuer - even one
	// minted for a different, unrelated application - would be accepted as
	// long as `iss` (and, if configured, `sub`) match.
	jwtAudience string

	// TokenVerifier is implemented by JWTAuthPlugin.
	_ TokenVerifier = (*JWTAuthPlugin)(nil)
	// Authenticator is implemented by JWTAuthPlugin.
	_ Authenticator = (*JWTAuthPlugin)(nil)
)

// TokenVerifier verifies a raw bearer token string and returns its claims.
// It is deliberately narrower than Authenticator (which is generic across
// whatever auth mode is configured, e.g. mtls) so that callers outside the
// gRPC interceptor path - such as an HTTP handler with no gRPC context to
// build - can verify a token directly, without needing to fabricate a fake
// gRPC context just to reuse Authenticate. Callers should type-assert an
// Authenticator to TokenVerifier and treat a failed assertion as "no JWT
// verification available" rather than an error, since the active plugin may
// legitimately be something else (e.g. mtls) or none at all.
type TokenVerifier interface {
	VerifyToken(tokenString string) (jwt.MapClaims, error)
}

// JWTAuthPlugin implements JWT bearer-token authentication. A caller is
// authorized if it presents a token signed by the configured issuer's JWKS,
// not expired, matching the configured issuer/audience/subject constraints.
type JWTAuthPlugin struct {
	keyfunc     keyfunc.Keyfunc
	issuer      string
	audience    string
	allowedSubs map[string]struct{}
	metrics     *authMetrics
}

func registerGRPCServerAuthJWTFlags(fs *pflag.FlagSet) {
	fs.StringVar(&jwtIssuer, "grpc-auth-jwt-issuer", jwtIssuer, "Expected `iss` claim for JWT auth. Required when --grpc-auth-mode=jwt.")
	fs.StringVar(&jwtJWKSURI, "grpc-auth-jwt-jwks-uri", jwtJWKSURI, "URI of the issuer's JWKS endpoint, used to verify JWT signatures. Required when --grpc-auth-mode=jwt.")
	fs.StringArrayVar(&jwtAllowedSubs, "grpc-auth-jwt-allowed-subs", jwtAllowedSubs, "Authorized `sub` claim value (specify multiple times for multiple subjects). If unset, any subject is authorized. Note: JWT verification has no revocation mechanism beyond token expiry.")
	fs.StringVar(&jwtAudience, "grpc-auth-jwt-audience", jwtAudience, "Expected `aud` claim for JWT auth. If empty, audience is not checked.")
}

// VerifyToken implements TokenVerifier. This is the single point both
// Authenticate (below, gRPC) and AuthenticateBearer (http_auth.go, HTTP and
// Connect) call to check a token, so recording metrics here - rather than
// separately in each caller - covers every transport in one place. There's
// no context.Context available (TokenVerifier's signature doesn't take one,
// and widening it would ripple across every call site for a metrics-only
// need), so context.TODO() is used; that only affects exemplar/trace
// correlation, not correctness.
func (p *JWTAuthPlugin) VerifyToken(tokenString string) (jwt.MapClaims, error) {
	opts := []jwt.ParserOption{
		jwt.WithValidMethods(jwtSigningMethods),
		jwt.WithIssuer(p.issuer),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(jwtClockSkewLeeway),
	}
	if p.audience != "" {
		opts = append(opts, jwt.WithAudience(p.audience))
	}

	claims := jwt.MapClaims{}
	if _, err := jwt.ParseWithClaims(tokenString, claims, p.keyfunc.Keyfunc, opts...); err != nil {
		p.metrics.record(context.TODO(), "jwt", AuthOutcomeInvalidToken)
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	if len(p.allowedSubs) > 0 {
		sub, err := claims.GetSubject()
		if err != nil {
			p.metrics.record(context.TODO(), "jwt", AuthOutcomeInvalidToken)
			return nil, fmt.Errorf("token has no valid sub claim: %w", err)
		}
		if _, ok := p.allowedSubs[sub]; !ok {
			p.metrics.record(context.TODO(), "jwt", AuthOutcomeSubjectNotAuthorized)
			return nil, fmt.Errorf("subject %q is not authorized", sub)
		}
	}

	p.metrics.record(context.TODO(), "jwt", AuthOutcomeSuccess)
	return claims, nil
}

// Authenticate implements Authenticator. This method will be used inside a
// middleware in grpc_server to authenticate incoming requests. Only the
// branches exclusive to this method (no metadata / header at all, present
// but not bearer-shaped) record their own outcome - VerifyToken below
// records success/invalid_token/subject_not_authorized itself, since it's
// also reachable independently from AuthenticateBearer (http_auth.go).
func (p *JWTAuthPlugin) Authenticate(ctx context.Context, fullMethod string) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		slog.WarnContext(ctx, "jwt auth: rejected request", "method", fullMethod, "reason", "no metadata in request")
		p.metrics.record(ctx, "jwt", AuthOutcomeNoMetadata)
		return nil, errGRPCAuthFailed
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		slog.WarnContext(ctx, "jwt auth: rejected request", "method", fullMethod, "reason", "no authorization header provided")
		p.metrics.record(ctx, "jwt", AuthOutcomeMissingHeader)
		return nil, errGRPCAuthFailed
	}
	token, err := ExtractBearerToken(values[0])
	if err != nil {
		slog.WarnContext(ctx, "jwt auth: rejected request", "method", fullMethod, "reason", err)
		p.metrics.record(ctx, "jwt", AuthOutcomeMalformedHeader)
		return nil, errGRPCAuthFailed
	}
	if _, err := p.VerifyToken(token); err != nil {
		slog.WarnContext(ctx, "jwt auth: rejected request", "method", fullMethod, "reason", err)
		return nil, errGRPCAuthFailed
	}
	return ctx, nil
}

func jwtAuthPluginInitializer() (Authenticator, error) {
	if jwtIssuer == "" {
		return nil, errors.New("--grpc-auth-jwt-issuer must be set when --grpc-auth-mode=jwt")
	}
	if jwtJWKSURI == "" {
		return nil, errors.New("--grpc-auth-jwt-jwks-uri must be set when --grpc-auth-mode=jwt")
	}

	if slices.Contains(jwtAllowedSubs, "") {
		return nil, fmt.Errorf("--grpc-auth-jwt-allowed-subs must not contain empty entries, got %q", jwtAllowedSubs)
	}
	allowedSubs := make(map[string]struct{}, len(jwtAllowedSubs))
	for _, s := range jwtAllowedSubs {
		allowedSubs[s] = struct{}{}
	}

	// Fail closed: if the JWKS endpoint is still unreachable once the retry
	// budget below is exhausted, startup fails rather than silently starting
	// with no usable keys. keyfunc.NewDefaultCtx (and the jwkset default HTTP
	// client it wraps) default NoErrorReturnFirstHTTPReq to true (fail OPEN on
	// first fetch failure), so that convenience constructor is deliberately
	// avoided here in favor of NewDefaultOverrideCtx with an explicit
	// override. The background refresh goroutine this launches is tied to
	// context.Background() (i.e. process lifetime) by design, since the
	// plugin registry has no shutdown hook to cancel it earlier.
	noErrorReturnFirstHTTPReq := false

	// A single unreachable-on-first-try JWKS endpoint used to fail startup
	// immediately, turning a transient blip at the issuer (briefly
	// restarting, a DNS/network hiccup right as this process boots) into a
	// full Multiadmin outage. Retry the initial fetch with backoff, bounded
	// by jwtJWKSStartupRetryBudget, before giving up - this is safe to block
	// on: run.go starts serving HTTP (including /live and /ready) before
	// Create() (and therefore this) ever runs, so a slow-but-eventually-
	// successful JWKS fetch here no longer risks a startup-probe deadlock.
	//nolint:gocritic // legitimate entry point: jwtAuthPluginInitializer has no caller-supplied context, this is the root of the retry budget's timeout tree
	budgetCtx, cancel := context.WithTimeout(context.Background(), jwtJWKSStartupRetryBudget)
	defer cancel()
	var kf keyfunc.Keyfunc
	var err error
	for attempt, retryErr := range retry.New(jwtJWKSRetryBaseDelay, jwtJWKSRetryMaxDelay).Attempts(budgetCtx) {
		if retryErr != nil {
			return nil, fmt.Errorf("failed to initialize JWKS client for %q: giving up after %d attempt(s) over %s: %w", jwtJWKSURI, attempt, jwtJWKSStartupRetryBudget, err)
		}
		//nolint:gocritic // intentionally tied to process lifetime, see comment above: no shutdown hook exists in the plugin registry to cancel this earlier
		kf, err = keyfunc.NewDefaultOverrideCtx(context.Background(), []string{jwtJWKSURI}, keyfunc.Override{
			HTTPTimeout:               jwtJWKSFetchTimeout,
			NoErrorReturnFirstHTTPReq: &noErrorReturnFirstHTTPReq,
		})
		if err == nil {
			break
		}
		slog.Warn("jwt auth: JWKS fetch failed, retrying with backoff", "attempt", attempt, "jwks_uri", jwtJWKSURI, "error", err)
	}
	// Unreachable: the loop above only exits via the early return (retry
	// budget exhausted) or this break (err == nil, kf populated).

	slog.Info("jwt auth plugin initialized successfully", "issuer", jwtIssuer, "jwks_uri", jwtJWKSURI, "allowed_subs", jwtAllowedSubs, "audience", jwtAudience)
	return &JWTAuthPlugin{
		keyfunc:     kf,
		issuer:      jwtIssuer,
		audience:    jwtAudience,
		allowedSubs: allowedSubs,
		metrics:     newAuthMetrics(),
	}, nil
}

func init() {
	if err := RegisterAuthPlugin("jwt", jwtAuthPluginInitializer); err != nil {
		slog.Error("failed to register jwt auth plugin", "error", err)
		panic(fmt.Sprintf("failed to register jwt auth plugin: %v", err))
	}
	grpcAuthServerFlagHooks = append(grpcAuthServerFlagHooks, registerGRPCServerAuthJWTFlags)
}
