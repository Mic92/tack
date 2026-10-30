package main

// This file authenticates callers of Tack's XRPC procedures with atproto service auth: a signed JWT
// that one atproto service (a knot, or a user's PDS acting for them) sends to another to prove who
// is calling. It also holds the replay guard that makes each knot push token single use.
//
// Tack previously used tangled.org/core/xrpc/serviceauth from Tangled v1.16, which answers 403 to
// every failure. knot2 drops a push on most 4xx answers (see the notify.go file comment), so a
// brief failure to resolve the knot's did:web document would silently lose CI runs. This
// middleware follows the Tangled v1.20 upstream spindle instead. It runs cheap claim checks first,
// answers 503 when the issuer cannot be resolved, and answers 403 only for failures that are the
// caller's fault.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/auth"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

// serviceAuthLeeway is the clock skew tolerated on token expiry. Middleware uses it for the
// unverified pre-check and also passes it to indigo's validator as TimestampLeeway, so both checks
// agree about a token that expires right at the boundary. The replay guard uses it too, so it
// never forgets a token that the middleware would still accept.
const serviceAuthLeeway = 5 * time.Second

// serviceAuthPrincipal is the verified caller of an authenticated procedure. Handlers read it with
// principalFrom.
type serviceAuthPrincipal struct {
	// Issuer is the DID that signed the token. It is a knot's did:web for pushes, and otherwise the
	// user (or the repository acting for one) that made the request.
	Issuer syntax.DID

	// TokenID and Expires are the token's jti (unique token ID) and exp (expiry) claims. They exist
	// for [replayGuard], which needs both to remember a token exactly as long as it could be
	// replayed. TokenID is empty when the token carries no jti.
	TokenID string
	Expires time.Time
}

// principalCtxKey is the context key under which Middleware stores the verified principal. A
// distinct unexported type cannot collide with a context key set by any other code, such as a
// library the request passes through. Within Tack, only Middleware sets it and only principalFrom
// reads it.
type principalCtxKey struct{}

// principalFrom returns the caller that serviceAuth.Middleware attached to ctx. The boolean is
// false when the request did not pass through the middleware, which handlers treat as
// unauthenticated.
func principalFrom(ctx context.Context) (*serviceAuthPrincipal, bool) {
	principal, ok := ctx.Value(principalCtxKey{}).(*serviceAuthPrincipal)
	return principal, ok
}

// serviceAuth verifies service auth JWTs addressed to this spindle and binds each one to the XRPC
// method it was minted for.
type serviceAuth struct {
	logger *slog.Logger

	// dir resolves issuer DIDs to their signing keys. It should cache, because Middleware looks up
	// each issuer once itself and indigo's validator looks it up again.
	dir identity.Directory

	// audience is Tack's own DID. Tokens whose aud claim does not name it are rejected.
	audience string

	// now is the clock for the expiry pre-check. It is a field rather than a direct time.Now call
	// so tests can pin the clock when exercising expiry boundaries.
	now func() time.Time
}

// newServiceAuth returns a verifier that accepts only tokens whose audience is audience, which
// callers set to Tack's own did:web.
func newServiceAuth(logger *slog.Logger, dir identity.Directory, audience string) *serviceAuth {
	return &serviceAuth{logger: logger, dir: dir, audience: audience, now: time.Now}
}

// unverifiedClaims is the subset of JWT claims Tack reads without checking the signature. Before
// verification they let Middleware reject obviously bad tokens without resolving an issuer the
// attacker chose. After verification they supply jti and exp, which indigo's validator does not
// return.
type unverifiedClaims struct {
	Issuer string `json:"iss"`

	// Audience stays raw because a JWT aud may be a single string or an array of strings. Use
	// audiences to read it.
	Audience json.RawMessage `json:"aud"`

	// Expires is a pointer so a missing exp can be told apart from zero.
	Expires *int64 `json:"exp"`

	ID string `json:"jti"`
}

// audiences returns the aud claim as a list, accepting both the string and array forms that JWT
// permits. A missing or malformed claim yields an empty list, which matches no audience.
func (c *unverifiedClaims) audiences() []string {
	var single string
	if err := json.Unmarshal(c.Audience, &single); err == nil {
		return []string{single}
	}
	var many []string
	_ = json.Unmarshal(c.Audience, &many)
	return many
}

// Middleware wraps next so it only runs for requests carrying a valid service auth token, and
// attaches the verified principal to the request context for principalFrom.
//
// The token's lxm claim (the lexicon method, meaning the XRPC NSID the token authorizes) must name
// the method in the request path, so a token minted for one procedure cannot be spent on another.
// Checks run cheapest first. Expiry, audience, and issuer syntax are read from the unverified
// payload so junk tokens never cost a DID lookup. Every rejection is a 403 except a failed issuer
// lookup, which is a 503 so knot2 keeps the push and retries it later.
func (a *serviceAuth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" {
			writeXRPCError(w, http.StatusForbidden, "AuthRequired", "service auth token is required")
			return
		}
		lxm, err := syntax.ParseNSID(path.Base(r.URL.Path))
		if err != nil {
			writeXRPCError(w, http.StatusForbidden, "Auth", "request path is not an XRPC method")
			return
		}

		claims, err := parseUnverifiedClaims(token)
		if err != nil {
			writeXRPCError(w, http.StatusForbidden, "Auth", err.Error())
			return
		}
		if claims.Expires == nil {
			writeXRPCError(w, http.StatusForbidden, "Auth", "token has no expiry")
			return
		}
		expires := time.Unix(*claims.Expires, 0)
		if a.now().After(expires.Add(serviceAuthLeeway)) {
			writeXRPCError(w, http.StatusForbidden, "Auth", "token is expired")
			return
		}
		if !slices.Contains(claims.audiences(), a.audience) {
			writeXRPCError(w, http.StatusForbidden, "Auth", "token is meant for another audience")
			return
		}
		issuer, err := syntax.ParseDID(claims.Issuer)
		if err != nil {
			writeXRPCError(w, http.StatusForbidden, "Auth", "token issuer is not a DID")
			return
		}

		// Resolve the issuer before validating, because the validator reports a lookup failure
		// as a bad signature and the two need different status codes. The directory caches, so
		// the validator's own lookup below is normally served from memory.
		if _, err := a.dir.LookupDID(r.Context(), issuer); err != nil {
			a.logger.Warn("service auth issuer lookup failed", "iss", issuer, "err", err)
			writeXRPCError(w, http.StatusServiceUnavailable, "IssuerUnavailable",
				fmt.Sprintf("could not resolve token issuer %s", issuer))
			return
		}
		validator := auth.ServiceAuthValidator{
			Audience:        a.audience,
			Dir:             a.dir,
			TimestampLeeway: serviceAuthLeeway,
		}
		if _, err := validator.Validate(r.Context(), token, &lxm); err != nil {
			a.logger.Warn("service auth rejected", "iss", issuer, "lxm", lxm, "err", err)
			writeXRPCError(w, http.StatusForbidden, "Auth", "signature verification failed: "+err.Error())
			return
		}

		principal := &serviceAuthPrincipal{Issuer: issuer, TokenID: claims.ID, Expires: expires}
		next.ServeHTTP(w, r.WithContext(
			context.WithValue(r.Context(), principalCtxKey{}, principal),
		))
	})
}

// parseUnverifiedClaims decodes the payload of a compact JWT (header.payload.signature) without
// checking the signature. Callers must treat the result as untrusted until the validator accepts
// the same token. The error messages are safe to return to the caller.
func parseUnverifiedClaims(token string) (*unverifiedClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, errors.New("token is not a compact JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, errors.New("token payload is not base64url")
	}
	var claims unverifiedClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, errors.New("token payload is not JSON")
	}
	return &claims, nil
}

// replayMaxLifetime caps how far in the future a replay-guarded token may expire. The guard keeps
// no state across restarts, so a long-lived token could be replayed after one. Bounding the
// lifetime bounds that window. knot2 push tokens live 60 seconds, well inside the cap.
const replayMaxLifetime = 5 * time.Minute

// replayCapacity bounds how many tokens the guard remembers at once. knot2 push tokens live 60
// seconds, so even a busy fleet of knots stays far below this.
const replayCapacity = 65536

// These are the reasons replayGuard.Admit refuses a token. Callers answer errReplayFull with 503,
// because a full guard is Tack's problem and the knot should retry. The others are 403.
var (
	errReplayNoID     = errors.New("token has no jti")
	errReplayLifetime = errors.New("token lives too long to track for replay")
	errReplaySeen     = errors.New("token was already used")
	errReplayFull     = errors.New("replay guard is full")
)

// replayGuard accepts each (issuer, jti) pair once until the token expires, so a captured knot push
// token cannot be sent again to start more builds. knot2 mints a fresh jti for every delivery
// attempt, so its retries are never mistaken for replays. A retried push is deduplicated by its
// content instead (see pipelineService.coalescePush and pipelineService.runPush).
type replayGuard struct {
	mu sync.Mutex

	// seen maps "issuer jti" to the token's expiry. Expired entries are swept only when the map
	// reaches replayCapacity, which keeps Admit cheap in the common case.
	seen map[string]time.Time

	now func() time.Time
}

// newReplayGuard returns an empty in-memory guard. Losing its contents on restart is acceptable
// because Admit refuses tokens that live longer than replayMaxLifetime, so a token captured
// before a restart can be replayed after it for at most that long.
func newReplayGuard() *replayGuard {
	return &replayGuard{seen: make(map[string]time.Time), now: time.Now}
}

// Admit records the token behind principal and returns nil if this is its first use. Otherwise it
// returns one of the errReplay errors. A token without a jti, or one that expires further out
// than replayMaxLifetime, is refused because the guard could not protect it. When the guard is at
// capacity, Admit first drops entries past their expiry plus serviceAuthLeeway, and refuses with
// errReplayFull only if that frees no room.
func (g *replayGuard) Admit(principal *serviceAuthPrincipal) error {
	if principal.TokenID == "" {
		return errReplayNoID
	}
	now := g.now()
	if principal.Expires.Sub(now) > replayMaxLifetime {
		return errReplayLifetime
	}
	key := principal.Issuer.String() + " " + principal.TokenID

	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.seen[key]; ok {
		return errReplaySeen
	}
	if len(g.seen) >= replayCapacity {
		for seenKey, expires := range g.seen {
			if now.After(expires.Add(serviceAuthLeeway)) {
				delete(g.seen, seenKey)
			}
		}
		if len(g.seen) >= replayCapacity {
			return errReplayFull
		}
	}
	g.seen[key] = principal.Expires
	return nil
}
