package main

// This file grants browsers access to the /xrpc routes.
//
// Since Tangled v1.20, the Tangled web app calls spindles directly from the user's browser instead
// of from a server. Page navigation and polling fetch queryPipelines and getPipeline, the logs view
// opens a WebSocket to subscribePipelineLogs, and the retry and approve buttons POST with an
// Authorization header. CORS (Cross-Origin Resource Sharing) is the browser rule that hides a
// response from a page on another origin unless the server opts in with Access-Control-* headers.
// A request with an Authorization header also triggers a preflight: the browser first sends an
// OPTIONS request and only sends the real one if the answer allows it. Without the headers set
// here, the CI views render empty even though the same requests succeed from a server.

import (
	"net/http"
	"net/url"
	"strings"
)

// corsPolicy is the set of browser origins allowed to call /xrpc. An origin is the scheme, host,
// and port of the calling page, as sent in the Origin request header (for example
// "https://tangled.org"). The zero value allows no origin, which turns CORS off.
type corsPolicy struct {
	// allowAll is set when the configuration contains "*". It makes responses carry a literal
	// "*" instead of echoing the caller's origin.
	allowAll bool

	// origins holds the allowed origins, lowercased and without a trailing slash.
	origins map[string]struct{}
}

// newCORSPolicy builds a policy from configured origins (TACK_CORS_ORIGINS). Each entry is trimmed,
// lowercased, and stripped of trailing slashes so it compares equal to what browsers send, and
// empty entries are ignored.
//
// The entry "*" allows every origin. That is safe because no route relies on credentials the
// browser attaches on its own, such as cookies. Every procedure that changes state authenticates
// with a service auth token that the calling page must place in the Authorization header itself, so
// a foreign page cannot borrow a user's identity by calling Tack.
func newCORSPolicy(origins []string) corsPolicy {
	policy := corsPolicy{origins: make(map[string]struct{}, len(origins))}
	for _, origin := range origins {
		origin = strings.TrimRight(strings.ToLower(strings.TrimSpace(origin)), "/")
		switch origin {
		case "":
		case "*":
			policy.allowAll = true
		default:
			policy.origins[origin] = struct{}{}
		}
	}
	return policy
}

// allows reports whether a request carrying the given Origin header value may call /xrpc. An empty
// origin is never allowed, because a request without one did not come from a cross-origin browser
// page and needs no CORS headers.
func (c corsPolicy) allows(origin string) bool {
	if origin == "" {
		return false
	}
	if c.allowAll {
		return true
	}
	_, ok := c.origins[strings.ToLower(origin)]
	return ok
}

// Middleware adds CORS headers to every /xrpc response from an allowed origin and answers
// preflight requests itself. Other paths pass through untouched.
//
// The headers are set before next runs so that error responses carry them too. Without them the
// browser hides the error body from the web app, which then cannot tell a 404 from a network
// failure.
//
// The middleware wraps the whole ServeMux rather than being registered as a route because every
// /xrpc route uses a method-qualified pattern ("GET /xrpc/..."). The mux would answer an OPTIONS
// preflight for those paths with 405 Method Not Allowed, and the browser would then drop the real
// request. A preflight from an unlisted origin still gets 204 No Content, but without
// Access-Control-* headers, which tells the browser to refuse the real request.
//
// Max-Age lets the browser cache a preflight answer for a day. The allowed methods and headers
// match what upstream's Go spindle sends.
func (c corsPolicy) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/xrpc/") {
			next.ServeHTTP(w, r)
			return
		}
		origin := r.Header.Get("Origin")
		allowed := c.allows(origin)
		if allowed {
			header := w.Header()
			header.Add("Vary", "Origin")
			if c.allowAll {
				header.Set("Access-Control-Allow-Origin", "*")
			} else {
				header.Set("Access-Control-Allow-Origin", origin)
			}
		}
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			if allowed {
				header := w.Header()
				header.Set("Access-Control-Allow-Methods", "GET, POST")
				header.Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
				header.Set("Access-Control-Max-Age", "86400")
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// CheckOrigin decides whether a WebSocket upgrade for the logs stream may proceed. It is installed
// as the gorilla/websocket Upgrader's CheckOrigin hook.
//
// CORS does not apply to WebSocket handshakes. Browsers always send an Origin header on them but
// open the connection whatever the server's Access-Control-* headers say, so this check is the
// only thing that keeps unlisted origins out. Allowed origins pass. So do requests without an
// Origin header (server-side clients such as upstream's cilogs SSH log viewer) and pages whose
// origin host equals the request's Host, which is the gorilla default this hook replaces. An Origin
// that does not parse as a URL is rejected.
func (c corsPolicy) CheckOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" || c.allows(origin) {
		return true
	}
	parsed, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return strings.EqualFold(parsed.Host, r.Host)
}
