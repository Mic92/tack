package buildkite

// Tests for the Buildkite REST client + webhook signature/token
// verifiers. Provider-level (Tangled translation) tests live with
// the provider in the main package.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestVerifySignature covers the HMAC mode end to end. The reference
// digest is computed with the documented "<timestamp>.<body>"
// preimage so the test pins the wire format, not just the helper.
func TestVerifySignature(t *testing.T) {
	const secret = "shhh"
	body := []byte(`{"event":"build.finished"}`)
	const ts = "1700000000"

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts))
	mac.Write([]byte("."))
	mac.Write(body)
	good := hex.EncodeToString(mac.Sum(nil))

	cases := []struct {
		name    string
		header  string
		secret  string
		body    []byte
		wantErr bool
	}{
		{"valid", "timestamp=" + ts + ",signature=" + good, secret, body, false},
		{"valid with whitespace", " timestamp=" + ts + " , signature=" + good + " ", secret, body, false},
		{"empty header", "", secret, body, true},
		{"empty server secret", "timestamp=" + ts + ",signature=" + good, "", body, true},
		{"missing timestamp", "signature=" + good, secret, body, true},
		{"missing signature", "timestamp=" + ts, secret, body, true},
		{"non-numeric timestamp", "timestamp=abc,signature=" + good, secret, body, true},
		{"wrong signature", "timestamp=" + ts + ",signature=00", secret, body, true},
		{"wrong body", "timestamp=" + ts + ",signature=" + good, secret, []byte("nope"), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := VerifySignature(c.header, c.secret, c.body)
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
		})
	}
}

// TestVerifyToken pins the token-mode behaviour: it must be a
// constant-time exact-match check, never a prefix or substring.
func TestVerifyToken(t *testing.T) {
	cases := []struct {
		name    string
		header  string
		expect  string
		wantErr bool
	}{
		{"match", "abc123", "abc123", false},
		{"empty header", "", "abc123", true},
		{"empty expected", "abc123", "", true},
		{"mismatch", "abc124", "abc123", true},
		{"prefix is not a match", "abc", "abc123", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := VerifyToken(c.header, c.expect)
			if (err != nil) != c.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, c.wantErr)
			}
		})
	}
}

// TestClientCreateBuild covers the request shape we send (auth
// header, JSON body, URL) and the response decoding for the happy
// path.
func TestClientCreateBuild(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/organizations/myorg/pipelines/mypipe/builds" {
			t.Errorf("bad path %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("bad auth %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("bad content-type %q", got)
		}
		var got CreateBuildRequest
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Commit != "abc" || got.Branch != "main" {
			t.Errorf("bad body: %+v", got)
		}
		if got.MetaData["k"] != "v" {
			t.Errorf("missing meta_data: %+v", got.MetaData)
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(Build{
			ID:     "uuid-1",
			Number: 42,
			State:  "scheduled",
		})
	}))
	defer srv.Close()

	prev := APIBase
	APIBase = srv.URL
	defer func() { APIBase = prev }()

	c := NewClient("tok")
	build, err := c.CreateBuild(context.Background(), "myorg", "mypipe", CreateBuildRequest{
		Commit:   "abc",
		Branch:   "main",
		MetaData: map[string]string{"k": "v"},
	})
	if err != nil {
		t.Fatalf("CreateBuild: %v", err)
	}
	if build.ID != "uuid-1" || build.Number != 42 || build.State != "scheduled" {
		t.Fatalf("unexpected build: %+v", build)
	}
}

// TestClientCreateBuildError makes sure non-2xx responses surface
// the upstream error body — that text ends up in operator logs, so
// silently dropping it would make misconfigurations very painful to
// diagnose.
func TestClientCreateBuildError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		fmt.Fprint(w, `{"message":"branch is required"}`)
	}))
	defer srv.Close()

	prev := APIBase
	APIBase = srv.URL
	defer func() { APIBase = prev }()

	c := NewClient("tok")
	_, err := c.CreateBuild(context.Background(), "myorg", "mypipe", CreateBuildRequest{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "branch is required") {
		t.Fatalf("error missing upstream body: %v", err)
	}
}

// TestClientGetJobLog confirms we send the right Accept header (so
// Buildkite returns plain text, not JSON) and surface 404 as
// ErrNotFound for callers to translate.
func TestClientGetJobLog(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if got := r.Header.Get("Accept"); got != "text/plain" {
				t.Errorf("bad accept %q", got)
			}
			if !strings.HasSuffix(r.URL.Path, "/builds/7/jobs/job-1/log") {
				t.Errorf("bad path %s", r.URL.Path)
			}
			io.WriteString(w, "line1\nline2\n")
		}))
		defer srv.Close()

		prev := APIBase
		APIBase = srv.URL
		defer func() { APIBase = prev }()

		body, err := NewClient("tok").
			GetJobLog(context.Background(), "myorg", "mypipe", 7, "job-1")
		if err != nil {
			t.Fatalf("GetJobLog: %v", err)
		}
		if body != "line1\nline2\n" {
			t.Fatalf("body = %q", body)
		}
	})
	t.Run("404", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "no", http.StatusNotFound)
		}))
		defer srv.Close()
		prev := APIBase
		APIBase = srv.URL
		defer func() { APIBase = prev }()

		_, err := NewClient("tok").
			GetJobLog(context.Background(), "myorg", "mypipe", 7, "job-1")
		if err != ErrNotFound {
			t.Fatalf("err = %v; want ErrNotFound", err)
		}
	})
}
