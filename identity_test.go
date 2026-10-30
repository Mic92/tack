package main

// These tests cover Tack's did:web DID document and the org.tangled.spindle.describeSpindle
// XRPC method. The DID document checks encode the rules knot2 (the Rust knot shipped with
// Tangled v1.20) applies before it delivers a notifyEvent call to a spindle. Those rules live
// in spindle_endpoint and spindle_xrpc_url in knot2/crates/knot-atproto/src/{resolve,lib}.rs.
// The document id must equal the DID named in the repo manifest exactly. The document must
// hold a service whose id ends in "#tangled_spindle" and whose type is "TangledSpindle". That
// service's endpoint must be an https URL, because knot2 refuses to call a spindle over plain
// http.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/identity"

	"go.mitchellh.com/tack/internal/orgtangled"
)

// fetchDIDDocument requests the DID document from a full Tack HTTP handler built from cfg and
// key, and returns both the raw response and the decoded document. Going through
// newHTTPHandler rather than calling didDocumentHandler directly also proves the document is
// mounted at didDocumentPath, which is where knot2 fetches it. A key of nil means no spindle
// key is configured. The test fails if the body is not a JSON document.
func fetchDIDDocument(t *testing.T, cfg config, key atcrypto.PublicKey) (*httptest.ResponseRecorder, didDocument) {
	t.Helper()
	st := newTestStore(t)
	handler := newHTTPHandler(slog.Default(), cfg, st, &stubProvider{}, nil,
		newPipelineService(t.Context(), cfg, st, newBroker(st), &stubProvider{}),
		identity.NewMockDirectory(), key)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, didDocumentPath, nil))
	var doc didDocument
	if err := json.Unmarshal(response.Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode %s: %v", response.Body, err)
	}
	return response, doc
}

// TestDIDDocumentSatisfiesKnot2 pins what knot2 reads before it delivers an
// org.tangled.event.notifyEvent call: an id equal to Tack's DID and a TangledSpindle service
// with an https endpoint. Without this document knot2 logs "DID document fetch returned HTTP
// 404" and push-triggered CI never reaches Tack. The test also pins the application/did+json
// content type that did:web resolvers expect, exactly one service entry, and no verification
// method when no key is configured.
func TestDIDDocumentSatisfiesKnot2(t *testing.T) {
	response, doc := fetchDIDDocument(t, config{Hostname: "tack.mitchellh.com", OwnerDID: testOwnerDID}, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	if got := response.Header().Get("Content-Type"); got != "application/did+json" {
		t.Fatalf("content type = %q", got)
	}
	if doc.DID != "did:web:tack.mitchellh.com" {
		t.Fatalf("id = %q", doc.DID)
	}
	if len(doc.Service) != 1 {
		t.Fatalf("services = %+v", doc.Service)
	}
	service := doc.Service[0]
	if !strings.HasSuffix(service.ID, "#tangled_spindle") || service.Type != "TangledSpindle" ||
		service.ServiceEndpoint != "https://tack.mitchellh.com" {
		t.Fatalf("service = %+v", service)
	}
	if len(doc.VerificationMethod) != 0 {
		t.Fatalf("keyless document published keys: %+v", doc.VerificationMethod)
	}

	// knot2 is not the only possible reader. Go atproto tools resolve DID documents with indigo,
	// which keys services by the id fragment without its "#", so indigo must find the same
	// service under "tangled_spindle".
	parsed := identity.ParseIdentity(&doc.DIDDocument)
	if endpoint := parsed.Services["tangled_spindle"]; endpoint.Type != "TangledSpindle" ||
		endpoint.URL != "https://tack.mitchellh.com" {
		t.Fatalf("parsed service = %+v", endpoint)
	}
}

// TestDIDDocumentPercentEncodesPort pins how a hostname with a port becomes a DID. did:web
// writes the port separator as "%3A", while the service endpoint keeps a literal colon. The
// DID comes from config.DID, which calls Tangled's serviceauth.DidWeb, the same function that
// produces the audience Tack requires in service auth tokens (the signed JWTs that callers send
// to prove who they are). Keeping both paths on one function means the document id, the
// manifest's spindle field, and token audiences match byte for byte. knot2 rejects a document
// whose id differs from the requested DID in any byte.
func TestDIDDocumentPercentEncodesPort(t *testing.T) {
	_, doc := fetchDIDDocument(t, config{Hostname: "localhost:8443", OwnerDID: testOwnerDID}, nil)
	if doc.DID != "did:web:localhost%3A8443" || doc.Service[0].ServiceEndpoint != "https://localhost:8443" {
		t.Fatalf("document = %+v", doc)
	}
}

// writeTestKey generates an ECDSA private key on curve and writes it to a PEM file in a
// temporary directory. If pkcs8 is true the key is written as a PKCS#8 "PRIVATE KEY" block,
// and otherwise as a SEC1 "EC PRIVATE KEY" block. It returns the file path and the private key
// so a test can compare what Tack publishes against the original.
func writeTestKey(t *testing.T, curve elliptic.Curve, pkcs8 bool) (string, *ecdsa.PrivateKey) {
	t.Helper()
	private, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var block *pem.Block
	if pkcs8 {
		der, err := x509.MarshalPKCS8PrivateKey(private)
		if err != nil {
			t.Fatal(err)
		}
		block = &pem.Block{Type: "PRIVATE KEY", Bytes: der}
	} else {
		der, err := x509.MarshalECPrivateKey(private)
		if err != nil {
			t.Fatal(err)
		}
		block = &pem.Block{Type: "EC PRIVATE KEY", Bytes: der}
	}
	path := filepath.Join(t.TempDir(), "spindle.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	return path, private
}

// TestDIDDocumentPublishesConfiguredKey pins the optional verification method that makes
// Tack's document match the upstream Tangled spindle's. For both PEM encodings the upstream
// spindle accepts (SEC1 and PKCS#8), the document must gain the multikey JSON-LD context and
// one "#atproto" Multikey method controlled by Tack's DID. indigo must then decode that method
// to exactly the configured public key, so an operator can reuse an upstream spindle key file
// and get the same published identity.
func TestDIDDocumentPublishesConfiguredKey(t *testing.T) {
	for _, pkcs8 := range []bool{false, true} {
		path, private := writeTestKey(t, elliptic.P256(), pkcs8)
		key, err := loadSpindleKey(path)
		if err != nil {
			t.Fatalf("pkcs8=%v: %v", pkcs8, err)
		}
		_, doc := fetchDIDDocument(t, config{Hostname: testSpindleHost, OwnerDID: testOwnerDID}, key)
		if len(doc.Context) != 2 || len(doc.VerificationMethod) != 1 {
			t.Fatalf("document = %+v", doc)
		}
		method := doc.VerificationMethod[0]
		if method.ID != testSpindleDID+"#atproto" || method.Type != "Multikey" ||
			method.Controller != testSpindleDID {
			t.Fatalf("verification method = %+v", method)
		}
		parsed := identity.ParseIdentity(&doc.DIDDocument)
		published, err := parsed.PublicKey()
		if err != nil {
			t.Fatal(err)
		}
		raw, err := private.PublicKey.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		expected, err := atcrypto.ParsePublicUncompressedBytesP256(raw)
		if err != nil {
			t.Fatal(err)
		}
		if !published.Equal(expected) {
			t.Fatal("published key does not match the configured key")
		}
	}
}

// TestLoadSpindleKeyRejectsBadKeys pins loadSpindleKey's error handling, so a misconfigured
// key stops Tack at startup instead of publishing a document with a wrong or missing key. An
// empty path is the one accepted case and means no key. A missing file must fail with the
// openssl command that generates a key, a key on a curve other than P-256 must name the
// expected curve, and a file that is not PEM must fail.
func TestLoadSpindleKeyRejectsBadKeys(t *testing.T) {
	if key, err := loadSpindleKey(""); err != nil || key != nil {
		t.Fatalf("empty path = %v, %v; want no key", key, err)
	}
	if _, err := loadSpindleKey(filepath.Join(t.TempDir(), "missing.pem")); err == nil ||
		!strings.Contains(err.Error(), "openssl ecparam") {
		t.Fatalf("missing key error = %v; want the openssl hint", err)
	}
	path, _ := writeTestKey(t, elliptic.P384(), false)
	if _, err := loadSpindleKey(path); err == nil || !strings.Contains(err.Error(), "want P-256") {
		t.Fatalf("P-384 key error = %v", err)
	}
	garbage := filepath.Join(t.TempDir(), "garbage.pem")
	if err := os.WriteFile(garbage, []byte("not pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSpindleKey(garbage); err == nil {
		t.Fatal("non-PEM key loaded")
	}
}

// TestDescribeSpindle pins the describeSpindle output: Tack's DID, a non-empty version, and
// exactly the "tangled1.0" capability. The describeSpindle lexicon treats a spindle that omits
// capabilities as legacy (Tangled v1.16 era), so this capability is what marks Tack as a
// current (Tangled v1.20) spindle. The fixture configures no contact email, so the contact
// field must be absent rather than an empty object.
func TestDescribeSpindle(t *testing.T) {
	f := newOrgFixture(t)
	response := f.get("/xrpc/" + orgtangled.NSIDDescribeSpindle)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	var output orgtangled.DescribeSpindleOutput
	if err := json.Unmarshal(response.Body.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	if output.DID != testSpindleDID || output.Version == "" ||
		!equalStrings(output.Capabilities, []string{"tangled1.0"}) || output.Contact != nil {
		t.Fatalf("describeSpindle = %+v", output)
	}
}
