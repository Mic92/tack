package main

// This file publishes Tack's identity as a spindle: its did:web DID document and the
// org.tangled.spindle.describeSpindle XRPC method.
//
// knot2 finds a repository's spindle through the DID named in its repo manifest (the
// knot-hosted org.tangled.repo.manifest record). Before it delivers an
// org.tangled.event.notifyEvent call, knot2 fetches that DID's document from
// https://<host>/.well-known/did.json, without caching it, and sends the call to the
// TangledSpindle service endpoint the document declares. A spindle that serves no document
// cannot be reached (knot2 logs "DID document fetch returned HTTP 404"), so this document is
// what makes push-triggered CI work at all.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime/debug"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/identity"

	"go.mitchellh.com/tack/internal/orgtangled"
)

// didDocumentPath is where did:web resolvers fetch the document for a DID with no path
// segments, such as did:web:tack.example.com. Tack's DID is always of that form.
const didDocumentPath = "/.well-known/did.json"

// spindleServiceID and spindleServiceType identify the service entry knot2 looks for in a
// spindle's DID document. knot2 accepts any entry whose id ends in "#tangled_spindle" and
// whose type is exactly "TangledSpindle", and reports "no tangled_spindle service endpoint"
// otherwise.
const (
	spindleServiceID   = "#tangled_spindle"
	spindleServiceType = "TangledSpindle"
)

// didDocument is a did:web DID document in the same shape the upstream Tangled spindle
// publishes. It adds the JSON-LD "@context" list, which indigo's DIDDocument lacks.
// Embedding indigo's type keeps every other field name identical to what atproto resolvers
// parse.
type didDocument struct {
	Context []string `json:"@context"`
	identity.DIDDocument
}

// newDIDDocument builds Tack's DID document for cfg. The document id is cfg.DID(), which
// must equal the DID knot2 asked for, because knot2 rejects a document that claims to
// describe a different DID.
//
// key may be nil. Tack signs nothing, and knot2 reads only the service entry, so no Tangled
// v1.20 consumer needs a verification method. When the operator supplies a key, the document
// gains an "#atproto" Multikey verification method and the multikey context, and matches
// the upstream spindle's document field for field.
func newDIDDocument(cfg config, key atcrypto.PublicKey) didDocument {
	did := cfg.DID()
	doc := didDocument{
		Context: []string{"https://www.w3.org/ns/did/v1"},
		DIDDocument: identity.DIDDocument{
			DID: did,
			Service: []identity.DocService{{
				ID:              spindleServiceID,
				Type:            spindleServiceType,
				ServiceEndpoint: "https://" + cfg.Hostname,
			}},
		},
	}
	if key != nil {
		doc.Context = append(doc.Context, "https://w3id.org/security/multikey/v1")
		doc.VerificationMethod = []identity.DocVerificationMethod{{
			ID:                 did.String() + "#atproto",
			Type:               "Multikey",
			Controller:         did.String(),
			PublicKeyMultibase: key.Multibase(),
		}}
	}
	return doc
}

// didDocumentHandler returns the handler for [didDocumentPath]. The document cannot change
// while the process runs, so it is encoded once up front. That also guarantees every fetch
// returns identical bytes. Write errors are logged only at debug level, because a failed
// write almost always means the client disconnected.
func didDocumentHandler(logger *slog.Logger, cfg config, key atcrypto.PublicKey) http.HandlerFunc {
	body, err := json.Marshal(newDIDDocument(cfg, key))
	if err != nil {
		// The document holds only strings, so encoding cannot fail at runtime. A failure
		// here is a programming error.
		panic(fmt.Sprintf("encode DID document: %v", err))
	}
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/did+json")
		if _, err := w.Write(body); err != nil {
			logger.Debug("write DID document", "err", err)
		}
	}
}

// loadSpindleKey reads the P-256 private key at path and returns its public half for the
// DID document. An empty path means no key is configured and returns (nil, nil).
//
// The file must be PEM encoded, in either SEC1 ("EC PRIVATE KEY") or PKCS#8 form. This
// matches what the upstream spindle accepts, so an operator can reuse the same key file.
// Any other key type or curve is an error, as is a missing or unreadable file. The read
// error includes the openssl command that generates a suitable key.
func loadSpindleKey(path string) (atcrypto.PublicKey, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read spindle key (generate one with "+
			"`openssl ecparam -name prime256v1 -genkey -noout -out %s`): %w", path, err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("spindle key %s is not PEM encoded", path)
	}
	private, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		parsed, pkcs8Err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if pkcs8Err != nil {
			return nil, fmt.Errorf("parse spindle key %s: %w", path, errors.Join(err, pkcs8Err))
		}
		var ok bool
		if private, ok = parsed.(*ecdsa.PrivateKey); !ok {
			return nil, fmt.Errorf("spindle key %s is not an ECDSA key", path)
		}
	}
	if private.Curve != elliptic.P256() {
		return nil, fmt.Errorf("spindle key %s is on %s, want P-256", path, private.Curve.Params().Name)
	}
	raw, err := private.Bytes()
	if err != nil {
		return nil, fmt.Errorf("encode spindle key %s: %w", path, err)
	}
	key, err := atcrypto.ParsePrivateBytesP256(raw)
	if err != nil {
		return nil, fmt.Errorf("load spindle key %s: %w", path, err)
	}
	return key.PublicKey()
}

// describeSpindleHandler implements org.tangled.spindle.describeSpindle, which reports the
// spindle's DID, version, capabilities, and optional operator contact. Nothing in Tangled
// v1.20 calls it yet. Tack serves it anyway because the lexicon treats a spindle that omits
// the capabilities list as legacy, and the "tangled1.0" capability marks Tack as current.
// The contact field is omitted when no contact email is configured.
func describeSpindleHandler(cfg config) http.HandlerFunc {
	output := orgtangled.DescribeSpindleOutput{
		DID:          cfg.DID().String(),
		Version:      tackVersion(),
		Capabilities: []string{"tangled1.0"},
	}
	if cfg.ContactEmail != "" {
		email := cfg.ContactEmail
		output.Contact = &orgtangled.SpindleContact{Email: &email}
	}
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, output)
	}
}

// tackVersion returns the version string describeSpindle reports, such as "tack v1.2.3".
// The version comes from the module build info that the Go toolchain stamps into the
// binary. Local builds without a real module version report "tack dev".
func tackVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" &&
		info.Main.Version != "(devel)" {
		return "tack " + info.Main.Version
	}
	return "tack dev"
}
