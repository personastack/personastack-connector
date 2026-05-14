package bridge

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"testing"
	"time"

	"github.com/personastack/agent-gateway/pkg/externalagentprotocol"
	"github.com/personastack/personastack-connector/internal/config"
	"github.com/personastack/personastack-connector/internal/runtime"
)

func TestConnectFrameSignsAPIVerifiableMessage(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	session := testSession(t, publicKey, privateKey)
	session.Now = func() time.Time {
		return time.Unix(100, 0).UTC()
	}

	frame, err := session.ConnectFrame("nonce-1")
	if err != nil {
		t.Fatalf("connect frame: %v", err)
	}
	signature, err := base64.StdEncoding.DecodeString(frame.Connect.CredentialProof)
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	if !ed25519.Verify(publicKey, []byte(CredentialProofMessage(frame)), signature) {
		t.Fatal("signature did not verify")
	}
	if frame.Connect.ProtocolVersion != externalagentprotocol.ProtocolVersionV1 {
		t.Fatalf("protocol version: got=%s", frame.Connect.ProtocolVersion)
	}
}

func TestRunAcceptedFrameCorrelatesRequestMessageID(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	session := testSession(t, publicKey, privateKey)
	request := externalagentprotocol.Frame{
		MessageID:    "request-1",
		RunID:        "run-1",
		AssignmentID: "assignment-1",
	}

	frame := session.RunAcceptedFrame(request, "native-1")
	if frame.MessageID != "request-1" || frame.RunAccepted.NativeRunID != "native-1" {
		t.Fatalf("unexpected frame: %+v", frame)
	}
}

func testSession(t *testing.T, publicKey ed25519.PublicKey, privateKey ed25519.PrivateKey) Session {
	t.Helper()
	session, err := NewSession(config.Binding{
		ConnectionID:         "conn-1",
		PersonaID:            "persona-1",
		ConnectionGeneration: 5,
		RuntimeKind:          runtime.AdapterKindHermes,
	}, Credential{
		ID:         "cred-1",
		PrivateKey: privateKey,
		PublicKey:  publicKey,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	return session
}
