package externalagentprotocol

import "testing"

func TestResolveWebsocketURLAcceptsHostPort(t *testing.T) {
	resolved, err := ResolveWebsocketURL("127.0.0.1:36334")
	if err != nil {
		t.Fatalf("resolve host port websocket url: %v", err)
	}

	want := "wss://127.0.0.1:36334/v1/external-agent/ws"
	if resolved != want {
		t.Fatalf("resolved url = %q, want %q", resolved, want)
	}
}
