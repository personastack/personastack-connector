package targetruntime

import (
	"testing"

	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
)

func TestLoopbackURLIsStableAndProfileScoped(t *testing.T) {
	target := &externalagentprotocol.RuntimeTarget{RuntimeKind: externalagentprotocol.RuntimeKindHermes, AccountCandidateID: "account-a", ProfileCandidateID: "profile-a"}
	first, err := LoopbackURL(target, "installation-secret")
	if err != nil {
		t.Fatalf("LoopbackURL() error = %v", err)
	}
	second, err := LoopbackURL(target, "installation-secret")
	if err != nil {
		t.Fatalf("LoopbackURL() second error = %v", err)
	}
	if first != second || first[:7] != "http://" {
		t.Fatalf("LoopbackURL() = %q then %q", first, second)
	}
	target.ProfileCandidateID = "profile-b"
	different, err := LoopbackURL(target, "installation-secret")
	if err != nil {
		t.Fatalf("LoopbackURL() changed target error = %v", err)
	}
	if different == first {
		t.Fatal("different profile received same target endpoint")
	}
}
