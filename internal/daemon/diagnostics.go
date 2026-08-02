package daemon

import (
	"strings"

	"github.com/personastack/personastack-connector/internal/diagnostics"
	"github.com/personastack/personastack-connector/internal/runtime"
)

var connectorDiagnosticRedactor = diagnostics.NewRedactor()

func safeDiagnosticNote(note string) string {
	redacted := strings.TrimSpace(connectorDiagnosticRedactor.Redact(note))
	if redacted == "" {
		return ""
	}
	if len(redacted) > 512 {
		return redacted[:512]
	}
	return redacted
}

func safeDetection(detection runtime.Detection) runtime.Detection {
	detection.Note = safeDiagnosticNote(detection.Note)
	return detection
}

func selectionRequiredDetection(detection runtime.Detection) runtime.Detection {
	if detection.State != runtime.AdapterStateReady && detection.State != runtime.AdapterStateMCPVerified {
		return detection
	}
	detection.State = runtime.AdapterStateTargetSelectionRequired
	detection.DiagnosticCode = "target_selection_required"
	detection.Note = "waiting for PersonaStack account and profile selection"
	return detection
}
