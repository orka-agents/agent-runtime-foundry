package harness

import "fmt"

func validateAcceptedTurn(request StartTurnRequest, accepted StartTurnResponse) error {
	if accepted.RuntimeSessionID != request.RuntimeSessionID {
		return fmt.Errorf("harness accepted runtime session %q, want %q", accepted.RuntimeSessionID, request.RuntimeSessionID)
	}
	if accepted.TurnID != request.TurnID {
		return fmt.Errorf("harness accepted turn %q, want %q", accepted.TurnID, request.TurnID)
	}
	if accepted.CorrelationID != "" && accepted.CorrelationID != request.CorrelationID {
		return fmt.Errorf("harness accepted correlation id %q, want %q", accepted.CorrelationID, request.CorrelationID)
	}
	return nil
}
