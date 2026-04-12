package validator

func ValidateCarryoverAction(action string) bool {
	return action == "carryover" || action == "keep"
}
