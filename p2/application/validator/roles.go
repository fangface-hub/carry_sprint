package validator

func ValidateRole(role string) bool {
	return role == "administrator" || role == "assignee"
}
