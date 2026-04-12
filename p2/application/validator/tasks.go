package validator

func ValidateImpact(v string) bool {
	return v == "high" || v == "medium" || v == "low"
}

func ValidateStatus(v string) bool {
	return v == "todo" || v == "in_progress" || v == "done"
}
