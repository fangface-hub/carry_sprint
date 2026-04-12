package validator

func HasAnyUserUpdate(name *string, email *string) bool {
	return name != nil || email != nil
}
