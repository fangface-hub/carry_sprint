package validator

var allowedMenuKeys = map[string]struct{}{
	"project_select":    {},
	"sprint_workspace":  {},
	"resource_settings": {},
	"calendar_settings": {},
	"user_management":   {},
}

func ValidateMenuKey(key string) bool {
	_, ok := allowedMenuKeys[key]
	return ok
}
