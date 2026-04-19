package validator

var allowedLocales = []string{"de", "fr", "it", "ja", "zh"}

var allowedLocaleSet = map[string]struct{}{
	"de": {},
	"fr": {},
	"it": {},
	"ja": {},
	"zh": {},
}

func ValidateLocale(locale string) bool {
	_, ok := allowedLocaleSet[locale]
	return ok
}

func SupportedLocales() []string {
	locales := make([]string, len(allowedLocales))
	copy(locales, allowedLocales)
	return locales
}
