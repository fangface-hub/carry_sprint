package validator

import "time"

func IsISODate(v string) bool {
	_, err := time.Parse("2006-01-02", v)
	return err == nil
}

func HasDuplicateDate(dates []string) bool {
	seen := map[string]struct{}{}
	for _, d := range dates {
		if _, ok := seen[d]; ok {
			return true
		}
		seen[d] = struct{}{}
	}
	return false
}
