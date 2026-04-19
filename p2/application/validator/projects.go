package validator

import "time"

// ValidateCreateProjectPayload returns false when start_date is after end_date.
func ValidateSprintDateRange(startDate, endDate string) bool {
	s, err1 := time.Parse("2006-01-02", startDate)
	e, err2 := time.Parse("2006-01-02", endDate)
	if err1 != nil || err2 != nil {
		return false
	}
	return !s.After(e)
}
