package middleware

import (
	"net/http"
	"strings"

	"carry_sprint/p1/transport/http/presenter"
)

func RequireRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := strings.TrimSpace(r.Header.Get("X-Request-Id"))
		if requestID == "" {
			presenter.WriteError(w, "missing-request-id", "INVALID_PATH_PARAM", "X-Request-Id is required", http.StatusBadRequest)
			return
		}
		next.ServeHTTP(w, r)
	})
}
