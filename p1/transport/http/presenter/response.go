package presenter

import (
	"encoding/json"
	"net/http"

	"carry_sprint/p1/shared/model"
)

func WriteOK(w http.ResponseWriter, requestID string, dataRaw json.RawMessage) {
	var data any
	if len(dataRaw) > 0 {
		_ = json.Unmarshal(dataRaw, &data)
	}
	writeJSON(w, http.StatusOK, model.HTTPResponse{RequestID: requestID, Result: "ok", Data: data})
}

func WriteError(w http.ResponseWriter, requestID, code, message string, status int) {
	writeJSON(w, status, model.HTTPResponse{RequestID: requestID, Result: "error", Error: &model.HTTPError{Code: code, Message: message}})
}

func writeJSON(w http.ResponseWriter, status int, body model.HTTPResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func MapErrorToStatus(code string) int {
	switch code {
	case "INVALID_PATH_PARAM", "INVALID_JSON", "UNKNOWN_COMMAND":
		return http.StatusBadRequest
	case "ROUTE_NOT_FOUND", "PROJECT_NOT_FOUND", "SPRINT_NOT_FOUND", "TARGET_SPRINT_NOT_FOUND", "USER_NOT_FOUND":
		return http.StatusNotFound
	case "INVALID_ESTIMATE", "INVALID_IMPACT", "DUPLICATE_RESOURCE_ID", "INVALID_RESOURCE_CAPACITY", "DUPLICATE_CALENDAR_DATE", "DUPLICATE_USER_ID", "INVALID_ROLE", "INVALID_MENU_KEY", "DUPLICATE_MENU_KEY":
		return http.StatusUnprocessableEntity
	case "UPSTREAM_TIMEOUT":
		return http.StatusGatewayTimeout
	case "UPSTREAM_UNAVAILABLE":
		return http.StatusBadGateway
	default:
		return http.StatusInternalServerError
	}
}
