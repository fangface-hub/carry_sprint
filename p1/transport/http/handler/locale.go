package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"carry_sprint/p1/shared/model"
	"carry_sprint/p1/transport/http/presenter"
)

func (h *Handler) HandleGetDefaultLocale(w http.ResponseWriter, r *http.Request) {
	queryParams := map[string]string{"accept_language": r.Header.Get("Accept-Language")}
	if userID := strings.TrimSpace(r.Header.Get("X-User-Id")); userID != "" {
		queryParams["user_id"] = userID
	}
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "resolve_default_locale", QueryParams: queryParams})
}

func (h *Handler) HandleGetUserLocaleSetting(w http.ResponseWriter, r *http.Request, userID string) {
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "get_user_locale_setting", PathParams: map[string]string{"user_id": userID}})
}

func (h *Handler) HandleSaveUserLocaleSetting(w http.ResponseWriter, r *http.Request, userID string) {
	var p map[string]any
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		presenter.WriteError(w, requestID(r), "INVALID_JSON", "invalid json payload", http.StatusBadRequest)
		return
	}
	payload, _ := json.Marshal(p)
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "save_user_locale_setting", PathParams: map[string]string{"user_id": userID}, Payload: payload})
}
