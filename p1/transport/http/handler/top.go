package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"carry_sprint/p1/shared/model"
	"carry_sprint/p1/transport/http/presenter"
)

func (h *Handler) HandleGetTopMenu(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimSpace(r.Header.Get("X-User-Id"))
	if userID == "" {
		presenter.WriteError(w, requestID(r), "INVALID_PATH_PARAM", "X-User-Id is required", http.StatusBadRequest)
		return
	}
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "get_top_menu", QueryParams: map[string]string{"user_id": userID}})
}

func (h *Handler) HandleGetUserMenuVisibility(w http.ResponseWriter, r *http.Request, userID string) {
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "get_user_menu_visibility", PathParams: map[string]string{"user_id": userID}})
}

func (h *Handler) HandleSaveUserMenuVisibility(w http.ResponseWriter, r *http.Request, userID string) {
	var p map[string]any
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		presenter.WriteError(w, requestID(r), "INVALID_JSON", "invalid json payload", http.StatusBadRequest)
		return
	}
	payload, _ := json.Marshal(p)
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "save_user_menu_visibility", PathParams: map[string]string{"user_id": userID}, Payload: payload})
}
