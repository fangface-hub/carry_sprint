package handler

import (
	"encoding/json"
	"net/http"

	"carry_sprint/p1/shared/model"
	"carry_sprint/p1/transport/http/presenter"
)

func (h *Handler) HandleListUsers(w http.ResponseWriter, r *http.Request) {
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "list_users"})
}

func (h *Handler) HandleRegisterUser(w http.ResponseWriter, r *http.Request) {
	var p map[string]any
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		presenter.WriteError(w, requestID(r), "INVALID_JSON", "invalid json payload", http.StatusBadRequest)
		return
	}
	payload, _ := json.Marshal(p)
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "register_user", Payload: payload})
}

func (h *Handler) HandleUpdateUser(w http.ResponseWriter, r *http.Request, userID string) {
	var p map[string]any
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		presenter.WriteError(w, requestID(r), "INVALID_JSON", "invalid json payload", http.StatusBadRequest)
		return
	}
	payload, _ := json.Marshal(p)
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "update_user", PathParams: map[string]string{"user_id": userID}, Payload: payload})
}

func (h *Handler) HandleDeleteUser(w http.ResponseWriter, r *http.Request, userID string) {
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "delete_user", PathParams: map[string]string{"user_id": userID}})
}
