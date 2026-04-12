package handler

import (
	"encoding/json"
	"net/http"

	"carry_sprint/p1/shared/model"
	"carry_sprint/p1/transport/http/presenter"
)

func (h *Handler) HandleGetProjectRoles(w http.ResponseWriter, r *http.Request, projectID string) {
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "get_project_roles", PathParams: map[string]string{"project_id": projectID}})
}

func (h *Handler) HandleSaveProjectRoles(w http.ResponseWriter, r *http.Request, projectID string) {
	var p map[string]any
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		presenter.WriteError(w, requestID(r), "INVALID_JSON", "invalid json payload", http.StatusBadRequest)
		return
	}
	payload, _ := json.Marshal(p)
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "save_project_roles", PathParams: map[string]string{"project_id": projectID}, Payload: payload})
}
