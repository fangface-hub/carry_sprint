package handler

import (
	"encoding/json"
	"net/http"

	"carry_sprint/p1/shared/model"
	"carry_sprint/p1/transport/http/presenter"
)

func (h *Handler) HandleUpdateTask(w http.ResponseWriter, r *http.Request, projectID, taskID string) {
	var p map[string]any
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		presenter.WriteError(w, requestID(r), "INVALID_JSON", "invalid json payload", http.StatusBadRequest)
		return
	}
	payload, _ := json.Marshal(p)
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "update_task", PathParams: map[string]string{"project_id": projectID, "task_id": taskID}, Payload: payload})
}
