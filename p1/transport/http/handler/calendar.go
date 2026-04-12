package handler

import (
	"encoding/json"
	"net/http"

	"carry_sprint/p1/shared/model"
	"carry_sprint/p1/transport/http/presenter"
)

func (h *Handler) HandleGetCalendar(w http.ResponseWriter, r *http.Request, projectID string) {
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "get_calendar", PathParams: map[string]string{"project_id": projectID}, QueryParams: map[string]string{"month": r.URL.Query().Get("month")}})
}

func (h *Handler) HandleSaveCalendar(w http.ResponseWriter, r *http.Request, projectID string) {
	var p map[string]any
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		presenter.WriteError(w, requestID(r), "INVALID_JSON", "invalid json payload", http.StatusBadRequest)
		return
	}
	payload, _ := json.Marshal(p)
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "save_calendar", PathParams: map[string]string{"project_id": projectID}, Payload: payload})
}
