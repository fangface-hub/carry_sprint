package handler

import (
	"net/http"

	"carry_sprint/p1/shared/model"
)

func (h *Handler) HandleListProjects(w http.ResponseWriter, r *http.Request) {
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "list_projects"})
}

func (h *Handler) HandleGetProjectSummary(w http.ResponseWriter, r *http.Request, projectID string) {
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "get_project_summary", PathParams: map[string]string{"project_id": projectID}})
}
