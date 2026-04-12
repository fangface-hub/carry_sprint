package handler

import (
	"net/http"

	"carry_sprint/p1/shared/model"
)

func (h *Handler) HandleGetSprintWorkspace(w http.ResponseWriter, r *http.Request, projectID, sprintID string) {
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "get_sprint_workspace", PathParams: map[string]string{"project_id": projectID, "sprint_id": sprintID}})
}
