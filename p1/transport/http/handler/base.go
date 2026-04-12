package handler

import (
	"encoding/json"
	"net/http"

	gateway "carry_sprint/p1/gateway/zmq"
	"carry_sprint/p1/shared/model"
	"carry_sprint/p1/transport/http/presenter"
)

type Handler struct {
	Client *gateway.Client
}

func (h *Handler) proxy(w http.ResponseWriter, req model.ZMQRequest) {
	resp, err := h.Client.Send(req)
	if err != nil {
		presenter.WriteError(w, req.RequestID, "UPSTREAM_UNAVAILABLE", err.Error(), presenter.MapErrorToStatus("UPSTREAM_UNAVAILABLE"))
		return
	}
	if resp.Status == "error" {
		if resp.Error == nil {
			presenter.WriteError(w, req.RequestID, "PERSISTENCE_ERROR", "unknown error", presenter.MapErrorToStatus("PERSISTENCE_ERROR"))
			return
		}
		presenter.WriteError(w, req.RequestID, resp.Error.Code, resp.Error.Message, presenter.MapErrorToStatus(resp.Error.Code))
		return
	}
	presenter.WriteOK(w, req.RequestID, resp.Data)
}

func decodeJSON(r *http.Request, v any) error {
	if r.Header.Get("Content-Type") == "" {
		return http.ErrNotSupported
	}
	return json.NewDecoder(r.Body).Decode(v)
}

func requestID(r *http.Request) string {
	return r.Header.Get("X-Request-Id")
}
