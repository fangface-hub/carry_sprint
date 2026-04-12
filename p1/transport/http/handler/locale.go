package handler

import (
	"net/http"

	"carry_sprint/p1/shared/model"
)

func (h *Handler) HandleGetDefaultLocale(w http.ResponseWriter, r *http.Request) {
	h.proxy(w, model.ZMQRequest{RequestID: requestID(r), Command: "resolve_default_locale", QueryParams: map[string]string{"accept_language": r.Header.Get("Accept-Language")}})
}
