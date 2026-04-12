package model

import "encoding/json"

type ZMQRequest struct {
	RequestID   string            `json:"request_id"`
	Command     string            `json:"command"`
	ProjectID   string            `json:"project_id"`
	PathParams  map[string]string `json:"path_params"`
	QueryParams map[string]string `json:"query_params"`
	Payload     json.RawMessage   `json:"payload"`
}

type ZMQResponse struct {
	RequestID string          `json:"request_id"`
	Status    string          `json:"status"`
	Data      json.RawMessage `json:"data,omitempty"`
	Error     *ZMQError       `json:"error,omitempty"`
}

type ZMQError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
