package model

type HTTPResponse struct {
	RequestID string     `json:"request_id"`
	Result    string     `json:"result"`
	Data      any        `json:"data,omitempty"`
	Error     *HTTPError `json:"error,omitempty"`
}

type HTTPError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
