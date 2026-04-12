package presenter

import (
	"encoding/json"

	"carry_sprint/p2/domain/model"
)

func OK(requestID string, payload any) model.ZMQResponse {
	b, _ := json.Marshal(payload)
	return model.ZMQResponse{RequestID: requestID, Status: "ok", Data: b}
}

func Error(requestID, code, message string) model.ZMQResponse {
	return model.ZMQResponse{RequestID: requestID, Status: "error", Error: &model.ZMQError{Code: code, Message: message}}
}
