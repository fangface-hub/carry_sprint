package zmq

import (
	"encoding/json"

	"carry_sprint/p1/shared/model"
)

func EncodeRequest(req model.ZMQRequest) ([]byte, error) {
	return json.Marshal(req)
}

func DecodeResponse(raw []byte) (model.ZMQResponse, error) {
	var resp model.ZMQResponse
	err := json.Unmarshal(raw, &resp)
	return resp, err
}
