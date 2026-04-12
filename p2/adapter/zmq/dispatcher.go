package zmq

import (
	"carry_sprint/p2/application/usecase"
	"carry_sprint/p2/domain/model"
)

type Dispatcher struct {
	Service *usecase.Service
}

func (d *Dispatcher) Dispatch(req model.ZMQRequest) model.ZMQResponse {
	return d.Service.Execute(req)
}
