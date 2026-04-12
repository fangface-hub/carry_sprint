package zmq

import (
	"context"
	"encoding/json"

	"carry_sprint/p2/domain/model"
	"github.com/go-zeromq/zmq4"
)

type Server struct {
	Endpoint   string
	Dispatcher *Dispatcher
}

func (s *Server) Run(ctx context.Context) error {
	rep := zmq4.NewRep(ctx)
	if err := rep.Listen(s.Endpoint); err != nil {
		return err
	}
	defer rep.Close()

	for {
		msg, err := rep.Recv()
		if err != nil {
			return err
		}
		if len(msg.Frames) == 0 {
			_ = rep.Send(zmq4.NewMsg([]byte(`{"status":"error","error":{"code":"INVALID_JSON","message":"empty payload"}}`)))
			continue
		}
		var req model.ZMQRequest
		if err := json.Unmarshal(msg.Frames[0], &req); err != nil {
			_ = rep.Send(zmq4.NewMsg([]byte(`{"status":"error","error":{"code":"INVALID_JSON","message":"invalid payload"}}`)))
			continue
		}
		resp := s.Dispatcher.Dispatch(req)
		b, _ := json.Marshal(resp)
		if err := rep.Send(zmq4.NewMsg(b)); err != nil {
			return err
		}
	}
}
