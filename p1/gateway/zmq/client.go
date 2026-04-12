package zmq

import (
	"context"
	"errors"
	"sync"

	"carry_sprint/p1/shared/model"
	"github.com/go-zeromq/zmq4"
)

type Client struct {
	sock zmq4.Socket
	mu   sync.Mutex
}

func NewClient(ctx context.Context, endpoint string) (*Client, error) {
	req := zmq4.NewReq(ctx)
	if err := req.Dial(endpoint); err != nil {
		return nil, err
	}
	return &Client{sock: req}, nil
}

func (c *Client) Close() error {
	if c.sock == nil {
		return nil
	}
	return c.sock.Close()
}

func (c *Client) Send(req model.ZMQRequest) (model.ZMQResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	b, err := EncodeRequest(req)
	if err != nil {
		return model.ZMQResponse{}, err
	}
	if err := c.sock.Send(zmq4.NewMsg(b)); err != nil {
		return model.ZMQResponse{}, err
	}
	msg, err := c.sock.Recv()
	if err != nil {
		return model.ZMQResponse{}, err
	}
	if len(msg.Frames) == 0 {
		return model.ZMQResponse{}, errors.New("empty response")
	}
	return DecodeResponse(msg.Frames[0])
}
