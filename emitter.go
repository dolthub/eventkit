package eventkit

import "context"

type Emitter interface {
	Send(ctx context.Context, req *LogEventsRequest) error
}

type NullEmitter struct{}

func (NullEmitter) Send(context.Context, *LogEventsRequest) error {
	return nil
}
