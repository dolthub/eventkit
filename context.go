package eventkit

import "context"

type contextKey struct{}

var eventContextKey = contextKey{}

func NewContextForEvent(ctx context.Context, evt *Event) context.Context {
	return context.WithValue(ctx, eventContextKey, evt)
}

func GetEventFromContext(ctx context.Context) *Event {
	evt, _ := ctx.Value(eventContextKey).(*Event)
	return evt
}
