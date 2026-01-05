package publisher

import "context"

type NewSubscriberEvent[T any] struct {
	Send   func(ctx context.Context, data T) bool
	Commit func()
}
