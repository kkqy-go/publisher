package publisher

import (
	"context"
)

type Subscriber[T any] struct {
	publisher *Publisher[T]
	subCtx    context.Context
	ch        chan T
}

func (s *Subscriber[T]) Close() {
	s.publisher.Unsubscribe(s)
}
func (s *Subscriber[T]) C() <-chan T {
	return s.ch
}

func (s *Subscriber[T]) Send(ctx context.Context, event T) {
	select {
	case <-ctx.Done():
		return
	case <-s.subCtx.Done():
		return
	case s.publisher.pendingEvents <- PendingEvent[T]{
		subscriber: s,
		event:      event,
	}:
	}

}
