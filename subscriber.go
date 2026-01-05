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
func (s *Subscriber[T]) Send(ctx context.Context, data T) bool {
	select {
	case s.ch <- data:
		return true
	case <-s.subCtx.Done():
		return false
	case <-ctx.Done():
		return false
	}
}
