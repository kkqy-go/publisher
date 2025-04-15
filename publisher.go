package publisher

import (
	"context"

	"github.com/kkqy-go/generic"
)

type Subscriber[T any] struct {
	subCtx context.Context
	ch     chan T
}

func (s *Subscriber[T]) Publish(ctx context.Context, data T) bool {
	select {
	case s.ch <- data:
		return true
	case <-s.subCtx.Done():
		return false
	case <-ctx.Done():
		return false
	}
}

type Publisher[T any] struct {
	subscribers              generic.SyncMap[*Subscriber[T], struct{}]
	newSubscriberSubscribers generic.SyncMap[*Subscriber[*Subscriber[T]], struct{}]
}

func (p *Publisher[T]) PublishChannel(ctx context.Context, pubChan chan T) {
	for {
		select {
		case data, ok := <-pubChan:
			if !ok {
				return
			}
			p.Publish(ctx, data)
		case <-ctx.Done():
			return
		}
	}
}

func (p *Publisher[T]) Publish(pubCtx context.Context, data T) {
	p.subscribers.Range(func(subscriber *Subscriber[T], _ struct{}) bool {
		ok := subscriber.Publish(pubCtx, data)
		if !ok {
			p.subscribers.Delete(subscriber)
		}
		return true
	})
}

func (p *Publisher[T]) Subscribe(subCtx context.Context) (<-chan T, error) {
	ch := make(chan T, 1)
	subscriber := &Subscriber[T]{
		subCtx: subCtx,
		ch:     ch,
	}
	p.subscribers.Store(subscriber, struct{}{})
	p.newSubscriberSubscribers.Range(func(eventSubscriber *Subscriber[*Subscriber[T]], _ struct{}) bool {
		select {
		case eventSubscriber.ch <- subscriber:
		case <-subCtx.Done():
			p.subscribers.Delete(subscriber)
			return false
		}
		return true
	})
	return ch, nil
}
func (p *Publisher[T]) NewSubscriberChan(ctx context.Context) (<-chan *Subscriber[T], error) {
	ch := make(chan *Subscriber[T], 1)
	eventSubscriber := &Subscriber[*Subscriber[T]]{
		subCtx: ctx,
		ch:     ch,
	}
	p.newSubscriberSubscribers.Store(eventSubscriber, struct{}{})
	return ch, nil
}

func NewPublisher[T any]() *Publisher[T] {
	p := &Publisher[T]{}
	return p
}
