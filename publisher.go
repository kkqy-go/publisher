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
	subscribers     generic.SyncMap[*Subscriber[T], struct{}]
	newSubscriberCh chan NewSubscriberEvent[T]
	subscribeBufLen int
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
func (p *Publisher[T]) Subscribe(subCtx context.Context) <-chan T {
	ch := make(chan T, p.subscribeBufLen)
	subscriber := &Subscriber[T]{
		subCtx: subCtx,
		ch:     ch,
	}
	p.subscribers.Store(subscriber, struct{}{})
	if p.newSubscriberCh != nil {
		select {
		case p.newSubscriberCh <- NewSubscriberEvent[T]{subscriber: subscriber, ctx: subCtx}:
		case <-subCtx.Done():
		}
	}
	return ch
}

type NewSubscriberEvent[T any] struct {
	subscriber *Subscriber[T]
	ctx        context.Context
}

func (p *NewSubscriberEvent[T]) Subscriber() *Subscriber[T] {
	return p.subscriber
}

func (p *NewSubscriberEvent[T]) Ctx() context.Context {
	return p.ctx
}

func (p *Publisher[T]) NewSubscriberC() <-chan NewSubscriberEvent[T] {
	return p.newSubscriberCh
}

type PublisherOption[T any] func(p *Publisher[T])

func WithSubscriberBufLen[T any](bufLen int) PublisherOption[T] {
	return func(p *Publisher[T]) {
		p.subscribeBufLen = bufLen
	}
}

func WithNewSubscriberChannel[T any](bufLen int) PublisherOption[T] {
	return func(p *Publisher[T]) {
		p.newSubscriberCh = make(chan NewSubscriberEvent[T], bufLen)
	}
}

func NewPublisher[T any](opts ...PublisherOption[T]) *Publisher[T] {
	p := &Publisher[T]{}
	for _, opt := range opts {
		opt(p)
	}
	return p
}
