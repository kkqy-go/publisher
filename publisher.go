package publisher

import (
	"context"
	"fmt" // Added for error formatting
	"reflect"
)

type config struct {
	pubBufLen int
	subBufLen int
}
type Publisher[T any] struct {
	eventMap    map[any]T
	eventCh     chan T
	newSubCh    chan *Subscriber[T]
	unSubCh     chan *Subscriber[T]
	subscribers map[*Subscriber[T]]struct{}
	config      config
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
func (p *Publisher[T]) Publish(pubCtx context.Context, event T) {
	select {
	case p.eventCh <- event:
	case <-pubCtx.Done():
		return
	}
}
func (p *Publisher[T]) Subscribe(subCtx context.Context) *Subscriber[T] {
	ch := make(chan T, p.config.subBufLen)
	subscriber := &Subscriber[T]{
		publisher: p,
		subCtx:    subCtx,
		ch:        ch,
	}
	select {
	case p.newSubCh <- subscriber:
	case <-subCtx.Done():
		close(ch)
	}
	return subscriber
}

func (p *Publisher[T]) Unsubscribe(subscriber *Subscriber[T]) {
	p.unSubCh <- subscriber
}

func (p *Publisher[T]) Run(ctx context.Context) error {
	for {
		select {
		case event := <-p.eventCh:
			var _e any = event
			if e, ok := _e.(EventWithConfig); ok {
				config := e.Config()
				if config.AutoPublishToNewSubscriber && config.Key != nil {
					keyType := reflect.TypeOf(config.Key)
					if !keyType.Comparable() {
						return fmt.Errorf("publisher: event key of type %s is not comparable", keyType)
					}
					p.eventMap[config.Key] = event
				}
			}
			for subscriber := range p.subscribers {
				subscriber.Send(ctx, event)
			}
		case subscriber := <-p.newSubCh:
			for _, event := range p.eventMap {
				subscriber.Send(ctx, event)
			}
			p.subscribers[subscriber] = struct{}{}
		case subscriber := <-p.unSubCh:
			if _, ok := p.subscribers[subscriber]; ok {
				close(subscriber.ch)
				delete(p.subscribers, subscriber)
			}
		case <-ctx.Done():
			for subscriber := range p.subscribers {
				close(subscriber.ch)
			}
			return nil
		}
	}
}

type PublisherOption[T any] func(p *Publisher[T])

func WithSubBufLen[T any](bufLen int) PublisherOption[T] {
	return func(p *Publisher[T]) {
		p.config.subBufLen = bufLen
	}
}

func WithPubBufLen[T any](bufLen int) PublisherOption[T] {
	return func(p *Publisher[T]) {
		p.config.pubBufLen = bufLen
	}
}

func NewPublisher[T any](opts ...PublisherOption[T]) *Publisher[T] {
	p := &Publisher[T]{
		subscribers: make(map[*Subscriber[T]]struct{}),
		eventMap:    make(map[any]T),
	}
	for _, opt := range opts {
		opt(p)
	}
	p.eventCh = make(chan T, p.config.pubBufLen)
	p.newSubCh = make(chan *Subscriber[T], 1)
	p.unSubCh = make(chan *Subscriber[T], 1)
	return p
}
