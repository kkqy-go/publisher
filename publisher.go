package publisher

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
)

type config struct {
	subBufLen int
}
type Publisher[T any] struct {
	eventMap             map[any]T
	sourceCh             chan T // Publisher now creates and owns this channel
	newSubCh             chan *Subscriber[T]
	unSubCh              chan *Subscriber[T]
	subscribers          map[*Subscriber[T]]struct{}
	config               config
	newSubscriberEventCh chan<- NewSubscriberEvent[T]
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
func (p *Publisher[T]) cleanup() {
	for subscriber := range p.subscribers {
		close(subscriber.ch)
	}
}
func (p *Publisher[T]) Run() error {
	defer p.cleanup()
	for {
		select {
		case event, ok := <-p.sourceCh:
			if !ok {
				// Source channel closed, publisher is shutting down.
				return nil
			}
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
				subscriber.send(event)
			}
		case subscriber := <-p.newSubCh:
			p.subscribers[subscriber] = struct{}{}
			for _, event := range p.eventMap {
				subscriber.send(event)
			}
			if p.newSubscriberEventCh != nil {
				var closeFlag atomic.Bool
				done := make(chan struct{})
				newSubscriberEvent := NewSubscriberEvent[T]{
					Send: func(ctx context.Context, event T) bool {
						return subscriber.send(event)
					},
					Commit: func() {
						if !closeFlag.Swap(true) {
							close(done)
						}
					},
				}
				select {
				case <-subscriber.subCtx.Done():
					subscriber.Close()
				case p.newSubscriberEventCh <- newSubscriberEvent:
					select {
					case <-done:
					case <-subscriber.subCtx.Done():
						subscriber.Close()
					}
				}
			}
		case subscriber := <-p.unSubCh:
			if _, ok := p.subscribers[subscriber]; ok {
				close(subscriber.ch)
				delete(p.subscribers, subscriber)
			}
		}
	}
}

type PublisherOption[T any] func(p *Publisher[T])

func WithSubBufLen[T any](bufLen int) PublisherOption[T] {
	return func(p *Publisher[T]) {
		p.config.subBufLen = bufLen
	}
}

func WithNewSubscriberEventChannel[T any](ch chan<- NewSubscriberEvent[T]) PublisherOption[T] {
	return func(p *Publisher[T]) {
		p.newSubscriberEventCh = ch
	}
}

func NewPublisher[T any](opts ...PublisherOption[T]) (*Publisher[T], chan<- T) {
	sourceCh := make(chan T) // Create an unbuffered channel internally
	p := &Publisher[T]{
		sourceCh:    sourceCh,
		subscribers: make(map[*Subscriber[T]]struct{}),
		eventMap:    make(map[any]T),
	}
	for _, opt := range opts {
		opt(p)
	}
	p.newSubCh = make(chan *Subscriber[T], 1)
	p.unSubCh = make(chan *Subscriber[T], 1)
	return p, sourceCh
}
