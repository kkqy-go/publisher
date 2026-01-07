package publisher

import (
	"context"
	"fmt" // For logging errors from the Run goroutine
	"reflect"
	"sync/atomic"
)

type config struct {
	subBufLen int
}
type Publisher[T any] struct {
	done                 chan struct{} // done is closed when the Run goroutine exits.
	closedFlag           atomic.Bool   // Ensures Close() is idempotent.
	eventMap             map[any]T
	sourceCh             chan T // Publisher now creates and owns this channel
	newSubCh             chan *Subscriber[T]
	unSubCh              chan *Subscriber[T]
	subscribers          map[*Subscriber[T]]struct{}
	config               config
	newSubscriberEventCh chan NewSubscriberEvent[T]
	errorEventCh         chan error
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
	case <-p.done:
		close(ch)
	case <-subCtx.Done():
		close(ch)
	}
	return subscriber
}

func (p *Publisher[T]) Unsubscribe(subscriber *Subscriber[T]) {
	select {
	case p.unSubCh <- subscriber:
	case <-p.done:
	}
}
func (p *Publisher[T]) Publish(ctx context.Context, event T) {
	select {
	case p.sourceCh <- event:
	case <-ctx.Done():
	}
}
func (p *Publisher[T]) C() chan<- T {
	return p.sourceCh
}
func (p *Publisher[T]) NewSubEventC() <-chan NewSubscriberEvent[T] {
	return p.newSubscriberEventCh
}
func (p *Publisher[T]) ErrorC() <-chan error {
	return p.errorEventCh
}

func (p *Publisher[T]) Close() {
	if p.closedFlag.Swap(true) {
		return
	}
	close(p.sourceCh)
}

type PublisherOption[T any] func(p *Publisher[T])

func WithSubBufLen[T any](bufLen int) PublisherOption[T] {
	return func(p *Publisher[T]) {
		p.config.subBufLen = bufLen
	}
}

func WithNewSubscriberEventChannel[T any](bufLen ...int) PublisherOption[T] {
	if len(bufLen) == 0 {
		bufLen = []int{0}
	}
	ch := make(chan NewSubscriberEvent[T], bufLen[0])
	return func(p *Publisher[T]) {
		p.newSubscriberEventCh = ch
	}
}

func WithErrorEventChannel[T any](bufLen ...int) PublisherOption[T] {
	if len(bufLen) == 0 {
		bufLen = []int{0}
	}
	ch := make(chan error, bufLen[0])
	return func(p *Publisher[T]) {
		p.errorEventCh = ch
	}
}

func NewPublisher[T any](opts ...PublisherOption[T]) *Publisher[T] {
	p := &Publisher[T]{
		done:        make(chan struct{}),
		sourceCh:    make(chan T),
		subscribers: make(map[*Subscriber[T]]struct{}),
		eventMap:    make(map[any]T),
	}
	for _, opt := range opts {
		opt(p)
	}
	p.newSubCh = make(chan *Subscriber[T], 1)
	p.unSubCh = make(chan *Subscriber[T], 1)

	go func() {
		cleanup := func() {
			for subscriber := range p.subscribers {
				close(subscriber.ch)
			}
			if p.newSubscriberEventCh != nil {
				close(p.newSubscriberEventCh)
			}
			close(p.done)
		}
		defer cleanup()
		for {
			select {
			case event, ok := <-p.sourceCh:
				if !ok {
					return
				}
				var _e any = event
				if e, ok := _e.(EventWithConfig); ok {
					config := e.EventConfig()
					if config.AutoPublishToNewSubscriber && config.Key != nil {
						keyType := reflect.TypeOf(config.Key)
						if !keyType.Comparable() {
							if p.errorEventCh != nil {
								p.errorEventCh <- fmt.Errorf("publisher error: event key of type %s is not comparable", keyType)
							}
							return
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
	}()
	return p
}
