package publisher

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// TestPublisher_SimplePubSub tests a basic scenario: one publisher, one subscriber.
func TestPublisher_SimplePubSub(t *testing.T) {
	p := NewPublisher[string]()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- p.Run(ctx)
	}()

	sub := p.Subscribe(context.Background())
	msg := "hello world"
	p.Publish(context.Background(), msg)

	select {
	case received := <-sub.C():
		if received != msg {
			t.Fatalf("expected message '%s', but got '%s'", msg, received)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for message")
	}
}

// TestPublisher_MultipleSubscribers tests that a message is broadcast to all subscribers.
func TestPublisher_MultipleSubscribers(t *testing.T) {
	p := NewPublisher[string]()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- p.Run(ctx)
	}()

	numSubscribers := 5
	subscribers := make([]*Subscriber[string], numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		subscribers[i] = p.Subscribe(context.Background())
	}

	msg := "hello everyone"
	p.Publish(context.Background(), msg)

	var wg sync.WaitGroup
	wg.Add(numSubscribers)

	for i := 0; i < numSubscribers; i++ {
		go func(i int, sub *Subscriber[string]) {
			defer wg.Done()
			select {
			case received := <-sub.C():
				if received != msg {
					t.Errorf("subscriber %d: expected message '%s', but got '%s'", i, msg, received)
				}
			case <-time.After(100 * time.Millisecond):
				t.Errorf("subscriber %d: timed out waiting for message", i)
			}
		}(i, subscribers[i])
	}

	wg.Wait()
}

// TestSubscriber_Close tests that a subscriber stops receiving messages after Close is called.
func TestSubscriber_Close(t *testing.T) {
	p := NewPublisher[string]()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	errCh := make(chan error, 1)
	go func() {
		errCh <- p.Run(ctx)
	}()

	sub1 := p.Subscribe(context.Background())
	sub2 := p.Subscribe(context.Background())

	sub1.Close()
	time.Sleep(50 * time.Millisecond)

	msg := "a message for sub2"
	p.Publish(context.Background(), msg)

	select {
	case msg, ok := <-sub1.C():
		if ok {
			t.Fatalf("closed subscriber received a message: '%s'", msg)
		}
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case received := <-sub2.C():
		if received != msg {
			t.Fatalf("sub2 expected message '%s', but got '%s'", msg, received)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("sub2 timed out waiting for message")
	}
}

type myEvent struct {
	id      int
	message string
}

func (e myEvent) Config() EventConfig {
	return EventConfig{
		Key:                        e.id,
		AutoPublishToNewSubscriber: true,
	}
}

func TestPublisher_EventReplay(t *testing.T) {
	p := NewPublisher[myEvent]()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	errCh := make(chan error, 1)
	go func() {
		errCh <- p.Run(ctx)
	}()

	event1 := myEvent{id: 1, message: "event 1"}
	event2 := myEvent{id: 2, message: "event 2"}

	p.Publish(context.Background(), event1)
	p.Publish(context.Background(), event2)

	time.Sleep(50 * time.Millisecond)

	sub := p.Subscribe(context.Background())

	receivedEvents := make(map[int]myEvent)
	for i := 0; i < 2; i++ {
		select {
		case received := <-sub.C():
			receivedEvents[received.id] = received
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("timed out waiting for replay event %d", i+1)
		}
	}

	if len(receivedEvents) != 2 {
		t.Fatalf("expected to receive 2 replay events, but got %d", len(receivedEvents))
	}
	if !reflect.DeepEqual(receivedEvents[1], event1) {
		t.Errorf("expected event1 %v, got %v", event1, receivedEvents[1])
	}
	if !reflect.DeepEqual(receivedEvents[2], event2) {
		t.Errorf("expected event2 %v, got %v", event2, receivedEvents[2])
	}
}

func TestPublisher_RunContextCancellation(t *testing.T) {
	p := NewPublisher[string]()
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- p.Run(ctx)
	}()

	sub := p.Subscribe(context.Background())
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case _, ok := <-sub.C():
		if ok {
			t.Fatal("subscriber channel should be closed after publisher context is cancelled")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for subscriber channel to be closed")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run() returned an unexpected error on context cancellation: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for Run loop to exit")
	}
}

func TestSubscriber_ContextCancellation(t *testing.T) {
	p := NewPublisher[string]()
	ctx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	errCh := make(chan error, 1)
	go func() {
		errCh <- p.Run(ctx)
	}()

	subCtx, cancelSub := context.WithCancel(context.Background())
	sub := p.Subscribe(subCtx)

	cancelSub()
	time.Sleep(50 * time.Millisecond)

	sent := make(chan bool, 1)
	go func() {
		sent <- sub.Send(context.Background(), "should not be sent")
	}()

	select {
	case res := <-sent:
		if res {
			t.Fatal("Send() returned true to a cancelled subscriber, should be false")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Send() blocked when sending to a subscriber with a cancelled context")
	}
}

type nonComparableKeyEvent struct {
	key     []int // slices are not comparable
	message string
}

func (e nonComparableKeyEvent) Config() EventConfig {
	return EventConfig{
		Key:                        e.key,
		AutoPublishToNewSubscriber: true,
	}
}

// TestPublisher_RunReturnsErrorOnNonComparableKey verifies that Run() exits with an error
// when an event with a non-comparable key is published.
func TestPublisher_RunReturnsErrorOnNonComparableKey(t *testing.T) {
	p := NewPublisher[nonComparableKeyEvent]()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- p.Run(ctx)
	}()

	// This event has a slice as a key, which is not comparable.
	badEvent := nonComparableKeyEvent{key: []int{1, 2}, message: "bad event"}
	p.Publish(context.Background(), badEvent)

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Run() did not return an error for a non-comparable key")
		}
		expectedErrStr := fmt.Sprintf("publisher: event key of type %T is not comparable", badEvent.key)
		if err.Error() != expectedErrStr {
			t.Fatalf("expected error string '%s', but got '%s'", expectedErrStr, err.Error())
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for Run() to return an error")
	}
}
