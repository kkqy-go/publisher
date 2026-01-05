package publisher

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// TestEvent is a simple event type for testing.
type TestEvent struct {
	ID         string
	IsSnapshot bool
	Key        string
}

// Config implements EventWithConfig for TestEvent.
func (e TestEvent) Config() EventConfig {
	return EventConfig{
		Key:                        e.Key,
		AutoPublishToNewSubscriber: e.IsSnapshot,
	}
}

// TestSnapshotOrder_FinalArch verifies the event order with the final API.
func TestSnapshotOrder_FinalArch(t *testing.T) {
	// 1. Setup
	newSubEventCh := make(chan NewSubscriberEvent[TestEvent], 1)

	// Create publisher with the new API: it returns the source channel.
	p, sourceCh := NewPublisher[TestEvent](WithNewSubscriberEventChannel(newSubEventCh), WithSubBufLen[TestEvent](10))

	runDone := make(chan struct{})
	go func() {
		p.Run()
		close(runDone)
	}()

	// Publish an internal snapshot event to populate eventMap
	internalSnapshotEvent := TestEvent{ID: "internal-1", IsSnapshot: true, Key: "key1"}
	sourceCh <- internalSnapshotEvent

	time.Sleep(20 * time.Millisecond) // Wait for event to be processed

	// 2. Start the external snapshot provider
	go func() {
		ctx := context.Background()
		select {
		case event := <-newSubEventCh:
			externalSnapshotEvent := TestEvent{ID: "external-1"}
			event.Send(ctx, externalSnapshotEvent)
			event.Commit()
		case <-time.After(200 * time.Millisecond):
		}
	}()

	// 3. Subscribe
	subCtx, subCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer subCancel()
	subscriber := p.Subscribe(subCtx)

	// 4. Publish a live event
	time.Sleep(20 * time.Millisecond) // Ensure subscribe is being processed
	liveEvent := TestEvent{ID: "live-1"}
	sourceCh <- liveEvent

	// 5. Verification
	expectedOrder := []TestEvent{
		internalSnapshotEvent,
		{ID: "external-1"},
		liveEvent,
	}

	receivedEvents := make([]TestEvent, 0, len(expectedOrder))
	for i := 0; i < len(expectedOrder); i++ {
		select {
		case event := <-subscriber.C():
			receivedEvents = append(receivedEvents, event)
		case <-subCtx.Done():
			t.Fatalf("Test timed out. Received: %+v", receivedEvents)
		}
	}

	if !reflect.DeepEqual(expectedOrder, receivedEvents) {
		t.Errorf("Event order mismatch.\nExpected: %+v\nGot:      %+v", expectedOrder, receivedEvents)
	}

	// 6. Cleanup
	close(sourceCh)
	<-runDone
}

// TestSnapshot_SlowProviderWithSubscriberCancel_FinalArch verifies robustness with the final API.
func TestSnapshot_SlowProviderWithSubscriberCancel_FinalArch(t *testing.T) {
	// 1. Setup
	newSubEventCh := make(chan NewSubscriberEvent[TestEvent], 1)
	p, sourceCh := NewPublisher[TestEvent](WithNewSubscriberEventChannel(newSubEventCh))

	runDone := make(chan struct{})
	go func() {
		p.Run()
		close(runDone)
	}()

	// 2. Setup a SLOW snapshot provider
	providerCommitDelay := 100 * time.Millisecond
	go func() {
		select {
		case event := <-newSubEventCh:
			time.Sleep(providerCommitDelay)
			event.Commit()
		case <-time.After(providerCommitDelay + 50 * time.Millisecond):
		}
	}()

	// 3. Subscribe with a SHORT timeout
	subscriberTimeout := 20 * time.Millisecond
	subCtx, subCancel := context.WithTimeout(context.Background(), subscriberTimeout)
	defer subCancel()

	subscriber := p.Subscribe(subCtx)

	// 4. Verification
	select {
	case event, ok := <-subscriber.C():
		if ok {
			t.Fatalf("Received an unexpected event on a cancelled subscriber channel: %+v", event)
		}
	case <-time.After(providerCommitDelay + 50*time.Millisecond):
		t.Fatal("Test timed out: subscriber channel was not closed as expected.")
	}

	// 5. Final check for health
	healthCheckCtx, healthCheckCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer healthCheckCancel()
	p.Subscribe(healthCheckCtx)

	if healthCheckCtx.Err() == context.DeadlineExceeded {
		t.Fatal("Publisher appears to be hung after a subscriber cancelled during snapshot.")
	}

	// 6. Cleanup
	close(sourceCh)
	<-runDone
}
