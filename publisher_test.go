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
	p := NewPublisher[TestEvent](
		WithNewSubscriberEventChannel[TestEvent](1),
		WithSubBufLen[TestEvent](10),
	)
	sourceCh := p.C()
	newSubEventCh := p.NewSubEventC()
	defer p.Close()

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
}

// TestSnapshot_SlowProviderWithSubscriberCancel_FinalArch verifies robustness with the final API.
func TestSnapshot_SlowProviderWithSubscriberCancel_FinalArch(t *testing.T) {
	// 1. Setup
	p := NewPublisher[TestEvent](WithNewSubscriberEventChannel[TestEvent](1))
	newSubEventCh := p.NewSubEventC()
	defer p.Close()

	// 2. Setup a SLOW snapshot provider
	providerCommitDelay := 100 * time.Millisecond
	go func() {
		select {
		case event := <-newSubEventCh:
			time.Sleep(providerCommitDelay)
			event.Commit()
		case <-time.After(providerCommitDelay + 50*time.Millisecond):
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

	// 5. Final check for health of Subscribe on a running publisher
	healthCheckCtx, healthCheckCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	healthCheckCancel()
	p.Subscribe(healthCheckCtx) // This should not block

	if healthCheckCtx.Err() == context.DeadlineExceeded {
		t.Fatal("Publisher appears to be hung after a subscriber cancelled during snapshot.")
	}
}

// TestSubscribeOnClosedPublisher verifies that subscribing to a closed publisher

// immediately returns a closed subscriber.

func TestSubscribeOnClosedPublisher(t *testing.T) {

	p := NewPublisher[TestEvent]()



	p.Close()



	// Allow a moment for the Run goroutine to exit and cleanup

	time.Sleep(20 * time.Millisecond)



	subCtx, subCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)

	defer subCancel()



	closedSub := p.Subscribe(subCtx)



	// A read from the subscriber's channel should not block and should indicate the channel is closed.

	select {

	case event, ok := <-closedSub.C():

		if ok {

			t.Fatalf("Channel from a new subscriber on a closed publisher should be closed, but received event: %+v", event)

		}

		// Correct behavior: ok is false, meaning channel is closed.

	case <-time.After(50 * time.Millisecond):

		t.Fatal("Reading from a new subscriber's channel on a closed publisher blocked.")

	}

}


