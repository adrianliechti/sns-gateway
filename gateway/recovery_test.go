package gateway

import (
	"context"
	"errors"
	"net/url"
	"testing"

	"github.com/adrianliechti/sns-gateway/backend"
	"github.com/adrianliechti/sns-gateway/backend/memory"
)

type failingBackend struct {
	backend.Backend
	failSave   bool
	failDelete bool
}

func (b *failingBackend) Save(ctx context.Context, s backend.State, rev uint64) error {
	if b.failSave {
		return errors.New("metadata unavailable")
	}
	return b.Backend.Save(ctx, s, rev)
}
func (b *failingBackend) DeleteSubscription(ctx context.Context, s backend.Subscription) error {
	if b.failDelete {
		return errors.New("consumer deletion unavailable")
	}
	return b.Backend.DeleteSubscription(ctx, s)
}

func TestDeletionFailureRecovery(t *testing.T) {
	ctx := context.Background()
	be := &failingBackend{Backend: memory.New()}
	g := newTestGateway(t, be, nil)
	arn := createTopic(t, g, "recovery")
	w := call(t, g, "Subscribe", url.Values{"TopicArn": {arn}, "Protocol": {"http"}, "Endpoint": {"http://endpoint/"}, "ReturnSubscriptionArn": {"true"}})
	success(t, w)
	subARN := xmlValue(t, w, "SubscriptionArn")
	be.failSave = true
	w = call(t, g, "Unsubscribe", url.Values{"SubscriptionArn": {subARN}})
	if w.Code != 500 {
		t.Fatal(w.Code)
	}
	s, _, _ := be.Load(ctx)
	sub, exists := s.Subscriptions[subARN]
	if !exists {
		t.Fatal("failed metadata save deleted subscription")
	}
	c, err := be.Consumer(ctx, sub)
	if err != nil {
		t.Fatal(err)
	}
	d, err := c.Next(ctx)
	if err != nil || d.Message().Type != "SubscriptionConfirmation" {
		t.Fatalf("failed delete removed cursor: %v", err)
	}
	_ = d.Ack()
	_ = c.Close()
	be.failSave = false
	be.failDelete = true
	w = call(t, g, "Unsubscribe", url.Values{"SubscriptionArn": {subARN}})
	success(t, w)
	if err := g.cleanup(ctx); err == nil {
		t.Fatal("expected cleanup failure")
	}
	s, _, _ = be.Load(ctx)
	if _, exists := s.Subscriptions[subARN]; exists {
		t.Fatal("subscription remained active")
	}
	if len(s.PendingDeletes) != 1 {
		t.Fatal("cleanup intent lost")
	}
	be.failDelete = false
	restarted := newTestGateway(t, be, nil)
	if err := restarted.cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	s, _, _ = be.Load(ctx)
	if len(s.PendingDeletes) != 0 {
		t.Fatal("tombstone not cleared")
	}
	c, err = be.Consumer(ctx, sub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Next(ctx); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("cursor survived cleanup: %v", err)
	}
}
