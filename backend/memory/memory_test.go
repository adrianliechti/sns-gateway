package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adrianliechti/sns-gateway/backend"
)

func TestCASAndSnapshotIsolation(t *testing.T) {
	b := New()
	ctx := context.Background()
	s, rev, err := b.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.Topics["one"] = backend.Topic{ARN: "one", Tags: map[string]string{"key": "value"}}
	if err := b.Save(ctx, s, rev); err != nil {
		t.Fatal(err)
	}
	s.Topics["one"].Tags["key"] = "changed"
	if err := b.Save(ctx, s, rev); !errors.Is(err, backend.ErrConflict) {
		t.Fatal(err)
	}
	actual, _, _ := b.Load(ctx)
	if actual.Topics["one"].Tags["key"] != "value" {
		t.Fatal("save shared caller state")
	}
	actual.Topics["one"].Tags["key"] = "changed"
	actual, _, _ = b.Load(ctx)
	if actual.Topics["one"].Tags["key"] != "value" {
		t.Fatal("load shared backend state")
	}
}

func TestIndependentConsumersRetryAndLateSubscribers(t *testing.T) {
	b := New()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, id := range []string{"a", "b"} {
		if err := b.EnsureSubscription(ctx, backend.Subscription{ID: id, TopicID: "topic"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Publish(ctx, backend.Message{ID: "message", TopicID: "topic"}); err != nil {
		t.Fatal(err)
	}
	if err := b.EnsureSubscription(ctx, backend.Subscription{ID: "late", TopicID: "topic"}); err != nil {
		t.Fatal(err)
	}
	a, _ := b.Consumer(ctx, backend.Subscription{ID: "a"})
	second, _ := b.Consumer(ctx, backend.Subscription{ID: "b"})
	d, err := a.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Retry(20 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	other, err := second.Next(ctx)
	if err != nil || other.Message().ID != "message" {
		t.Fatal(err)
	}
	if err := other.Ack(); err != nil {
		t.Fatal(err)
	}
	retry, err := a.Next(ctx)
	if err != nil || retry.Message().ID != "message" {
		t.Fatal(err)
	}
	if err := retry.Ack(); err != nil {
		t.Fatal(err)
	}
	late, _ := b.Consumer(ctx, backend.Subscription{ID: "late"})
	expired, done := context.WithTimeout(ctx, 20*time.Millisecond)
	defer done()
	if _, err := late.Next(expired); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late subscriber replayed an old message: %v", err)
	}
}
