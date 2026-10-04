package nats

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/adrianliechti/sns-gateway/backend"
)

func TestJetStreamPersistenceAndFanout(t *testing.T) {
	endpoint := os.Getenv("SNS_TEST_NATS_URL")
	if endpoint == "" {
		t.Skip("set SNS_TEST_NATS_URL to a disposable JetStream server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	opts := Options{URL: endpoint, Namespace: "sns_test_" + strings.ReplaceAll(backend.NewID(), "-", "")}
	b, err := New(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer b.Close()
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.js.DeleteStream(cleanup, b.stream)
		_ = b.js.DeleteKeyValue(cleanup, opts.Namespace+"_state")
	})
	s, revision, err := b.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.Topics["topic"] = backend.Topic{ARN: "topic", ID: "generation", Name: "topic"}
	s.Subscriptions["filtered"] = backend.Subscription{ARN: "filtered", FilterPolicy: `{"event":["created"]}`, FilterPolicyScope: "MessageBody"}
	if err := b.Save(ctx, s, revision); err != nil {
		t.Fatal(err)
	}
	if err := b.Save(ctx, s, revision); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("stale save: %v", err)
	}
	one := backend.Subscription{ID: backend.NewID(), TopicID: "generation"}
	two := backend.Subscription{ID: backend.NewID(), TopicID: "generation"}
	for _, sub := range []backend.Subscription{one, two} {
		if err := b.EnsureSubscription(ctx, sub); err != nil {
			t.Fatal(err)
		}
	}
	message := backend.Message{ID: backend.NewID(), TopicID: "generation", Body: "persisted"}
	if err := b.Publish(ctx, message); err != nil {
		t.Fatal(err)
	}
	// A second connection represents a restarted gateway. State, the signing
	// identity (when present), queued messages and cursors are broker-owned.
	reopened, err := New(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	loaded, _, err := reopened.Load(ctx)
	if err != nil || loaded.Topics["topic"].ID != "generation" {
		t.Fatalf("%+v %v", loaded, err)
	}
	if loaded.Subscriptions["filtered"] != s.Subscriptions["filtered"] {
		t.Fatal("subscription filter changed after reconnect")
	}
	for _, sub := range []backend.Subscription{one, two} {
		if err := reopened.EnsureSubscription(ctx, sub); err != nil {
			t.Fatal(err)
		} // Must not reset the existing cursor.
		consumer, err := reopened.Consumer(ctx, sub)
		if err != nil {
			t.Fatal(err)
		}
		delivery, err := consumer.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if delivery.Message().ID != message.ID {
			t.Fatal("wrong message")
		}
		if sub.ID == one.ID {
			if err := delivery.Retry(10 * time.Millisecond); err != nil {
				t.Fatal(err)
			}
			delivery, err = consumer.Next(ctx)
			if err != nil || delivery.Message().ID != message.ID {
				t.Fatalf("retry: %v", err)
			}
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
		_ = consumer.Close()
	}
	if err := reopened.DeleteSubscription(ctx, one); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Consumer(ctx, one); err == nil {
		t.Fatal("deleted cursor still exists")
	}
}
