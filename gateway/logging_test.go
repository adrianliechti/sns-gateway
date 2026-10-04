package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/adrianliechti/sns-gateway/backend/memory"
)

type logCapture struct {
	mu sync.Mutex
	bytes.Buffer
	events chan map[string]any
}

func (c *logCapture) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var record map[string]any
	if json.Unmarshal(b, &record) == nil {
		c.events <- record
	}
	return c.Buffer.Write(b)
}
func (c *logCapture) text() string { c.mu.Lock(); defer c.mu.Unlock(); return c.Buffer.String() }

func TestDebugEventsAndSecretRedaction(t *testing.T) {
	logs := &logCapture{events: make(chan map[string]any, 100)}
	var attempts atomic.Int32
	transport := roundTrip(func(*http.Request) (*http.Response, error) {
		status := 200
		switch attempts.Add(1) {
		case 1:
			status = 503
		case 3:
			status = 400
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	be := memory.New()
	g := newTestGateway(t, be, transport)
	g.opts.Logger = slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	arn := createTopic(t, g, "logs")
	w := call(t, g, "Subscribe", url.Values{"TopicArn": {arn}, "Protocol": {"http"}, "Endpoint": {"http://endpoint/endpoint-path-secret?token=endpoint-query-secret"}, "ReturnSubscriptionArn": {"true"}})
	success(t, w)
	state, _, _ := be.Load(context.Background())
	sub := state.Subscriptions[xmlValue(t, w, "SubscriptionArn")]
	w = call(t, g, "ConfirmSubscription", url.Values{"TopicArn": {arn}, "Token": {sub.Token}})
	success(t, w)
	w = call(t, g, "Publish", url.Values{"TopicArn": {arn}, "Message": {"payload-secret"}, "MessageAttributes.entry.1.Name": {"kind"}, "MessageAttributes.entry.1.Value.DataType": {"String"}, "MessageAttributes.entry.1.Value.StringValue": {"attribute-secret"}})
	success(t, w)
	messageID, requestID := xmlValue(t, w, "MessageId"), w.Header().Get("x-amzn-RequestId")
	stop := startWorkers(t, g)
	seen := map[string]bool{}
	correlated := false
	for !seen["delivery.acknowledged"] {
		record := receive(t, logs.events)
		event, _ := record["event"].(string)
		seen[event] = true
		if event == "message.accepted" && record["message_id"] == messageID && record["request_id"] == requestID {
			correlated = true
		}
		if event == "delivery.acknowledged" && record["message_id"] != messageID {
			t.Fatal("wrong delivery correlation")
		}
	}
	for _, event := range []string{"api.operation", "api.response", "message.accepted", "worker.started", "delivery.attempt", "delivery.response", "delivery.retry_scheduled", "delivery.acknowledged"} {
		if !seen[event] {
			t.Errorf("missing event %s", event)
		}
	}
	if !correlated {
		t.Error("accepted message did not correlate to request")
	}
	w = call(t, g, "Publish", url.Values{"TopicArn": {arn}, "Message": {"discard-secret"}})
	success(t, w)
	discardID := xmlValue(t, w, "MessageId")
	for {
		record := receive(t, logs.events)
		if record["event"] == "delivery.acknowledged" && record["message_id"] == discardID {
			t.Fatal("permanent failure logged as successful delivery")
		}
		if record["event"] == "delivery.discarded" && record["message_id"] == discardID {
			break
		}
	}
	w = call(t, g, "SetSubscriptionAttributes", url.Values{"SubscriptionArn": {sub.ARN}, "AttributeName": {"FilterPolicy"}, "AttributeValue": {`{"kind":["filter-policy-secret"]}`}})
	success(t, w)
	w = call(t, g, "Publish", url.Values{"TopicArn": {arn}, "Message": {"filtered-payload-secret"}})
	success(t, w)
	filteredID := xmlValue(t, w, "MessageId")
	for {
		record := receive(t, logs.events)
		if record["event"] == "delivery.attempt" && record["message_id"] == filteredID {
			t.Fatal("filtered message reached the webhook")
		}
		if record["event"] == "delivery.filtered" && record["message_id"] == filteredID {
			if record["subscription_arn"] != sub.ARN || record["scope"] != filterAttributes {
				t.Fatal("filtered event missing subscription or scope", record)
			}
			break
		}
	}
	stop()
	for _, secret := range []string{"test-secret", "payload-secret", "attribute-secret", "discard-secret", "filter-policy-secret", "filtered-payload-secret", "endpoint-path-secret", "endpoint-query-secret", sub.Token, "AWS4-HMAC-SHA256"} {
		if strings.Contains(logs.text(), secret) {
			t.Errorf("log exposed a secret: %q", secret)
		}
	}
}

func TestDebugEventsDisabledAtInfoLevel(t *testing.T) {
	var output bytes.Buffer
	g := newTestGateway(t, memory.New(), nil)
	g.opts.Logger = slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelInfo}))
	createTopic(t, g, "quiet")
	if output.Len() != 0 {
		t.Fatalf("debug events logged at info level: %s", output.String())
	}
}
