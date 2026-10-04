package gateway

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/adrianliechti/sns-gateway/backend"
)

type requestIDKey struct{}

// Keep log fields explicit: request forms, message bodies/attributes, endpoint
// URLs and confirmation tokens can contain application secrets.
func (g *Gateway) event(ctx context.Context, name string, fields ...any) {
	if !g.opts.Logger.Enabled(ctx, slog.LevelDebug) {
		return
	}
	attributes := []any{"event", name}
	if id, ok := ctx.Value(requestIDKey{}).(string); ok {
		attributes = append(attributes, "request_id", id)
	}
	attributes = append(attributes, fields...)
	g.opts.Logger.DebugContext(ctx, "SNS event", attributes...)
}

func (g *Gateway) logOperation(ctx context.Context, q url.Values, result any) {
	fields := []any{"action", q.Get("Action")}
	topic, subscription := q.Get("TopicArn"), q.Get("SubscriptionArn")
	if topic == "" {
		topic = q.Get("ResourceArn")
	}
	switch result := result.(type) {
	case arnResult:
		topic = result.ARN
	case subscriptionResult:
		subscription = result.ARN
	case publishResult:
		fields = append(fields, "message_id", result.ID)
	case batchResult:
		fields = append(fields, "successful", len(result.Successful), "failed", len(result.Failed))
	}
	if topic != "" {
		fields = append(fields, "topic_arn", topic)
	}
	if subscription != "" {
		fields = append(fields, "subscription_arn", subscription)
	}
	g.event(ctx, "api.operation", fields...)
}

func (g *Gateway) publish(ctx context.Context, m backend.Message) error {
	if err := g.be.Publish(ctx, m); err != nil {
		return err
	}
	g.event(ctx, "message.accepted", "message_id", m.ID, "topic_arn", m.TopicARN, "type", m.Type, "bytes", messageSize(m))
	return nil
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status != 0 {
		return
	}
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
