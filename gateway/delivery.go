package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/adrianliechti/sns-gateway/backend"
)

type envelope struct {
	Type              string                              `json:"Type"`
	MessageID         string                              `json:"MessageId"`
	TopicARN          string                              `json:"TopicArn"`
	Subject           string                              `json:"Subject,omitempty"`
	Message           string                              `json:"Message"`
	Timestamp         string                              `json:"Timestamp"`
	Token             string                              `json:"Token,omitempty"`
	SubscribeURL      string                              `json:"SubscribeURL,omitempty"`
	UnsubscribeURL    string                              `json:"UnsubscribeURL,omitempty"`
	SignatureVersion  string                              `json:"SignatureVersion"`
	Signature         string                              `json:"Signature"`
	SigningCertURL    string                              `json:"SigningCertURL"`
	MessageAttributes map[string]backend.MessageAttribute `json:"MessageAttributes,omitempty"`
}

// Run resumes every persisted subscription. Run exactly once per gateway and
// cancel its context at shutdown. Backend errors leave messages unacknowledged.
func (g *Gateway) Run(ctx context.Context) error {
	workers := map[string]context.CancelFunc{}
	var wg sync.WaitGroup
	defer func() {
		for _, cancel := range workers {
			cancel()
		}
		wg.Wait()
	}()
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	for {
		s, _, err := g.be.Load(ctx)
		if err != nil && ctx.Err() == nil {
			g.opts.Logger.Error("subscription reconciliation failed", "error", err)
		}
		if err == nil {
			if len(s.PendingDeletes) > 0 {
				if err := g.cleanup(ctx); err != nil && ctx.Err() == nil {
					g.opts.Logger.Error("subscription cleanup failed", "error", err)
				}
			}
			for arn, cancel := range workers {
				if _, exists := s.Subscriptions[arn]; !exists {
					cancel()
					delete(workers, arn)
				}
			}
			for arn, sub := range s.Subscriptions {
				if _, exists := workers[arn]; !exists {
					workerCtx, cancel := context.WithCancel(ctx)
					workers[arn] = cancel
					wg.Add(1)
					go func() { defer wg.Done(); g.worker(workerCtx, sub) }()
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (g *Gateway) worker(ctx context.Context, sub backend.Subscription) {
	g.event(ctx, "worker.started", "subscription_arn", sub.ARN)
	defer g.event(ctx, "worker.stopped", "subscription_arn", sub.ARN)
	for ctx.Err() == nil {
		err := g.consume(ctx, sub)
		if ctx.Err() != nil {
			return
		}
		g.opts.Logger.Error("subscription worker restarting", "subscription", sub.ARN, "error", err)
		if !pause(ctx, time.Second) {
			return
		}
	}
}

func (g *Gateway) consume(ctx context.Context, sub backend.Subscription) error {
	// The API establishes cursors; workers only bind to them. A removed consumer
	// must never be recreated by a stale worker racing an unsubscribe.
	c, err := g.be.Consumer(ctx, sub)
	if err != nil {
		return err
	}
	defer c.Close()
	for {
		d, err := c.Next(ctx)
		if err != nil {
			return err
		}
		m := d.Message()
		s, _, err := g.be.Load(ctx)
		if err != nil {
			return err
		}
		current, exists := s.Subscriptions[sub.ARN]
		if !exists || (m.Type == "Notification" && (current.ConfirmedAt.IsZero() || m.Timestamp.Before(current.ConfirmedAt))) || (m.Type == "SubscriptionConfirmation" && (!current.ConfirmedAt.IsZero() || time.Since(current.CreatedAt) > 48*time.Hour)) {
			if err := d.Ack(); err != nil {
				return err
			}
			g.event(ctx, "delivery.skipped", "subscription_arn", sub.ARN, "message_id", m.ID, "reason", "subscription_inactive_or_message_predates_confirmation")
			continue
		}
		retry, err := g.deliver(ctx, current, m)
		deliveryFailed := err != nil
		if err != nil {
			g.opts.Logger.Warn("delivery failed", "subscription", sub.ARN, "message_id", m.ID, "retry", retry, "error", err)
		}
		if retry {
			err = d.Retry(g.opts.RetryDelay)
		} else {
			err = d.Ack()
		}
		if err != nil {
			return err
		}
		switch {
		case retry:
			g.event(ctx, "delivery.retry_scheduled", "subscription_arn", sub.ARN, "message_id", m.ID, "delay", g.opts.RetryDelay)
		case deliveryFailed:
			g.event(ctx, "delivery.discarded", "subscription_arn", sub.ARN, "message_id", m.ID, "reason", "permanent_failure")
		default:
			g.event(ctx, "delivery.acknowledged", "subscription_arn", sub.ARN, "message_id", m.ID)
		}
	}
}

func (g *Gateway) deliver(ctx context.Context, sub backend.Subscription, m backend.Message) (bool, error) {
	body := []byte(m.Body)
	raw := sub.Raw && m.Type == "Notification"
	if !raw {
		e := envelope{Type: m.Type, MessageID: m.ID, TopicARN: m.TopicARN, Subject: m.Subject, Message: m.Body, Timestamp: m.Timestamp.UTC().Format(time.RFC3339Nano), MessageAttributes: m.Attributes}
		if m.Type == "SubscriptionConfirmation" {
			e.Token = sub.Token
			q := url.Values{"Action": {"ConfirmSubscription"}, "Version": {"2010-03-31"}, "TopicArn": {sub.TopicARN}, "Token": {sub.Token}}
			e.SubscribeURL = strings.TrimRight(g.opts.PublicURL, "/") + "/?" + q.Encode()
		} else {
			q := url.Values{"Action": {"Unsubscribe"}, "Version": {"2010-03-31"}, "SubscriptionArn": {sub.ARN}, "Token": {sub.Token}}
			e.UnsubscribeURL = strings.TrimRight(g.opts.PublicURL, "/") + "/?" + q.Encode()
		}
		if err := g.signEnvelope(&e); err != nil {
			return true, err
		}
		var err error
		body, err = json.Marshal(e)
		if err != nil {
			return true, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.Endpoint, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "text/plain; charset=UTF-8")
	req.Header.Set("x-amz-sns-message-type", m.Type)
	req.Header.Set("x-amz-sns-message-id", m.ID)
	req.Header.Set("x-amz-sns-topic-arn", m.TopicARN)
	req.Header.Set("x-amz-sns-subscription-arn", sub.ARN)
	if raw {
		req.Header.Set("x-amz-sns-rawdelivery", "true")
	}
	started := time.Now()
	g.event(ctx, "delivery.attempt", "subscription_arn", sub.ARN, "message_id", m.ID, "type", m.Type, "raw", raw)
	response, err := g.opts.HTTPClient.Do(req)
	if err != nil {
		return true, errors.New("HTTP endpoint request failed")
	} // URLs may contain endpoint secrets.
	defer response.Body.Close()
	g.event(ctx, "delivery.response", "subscription_arn", sub.ARN, "message_id", m.ID, "status", response.StatusCode, "duration", time.Since(started))
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return false, nil
	}
	return response.StatusCode == 429 || response.StatusCode >= 500, fmt.Errorf("HTTP status %d", response.StatusCode)
}

func pause(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// Commit logical deletion before deleting a cursor. If the broker or process
// fails during cleanup, the tombstone survives and is retried after restart.
func (g *Gateway) cleanup(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	s, revision, err := g.be.Load(ctx)
	if err != nil {
		return err
	}
	if len(s.PendingDeletes) == 0 {
		return nil
	}
	for arn, sub := range s.PendingDeletes {
		if err := g.be.DeleteSubscription(ctx, sub); err != nil {
			return err
		}
		g.event(ctx, "subscription.consumer_deleted", "subscription_arn", arn)
		delete(s.PendingDeletes, arn)
	}
	return g.be.Save(ctx, s, revision)
}
