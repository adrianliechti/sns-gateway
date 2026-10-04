// Package memory provides a volatile backend for development and tests.
package memory

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/adrianliechti/sns-gateway/backend"
)

type Backend struct {
	mu       sync.Mutex
	state    backend.State
	revision uint64
	queues   map[string]*queue
}

type queue struct {
	sub   backend.Subscription
	items []*item
}

type item struct {
	message   backend.Message
	available time.Time
	lease     uint64
}

func New() *Backend { return &Backend{state: backend.NewState(), queues: map[string]*queue{}} }

func clone[T any](v T) T {
	b, _ := json.Marshal(v)
	var copy T
	_ = json.Unmarshal(b, &copy)
	return copy
}

func (b *Backend) Load(ctx context.Context) (backend.State, uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return clone(b.state), b.revision, ctx.Err()
}

func (b *Backend) Save(ctx context.Context, s backend.State, revision uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if revision != b.revision {
		return backend.ErrConflict
	}
	b.state = clone(s)
	b.revision++
	return nil
}

func (b *Backend) EnsureSubscription(ctx context.Context, s backend.Subscription) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.queues[s.ID] == nil {
		b.queues[s.ID] = &queue{sub: s}
	}
	return nil
}

func (b *Backend) DeleteSubscription(ctx context.Context, s backend.Subscription) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.queues, s.ID)
	return nil
}

func (b *Backend) Publish(ctx context.Context, m backend.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, q := range b.queues {
		if m.SubscriptionID == q.sub.ID || (m.SubscriptionID == "" && m.TopicID == q.sub.TopicID) {
			q.items = append(q.items, &item{message: clone(m)})
		}
	}
	return nil
}

type consumer struct {
	b  *Backend
	id string
}
type delivery struct {
	b     *Backend
	id    string
	item  *item
	lease uint64
}

func (b *Backend) Consumer(ctx context.Context, s backend.Subscription) (backend.Consumer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &consumer{b, s.ID}, nil
}

func (c *consumer) Next(ctx context.Context) (backend.Delivery, error) {
	timer := time.NewTicker(10 * time.Millisecond)
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.b.mu.Lock()
		q := c.b.queues[c.id]
		if q == nil {
			c.b.mu.Unlock()
			return nil, backend.ErrNotFound
		}
		for _, i := range q.items {
			if !time.Now().Before(i.available) {
				i.available = time.Now().Add(time.Minute)
				i.lease++
				d := &delivery{c.b, c.id, i, i.lease}
				c.b.mu.Unlock()
				return d, nil
			}
		}
		c.b.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (d *delivery) Message() backend.Message { return clone(d.item.message) }
func (d *delivery) Ack() error {
	d.b.mu.Lock()
	defer d.b.mu.Unlock()
	if q := d.b.queues[d.id]; q != nil && d.item.lease == d.lease {
		for i, item := range q.items {
			if item == d.item {
				q.items = append(q.items[:i], q.items[i+1:]...)
				break
			}
		}
	}
	return nil
}
func (d *delivery) Retry(delay time.Duration) error {
	d.b.mu.Lock()
	defer d.b.mu.Unlock()
	if d.item.lease == d.lease {
		d.item.available = time.Now().Add(delay)
	}
	return nil
}
func (c *consumer) Close() error { return nil }
func (b *Backend) Close() error  { return nil }
