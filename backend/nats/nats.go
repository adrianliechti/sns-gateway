// Package nats persists SNS state and independent subscription cursors in JetStream.
package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/adrianliechti/sns-gateway/backend"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const maxStateSize = 768 * 1024

type Options struct {
	URL             string
	Namespace       string
	CredentialsFile string
	Token           string
	Replicas        int
	MaxBytes        int64
	Logger          *slog.Logger
}

type Backend struct {
	nc     *natsgo.Conn
	js     jetstream.JetStream
	kv     jetstream.KeyValue
	stream string
	prefix string
}

func New(ctx context.Context, opts Options) (_ *Backend, err error) {
	if opts.URL == "" {
		opts.URL = natsgo.DefaultURL
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Namespace == "" {
		opts.Namespace = "sns"
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`).MatchString(opts.Namespace) {
		return nil, errors.New("invalid NATS namespace")
	}
	if opts.Replicas == 0 {
		opts.Replicas = 1
	}
	if opts.Replicas < 1 || opts.Replicas > 5 {
		return nil, errors.New("NATS replicas must be 1–5")
	}
	if opts.MaxBytes == 0 {
		opts.MaxBytes = 1024 * 1024 * 1024
	}
	if opts.MaxBytes < 2*1024*1024 {
		return nil, errors.New("NATS max bytes must be at least 2 MiB")
	}
	options := []natsgo.Option{natsgo.Name("sns-gateway"), natsgo.Timeout(5 * time.Second), natsgo.MaxReconnects(-1)}
	options = append(options,
		natsgo.DisconnectErrHandler(func(_ *natsgo.Conn, err error) {
			opts.Logger.Debug("NATS event", "event", "nats.disconnected", "namespace", opts.Namespace, "had_error", err != nil)
		}),
		natsgo.ReconnectHandler(func(_ *natsgo.Conn) {
			opts.Logger.Debug("NATS event", "event", "nats.reconnected", "namespace", opts.Namespace)
		}),
		natsgo.ClosedHandler(func(_ *natsgo.Conn) {
			opts.Logger.Debug("NATS event", "event", "nats.closed", "namespace", opts.Namespace)
		}),
	)
	if opts.CredentialsFile != "" && opts.Token != "" {
		return nil, errors.New("use either NATS credentials file or token")
	}
	if opts.CredentialsFile != "" {
		options = append(options, natsgo.UserCredentials(opts.CredentialsFile))
	}
	if opts.Token != "" {
		options = append(options, natsgo.Token(opts.Token))
	}
	nc, err := natsgo.Connect(opts.URL, options...)
	if err != nil {
		return nil, errors.New("unable to connect to NATS")
	}
	defer func() {
		if err != nil {
			nc.Close()
		}
	}()
	if nc.MaxPayload() < 2*1024*1024 {
		return nil, errors.New("NATS max_payload must be at least 2 MiB")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	b := &Backend{nc: nc, js: js, stream: opts.Namespace + "_messages", prefix: opts.Namespace}
	// Create is idempotent for matching configuration and refuses to silently
	// mutate a pre-existing stream owned by another application.
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: b.stream, Subjects: []string{b.prefix + ".topic.*", b.prefix + ".confirm.*"}, Retention: jetstream.InterestPolicy, Storage: jetstream.FileStorage, Replicas: opts.Replicas, Discard: jetstream.DiscardNew, MaxBytes: opts.MaxBytes, MaxMsgSize: 2 * 1024 * 1024})
	if err != nil {
		return nil, fmt.Errorf("initialize message stream: %w", err)
	}
	b.kv, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: opts.Namespace + "_state", History: 1, Storage: jetstream.FileStorage, Replicas: opts.Replicas, MaxValueSize: maxStateSize})
	if err != nil {
		return nil, fmt.Errorf("initialize metadata bucket: %w", err)
	}
	opts.Logger.Debug("NATS event", "event", "nats.ready", "namespace", opts.Namespace, "stream", b.stream, "replicas", opts.Replicas)
	return b, nil
}

func (b *Backend) Load(ctx context.Context) (backend.State, uint64, error) {
	e, err := b.kv.Get(ctx, "state")
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return backend.NewState(), 0, nil
	}
	if err != nil {
		return backend.State{}, 0, err
	}
	var state backend.State
	if err := json.Unmarshal(e.Value(), &state); err != nil {
		return state, 0, err
	}
	if state.Topics == nil || state.Subscriptions == nil {
		return state, 0, errors.New("invalid persisted SNS metadata")
	}
	return state, e.Revision(), nil
}

func (b *Backend) Save(ctx context.Context, state backend.State, revision uint64) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if len(data) > maxStateSize {
		return errors.New("SNS metadata capacity exceeded (768 KiB per namespace)")
	}
	if revision == 0 {
		_, err = b.kv.Create(ctx, "state", data)
	} else {
		_, err = b.kv.Update(ctx, "state", data, revision)
	}
	if errors.Is(err, jetstream.ErrKeyExists) {
		return backend.ErrConflict
	}
	return err
}

func (b *Backend) EnsureSubscription(ctx context.Context, s backend.Subscription) error {
	_, err := b.js.CreateConsumer(ctx, b.stream, jetstream.ConsumerConfig{Durable: s.ID, DeliverPolicy: jetstream.DeliverNewPolicy, AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Minute, MaxAckPending: 1, FilterSubjects: []string{b.prefix + ".topic." + s.TopicID, b.prefix + ".confirm." + s.ID}})
	return err
}

func (b *Backend) DeleteSubscription(ctx context.Context, s backend.Subscription) error {
	err := b.js.DeleteConsumer(ctx, b.stream, s.ID)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil
	}
	return err
}

func (b *Backend) Publish(ctx context.Context, m backend.Message) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	subject := b.prefix + ".topic." + m.TopicID
	if m.SubscriptionID != "" {
		subject = b.prefix + ".confirm." + m.SubscriptionID
	}
	_, err = b.js.Publish(ctx, subject, data, jetstream.WithMsgID(m.ID))
	return err
}

func (b *Backend) Consumer(ctx context.Context, s backend.Subscription) (backend.Consumer, error) {
	c, err := b.js.Consumer(ctx, b.stream, s.ID)
	if err != nil {
		return nil, err
	}
	return &consumer{c: c}, nil
}

type consumer struct{ c jetstream.Consumer }
type delivery struct {
	msg  jetstream.Msg
	data backend.Message
}

func (c *consumer) Next(ctx context.Context) (backend.Delivery, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := c.c.Fetch(1, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg, ok := <-batch.Messages():
			if !ok {
				if err := batch.Error(); err != nil && !errors.Is(err, natsgo.ErrTimeout) {
					return nil, err
				}
				continue
			}
			var data backend.Message
			if err := json.Unmarshal(msg.Data(), &data); err != nil {
				_ = msg.Term()
				return nil, fmt.Errorf("invalid stored message: %w", err)
			}
			return &delivery{msg: msg, data: data}, nil
		}
	}
}

func (d *delivery) Message() backend.Message        { return d.data }
func (d *delivery) Ack() error                      { return d.msg.Ack() }
func (d *delivery) Retry(delay time.Duration) error { return d.msg.NakWithDelay(delay) }
func (c *consumer) Close() error                    { return nil }
func (b *Backend) Close() error                     { b.nc.Close(); return nil }
