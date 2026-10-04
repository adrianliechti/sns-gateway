// Package backend defines the persistence and delivery contract of the gateway.
package backend

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"
)

var (
	ErrConflict = errors.New("state revision conflict")
	ErrNotFound = errors.New("not found")
)

type Topic struct {
	ARN         string
	ID          string // Unique generation, so deleting and recreating a name cannot replay messages.
	Name        string
	DisplayName string
	Tags        map[string]string
}

type Subscription struct {
	ARN               string
	ID                string
	TopicARN          string
	TopicID           string
	Protocol          string
	Endpoint          string
	Token             string
	CreatedAt         time.Time
	ConfirmedAt       time.Time
	Raw               bool
	FilterPolicy      string `json:",omitempty"`
	FilterPolicyScope string `json:",omitempty"`
}

type State struct {
	Topics         map[string]Topic
	Subscriptions  map[string]Subscription
	PendingDeletes map[string]Subscription
	SigningKey     []byte
	SigningCert    []byte
}

func NewState() State {
	return State{Topics: map[string]Topic{}, Subscriptions: map[string]Subscription{}}
}

type MessageAttribute struct {
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

type Message struct {
	ID             string
	TopicARN       string
	TopicID        string
	SubscriptionID string // Set only for subscription confirmation messages.
	Type           string
	Body           string
	Subject        string
	Attributes     map[string]MessageAttribute
	Timestamp      time.Time
}

// Backend stores metadata atomically and messages independently. EnsureSubscription
// must establish a durable cursor before Subscribe succeeds. Publish returns only
// after acceptance. Consumers fan out independently and acknowledge after delivery.
// A namespace is owned by one gateway process; CAS protects against lost updates.
type Backend interface {
	Load(context.Context) (State, uint64, error)
	Save(context.Context, State, uint64) error
	EnsureSubscription(context.Context, Subscription) error
	DeleteSubscription(context.Context, Subscription) error
	Publish(context.Context, Message) error
	Consumer(context.Context, Subscription) (Consumer, error)
	Close() error
}

type Consumer interface {
	Next(context.Context) (Delivery, error)
	Close() error
}

type Delivery interface {
	Message() Message
	Ack() error
	Retry(time.Duration) error
}

func NewID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
