// The filtering example subscribes a webhook and publishes three events. Only
// the order.created event with a total of at least 100 should reach the webhook.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/adrianliechti/sns-gateway/examples/go/internal/example"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
)

func main() {
	options := example.Flags(flag.CommandLine)
	scope := flag.String("scope", "MessageAttributes", "filter scope: MessageAttributes or MessageBody")
	listen := flag.String("listen", "127.0.0.1:8081", "webhook HTTP listen address")
	callback := flag.String("callback-url", "http://127.0.0.1:8081/sns", "webhook URL reachable by the gateway")
	flag.Parse()
	policy := `{"event":["order.created"],"total":[{"numeric":[">=",100]}]}`
	switch *scope {
	case "MessageAttributes":
	case "MessageBody":
		policy = `{"order":{"event":["order.created"],"total":[{"numeric":[">=",100]}]}}`
	default:
		log.Fatal("scope must be MessageAttributes or MessageBody")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("Filtering %s with %s", *scope, policy)
	err := example.RunSubscriber(ctx, *options, example.SubscriberOptions{
		Listen: *listen, CallbackURL: *callback,
		Attributes: map[string]string{"FilterPolicy": policy, "FilterPolicyScope": *scope},
		OnReady: func(ctx context.Context, client *sns.Client, topicARN string) error {
			return publishExamples(ctx, client, topicARN, *scope)
		},
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func publishExamples(ctx context.Context, client *sns.Client, topicARN, scope string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for _, order := range []struct {
		ID    string `json:"id"`
		Event string `json:"event"`
		Total int    `json:"total"`
	}{
		{"matching-order", "order.created", 150},
		{"wrong-event", "order.cancelled", 150},
		{"below-threshold", "order.created", 50},
	} {
		body, err := json.Marshal(map[string]any{"order": order})
		if err != nil {
			return err
		}
		input := &sns.PublishInput{TopicArn: aws.String(topicARN), Message: aws.String(string(body))}
		if scope == "MessageAttributes" {
			input.MessageAttributes = map[string]types.MessageAttributeValue{
				"event": {DataType: aws.String("String"), StringValue: aws.String(order.Event)},
				"total": {DataType: aws.String("Number"), StringValue: aws.String(strconv.Itoa(order.Total))},
			}
		}
		result, err := client.Publish(ctx, input)
		if err != nil {
			return fmt.Errorf("publish %s: %w", order.ID, err)
		}
		log.Printf("Published %s: %s", order.ID, aws.ToString(result.MessageId))
	}
	log.Print("Only matching-order should be received. Press Ctrl+C to unsubscribe and exit.")
	return nil
}
