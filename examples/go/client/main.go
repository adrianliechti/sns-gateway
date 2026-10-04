// The client publishes a message using the ordinary AWS SNS Go SDK.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/adrianliechti/sns-gateway/examples/go/internal/example"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
)

func main() {
	options := example.Flags(flag.CommandLine)
	message := flag.String("message", `{"order_id":"123","status":"created"}`, "message body to publish")
	subject := flag.String("subject", "Order created", "notification subject")
	flag.Parse()
	if err := run(*options, *message, *subject); err != nil {
		log.Fatal(err)
	}
}

func run(options example.Options, message, subject string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := options.Client()

	// CreateTopic is idempotent, so the client and server can share a topic name.
	topic, err := client.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(options.Topic)})
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}

	result, err := client.Publish(ctx, &sns.PublishInput{
		TopicArn: topic.TopicArn,
		Subject:  aws.String(subject),
		Message:  aws.String(message),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"source": {DataType: aws.String("String"), StringValue: aws.String("go-example")},
		},
	})
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	log.Printf("Published message %s to %s", aws.ToString(result.MessageId), aws.ToString(topic.TopicArn))
	return nil
}
