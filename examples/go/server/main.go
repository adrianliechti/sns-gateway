// The server subscribes an HTTP webhook and receives SNS notifications.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/adrianliechti/sns-gateway/examples/go/internal/example"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

func main() {
	options := example.Flags(flag.CommandLine)
	listen := flag.String("listen", "127.0.0.1:8080", "webhook HTTP listen address")
	callback := flag.String("callback-url", "http://127.0.0.1:8080/sns", "webhook URL reachable by the gateway")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *options, *listen, *callback); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func run(ctx context.Context, options example.Options, listen, callback string) error {
	callbackURL, err := url.Parse(callback)
	if err != nil || callbackURL.Hostname() == "" || (callbackURL.Scheme != "http" && callbackURL.Scheme != "https") || callbackURL.Path != "/sns" || callbackURL.RawQuery != "" || callbackURL.Fragment != "" || callbackURL.User != nil {
		return errors.New("callback-url must be an HTTP(S) URL with path /sns and no credentials, query, or fragment")
	}
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client := options.Client()
	topic, err := client.CreateTopic(startup, &sns.CreateTopicInput{Name: aws.String(options.Topic)})
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	verify, err := example.LoadVerifier(startup, options.Endpoint)
	if err != nil {
		return err
	}

	confirmed := make(chan struct{})
	var once sync.Once
	ready := func() { once.Do(func() { close(confirmed) }) }
	mux := http.NewServeMux()
	mux.HandleFunc("POST /sns", func(w http.ResponseWriter, r *http.Request) {
		var notification example.Notification
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2*1024*1024))
		if err := decoder.Decode(&notification); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			http.Error(w, "unexpected trailing data", http.StatusBadRequest)
			return
		}
		if notification.TopicARN != aws.ToString(topic.TopicArn) || verify(notification) != nil {
			http.Error(w, "invalid topic or signature", http.StatusForbidden)
			return
		}
		switch notification.Type {
		case "SubscriptionConfirmation":
			// Use our configured SNS client rather than following SubscribeURL.
			result, err := client.ConfirmSubscription(r.Context(), &sns.ConfirmSubscriptionInput{TopicArn: topic.TopicArn, Token: aws.String(notification.Token)})
			if err != nil {
				log.Printf("Confirmation failed: %v", err)
				http.Error(w, "confirmation failed", http.StatusServiceUnavailable)
				return
			}
			log.Printf("Confirmed subscription %s", aws.ToString(result.SubscriptionArn))
			ready()
		case "Notification":
			// SNS delivery is at least once. Real handlers should use MessageId to
			// make side effects idempotent, and acknowledge only after processing.
			log.Printf("Received message %s: %q", notification.MessageID, notification.Message)
		}
		w.WriteHeader(http.StatusNoContent)
	})

	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(listener) }()
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
		}
	}()
	log.Printf("Webhook listening on %s; callback %s", listener.Addr(), callback)

	subscription, err := client.Subscribe(startup, &sns.SubscribeInput{
		TopicArn:              topic.TopicArn,
		Protocol:              aws.String(callbackURL.Scheme),
		Endpoint:              aws.String(callback),
		ReturnSubscriptionArn: true,
	})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := client.Unsubscribe(cleanup, &sns.UnsubscribeInput{SubscriptionArn: subscription.SubscriptionArn}); err != nil {
			log.Printf("Unsubscribe failed: %v", err)
		}
	}()
	// An earlier run may have left this same endpoint already confirmed.
	attributes, err := client.GetSubscriptionAttributes(startup, &sns.GetSubscriptionAttributesInput{SubscriptionArn: subscription.SubscriptionArn})
	if err != nil {
		return fmt.Errorf("get subscription: %w", err)
	}
	if attributes.Attributes["PendingConfirmation"] == "false" {
		ready()
	}
	select {
	case <-confirmed:
		log.Printf("Ready on topic %s; run task example-client", aws.ToString(topic.TopicArn))
	case <-startup.Done():
		return fmt.Errorf("waiting for confirmation (check that the gateway can reach callback-url): %w", startup.Err())
	case err := <-serverDone:
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-serverDone:
		return err
	}
}
