// The server subscribes an HTTP webhook and receives SNS notifications.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/adrianliechti/sns-gateway/examples/go/internal/example"
)

func main() {
	options := example.Flags(flag.CommandLine)
	listen := flag.String("listen", "127.0.0.1:8080", "webhook HTTP listen address")
	callback := flag.String("callback-url", "http://127.0.0.1:8080/sns", "webhook URL reachable by the gateway")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := example.RunSubscriber(ctx, *options, example.SubscriberOptions{Listen: *listen, CallbackURL: *callback}); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
