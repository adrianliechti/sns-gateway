# Go client and server

Two small programs use the standard AWS SNS Go SDK against the gateway:

- [client/main.go](client/main.go) creates a topic if needed and publishes a message with a subject and a message attribute.
- [server/main.go](server/main.go) starts an HTTP webhook, subscribes it, confirms the subscription, and prints incoming messages.

The shared [client setup](internal/example/client.go) configures the gateway endpoint, region, and credentials. The [signature verifier](internal/example/verify.go) checks incoming envelopes using the certificate fetched from the configured gateway. It never follows URLs supplied in webhook messages.

## Run in three terminals

Run these commands from the repository root. First start the gateway and NATS:

```sh
task dev
```

Then start the subscriber:

```sh
task example-server
```

Wait for `Ready on topic ...`, then publish from another terminal:

```sh
task example-client
task example-client -- --message 'Hello from Go!'
task example-client -- --subject 'Order updated' --message '{"order_id":"123","status":"shipped"}'
```

The client prints a message ID. The server prints the same ID and the received body:

```text
Published message <id> to arn:aws:sns:us-east-1:000000000000:example-events
Received message <id>: "Hello from Go!"
```

The defaults match `task dev`: gateway `http://127.0.0.1:9001`, region `us-east-1`, topic `example-events`, and development credentials `gateway-local` / `gateway-local-dev-secret`. The Task commands load the repository `.env`; exported environment variables take precedence. Set `SNS_ACCESS_KEY` and `SNS_SECRET_KEY` to match a gateway with different credentials.

The server listens on `127.0.0.1:8080` and registers `http://127.0.0.1:8080/sns`. It validates the topic and RSA signature before confirming or processing a notification. Ctrl+C unsubscribes its webhook and shuts down the HTTP server. The shared topic is left in place. Each program only contacts the configured gateway; it does not require an AWS account.

## Configuration

Both programs accept `--endpoint`, `--region`, and `--topic`. Endpoint and region also default from `SNS_ENDPOINT` and `SNS_REGION`. The client additionally accepts `--message` and `--subject`; the server accepts `--listen` and `--callback-url`.

```sh
task example-server -- --endpoint http://127.0.0.1:9010 --topic orders
task example-client -- --endpoint http://127.0.0.1:9010 --topic orders --message 'hello'
```

If the gateway itself runs in Docker Compose, it must reach the host webhook. With Docker Desktop:

```sh
task example-server -- --listen 0.0.0.0:8080 --callback-url http://host.docker.internal:8080/sns
```

The example server serves plain HTTP. An HTTPS callback URL requires a TLS reverse proxy forwarding `/sns` to this server. For a remote gateway, use its trusted HTTPS endpoint so fetching the signing certificate is authenticated.

Without Task, run from this directory and export any custom credentials yourself:

```sh
go run ./server
# In another terminal:
go run ./client --message 'Hello from Go!'
```

Messages published before the server confirms its subscription are not replayed. SNS delivery is at least once; this example prints each delivery. An application that changes data should deduplicate by message ID or make its processing idempotent before returning success.

This directory has its own Go module so the example's SNS SDK dependency stays separate from the gateway's runtime dependencies.
