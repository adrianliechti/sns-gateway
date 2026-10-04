# Go examples

These programs use the standard AWS SNS Go SDK against the gateway:

- [client/main.go](client/main.go) creates a topic if needed and publishes a message with a subject and a message attribute.
- [server/main.go](server/main.go) starts an HTTP webhook, subscribes it, confirms the subscription, and prints incoming messages.
- [filtering/main.go](filtering/main.go) subscribes with a filter policy, then publishes matching and nonmatching events using either message attributes or a nested JSON body.

The shared [client setup](internal/example/client.go) configures the gateway endpoint, region, and credentials. The [signature verifier](internal/example/verify.go) checks incoming envelopes using the certificate fetched from the configured gateway. It never follows URLs supplied in webhook messages.

The server and filtering example share the [webhook subscriber](internal/example/subscriber.go), which handles signature verification, confirmation, and shutdown cleanup.

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

## Filtering example

With `task dev` running in another terminal:

```sh
task example-filtering
```

The example listens on `127.0.0.1:8081`, confirms its own subscription, and publishes three orders. Its policy requires `event=order.created` and `total>=100`:

| Order | Event | Total | Expected delivery |
| --- | --- | --- | --- |
| `matching-order` | `order.created` | 150 | Yes |
| `wrong-event` | `order.cancelled` | 150 | No |
| `below-threshold` | `order.created` | 50 | No |

All three publishes return a message ID, but only `matching-order` should appear in a `Received message ...` line. Ctrl+C unsubscribes and exits. To try the same selection using a nested JSON body, stop the first run, then run:

```sh
task example-filtering -- --scope MessageBody
```

The default `MessageAttributes` mode sends String and Number attributes. `MessageBody` mode publishes no message attributes and filters `order.event` and `order.total` through a nested policy. Both modes send the same JSON body. The example shares the default `example-events` topic; use `--topic filtering-demo` to isolate it from the regular server example, which would otherwise also receive these messages.

Without Task, run `go run ./filtering` or `go run ./filtering --scope MessageBody` from this directory. With a gateway in Docker Desktop, use `--listen 0.0.0.0:8081 --callback-url http://host.docker.internal:8081/sns`.

## Configuration

All programs accept `--endpoint`, `--region`, and `--topic`. Endpoint and region also default from `SNS_ENDPOINT` and `SNS_REGION`. The client additionally accepts `--message` and `--subject`; both subscribers accept `--listen` and `--callback-url`. The filtering example also accepts `--scope`.

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
