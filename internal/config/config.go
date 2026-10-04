package config

import (
	"errors"
	"flag"
	"io"
)

type Config struct {
	Backend, Listen, Region, AccountID, PublicURL      string
	AccessKey, SecretKey                               string
	NATSURL, NATSNamespace, NATSCredentials, NATSToken string
	NATSReplicas                                       int
	NATSMaxBytes                                       int64
	CertFile, KeyFile                                  string
	Debug                                              bool
}

func Parse(args []string, getenv func(string) string, output io.Writer) (Config, error) {
	env := func(key, fallback string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return fallback
	}
	c := Config{AccessKey: getenv("SNS_ACCESS_KEY"), SecretKey: getenv("SNS_SECRET_KEY"), NATSToken: getenv("NATS_TOKEN")}
	f := flag.NewFlagSet("gateway", flag.ContinueOnError)
	f.SetOutput(output)
	f.BoolVar(&c.Debug, "debug", false, "log API and delivery events at debug level")
	f.StringVar(&c.Backend, "backend", env("SNS_BACKEND", "nats"), "nats or memory")
	f.StringVar(&c.Listen, "listen", env("SNS_LISTEN", "127.0.0.1:9001"), "HTTP listen address")
	f.StringVar(&c.Region, "region", env("SNS_REGION", "us-east-1"), "AWS signing region")
	f.StringVar(&c.AccountID, "account-id", env("SNS_ACCOUNT_ID", "000000000000"), "12-digit account ID used in ARNs")
	f.StringVar(&c.PublicURL, "public-url", env("SNS_PUBLIC_URL", "http://127.0.0.1:9001"), "URL reachable by subscribers for confirmation and certificates")
	// The URL is environment-only because it may include broker credentials.
	c.NATSURL = env("NATS_URL", "nats://127.0.0.1:4222")
	f.StringVar(&c.NATSNamespace, "nats-namespace", env("NATS_NAMESPACE", "sns"), "namespace for gateway-owned NATS resources")
	f.StringVar(&c.NATSCredentials, "nats-credentials", getenv("NATS_CREDENTIALS"), "NATS credentials file")
	f.IntVar(&c.NATSReplicas, "nats-replicas", 1, "JetStream storage replicas")
	f.Int64Var(&c.NATSMaxBytes, "nats-max-bytes", 1024*1024*1024, "maximum queued message bytes")
	f.StringVar(&c.CertFile, "tls-cert", getenv("SNS_TLS_CERT"), "HTTPS certificate file")
	f.StringVar(&c.KeyFile, "tls-key", getenv("SNS_TLS_KEY"), "HTTPS key file")
	if err := f.Parse(args); err != nil {
		return c, err
	}
	if f.NArg() != 0 {
		return c, errors.New("unexpected positional arguments")
	}
	if c.Backend != "nats" && c.Backend != "memory" {
		return c, errors.New("backend must be nats or memory")
	}
	if c.AccessKey == "" || c.SecretKey == "" {
		return c, errors.New("SNS_ACCESS_KEY and SNS_SECRET_KEY are required")
	}
	if (c.CertFile == "") != (c.KeyFile == "") {
		return c, errors.New("both TLS certificate and key are required")
	}
	return c, nil
}
