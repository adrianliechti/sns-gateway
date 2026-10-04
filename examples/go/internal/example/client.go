// Package example contains the shared setup for the client and webhook examples.
package example

import (
	"context"
	"flag"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type Options struct {
	Endpoint string
	Region   string
	Topic    string
}

func Flags(f *flag.FlagSet) *Options {
	o := &Options{}
	f.StringVar(&o.Endpoint, "endpoint", env("SNS_ENDPOINT", "http://127.0.0.1:9001"), "SNS gateway endpoint")
	f.StringVar(&o.Region, "region", env("SNS_REGION", "us-east-1"), "AWS signing region")
	f.StringVar(&o.Topic, "topic", "example-events", "shared topic name")
	return o
}

func (o Options) Client() *sns.Client {
	// Match task dev by default. Credentials stay in the environment, not flags.
	credentials := aws.Credentials{
		AccessKeyID:     env("SNS_ACCESS_KEY", "gateway-local"),
		SecretAccessKey: env("SNS_SECRET_KEY", "gateway-local-dev-secret"),
	}
	return sns.New(sns.Options{
		BaseEndpoint: aws.String(strings.TrimRight(o.Endpoint, "/")),
		Region:       o.Region,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return credentials, nil
		}),
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
	})
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
