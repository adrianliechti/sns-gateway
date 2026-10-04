package config

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
)

func TestCredentialsAndHelp(t *testing.T) {
	env := map[string]string{"SNS_ACCESS_KEY": "access-secret", "SNS_SECRET_KEY": "signing-secret", "NATS_TOKEN": "broker-secret", "NATS_URL": "nats://user:password-secret@localhost:4222"}
	get := func(key string) string { return env[key] }
	var help bytes.Buffer
	if _, err := Parse([]string{"-help"}, get, &help); !errors.Is(err, flag.ErrHelp) {
		t.Fatal(err)
	}
	for _, secret := range env {
		if strings.Contains(help.String(), secret) {
			t.Fatal("help exposed credentials")
		}
	}
	c, err := Parse(nil, get, io.Discard)
	if err != nil || c.Debug || c.Backend != "nats" || c.Listen != "127.0.0.1:9001" {
		t.Fatalf("%+v %v", c, err)
	}
	c, err = Parse([]string{"--debug"}, get, io.Discard)
	if err != nil || !c.Debug {
		t.Fatalf("--debug: %+v %v", c, err)
	}
	if _, err := Parse([]string{"-backend", "unknown"}, get, io.Discard); err == nil {
		t.Fatal("unknown backend accepted")
	}
	if _, err := Parse([]string{"-tls-cert", "cert.pem"}, get, io.Discard); err == nil {
		t.Fatal("missing TLS key accepted")
	}
	delete(env, "SNS_SECRET_KEY")
	if _, err := Parse(nil, get, io.Discard); err == nil {
		t.Fatal("missing credentials accepted")
	}
}
