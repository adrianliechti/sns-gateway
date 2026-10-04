package example

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Notification is the SNS envelope used for both confirmation and delivery.
type Notification struct {
	Type             string  `json:"Type"`
	MessageID        string  `json:"MessageId"`
	TopicARN         string  `json:"TopicArn"`
	Subject          *string `json:"Subject"`
	Message          string  `json:"Message"`
	Timestamp        string  `json:"Timestamp"`
	Token            string  `json:"Token"`
	SubscribeURL     string  `json:"SubscribeURL"`
	SignatureVersion string  `json:"SignatureVersion"`
	Signature        string  `json:"Signature"`
}

// LoadVerifier trusts the configured gateway's certificate. It never fetches a
// URL supplied by a notification. Use a trusted HTTPS gateway outside local dev.
func LoadVerifier(ctx context.Context, endpoint string) (func(Notification) error, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+"/SimpleNotificationService.pem", nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch gateway signing certificate: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("signing certificate returned HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("gateway returned an invalid signing certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := cert.PublicKey.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("gateway signing certificate is not RSA")
	}

	return func(n Notification) error {
		if time.Now().Before(cert.NotBefore) || time.Now().After(cert.NotAfter) {
			return errors.New("signing certificate is outside its validity period")
		}
		if n.SignatureVersion != "2" || n.MessageID == "" {
			return errors.New("invalid signature metadata")
		}
		if _, err := time.Parse(time.RFC3339Nano, n.Timestamp); err != nil {
			return errors.New("invalid notification timestamp")
		}
		var canonical strings.Builder
		field := func(name, value string) { canonical.WriteString(name + "\n" + value + "\n") }
		field("Message", n.Message)
		field("MessageId", n.MessageID)
		switch n.Type {
		case "Notification":
			if n.Subject != nil {
				field("Subject", *n.Subject)
			}
		case "SubscriptionConfirmation":
			field("SubscribeURL", n.SubscribeURL)
		default:
			return errors.New("unsupported message type")
		}
		field("Timestamp", n.Timestamp)
		if n.Type == "SubscriptionConfirmation" {
			field("Token", n.Token)
		}
		field("TopicArn", n.TopicARN)
		field("Type", n.Type)
		digest := sha256.Sum256([]byte(canonical.String()))
		signature, err := base64.StdEncoding.DecodeString(n.Signature)
		if err != nil {
			return errors.New("invalid signature encoding")
		}
		return rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature)
	}, nil
}
