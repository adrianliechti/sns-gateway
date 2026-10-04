package gateway

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"math/big"
	"strings"
	"time"
)

// A namespace owns one persistent signing identity. The certificate is served at
// the gateway's public URL; consumers must explicitly trust this custom endpoint.
func (g *Gateway) initSigning(ctx context.Context) error {
	s, revision, err := g.be.Load(ctx)
	if err != nil {
		return err
	}
	if len(s.SigningKey) == 0 {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return err
		}
		serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
		if err != nil {
			return err
		}
		template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "SNS Gateway"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().AddDate(10, 0, 0), KeyUsage: x509.KeyUsageDigitalSignature}
		der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
		if err != nil {
			return err
		}
		s.SigningKey = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
		s.SigningCert = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		if err := g.be.Save(ctx, s, revision); err != nil {
			return err
		}
	}
	keyBlock, _ := pem.Decode(s.SigningKey)
	certBlock, _ := pem.Decode(s.SigningCert)
	if keyBlock == nil || certBlock == nil {
		return errors.New("invalid persisted signing identity")
	}
	key, err := x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
	if err != nil {
		return err
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return err
	}
	pub, ok := cert.PublicKey.(*rsa.PublicKey)
	if !ok || !pub.Equal(&key.PublicKey) {
		return errors.New("persisted signing key and certificate do not match")
	}
	if time.Now().After(cert.NotAfter) {
		return errors.New("persisted signing certificate has expired")
	}
	g.signingKey = key
	g.signingCert = s.SigningCert
	return nil
}

func (g *Gateway) signEnvelope(e *envelope) error {
	var canonical strings.Builder
	field := func(name, value string) { canonical.WriteString(name + "\n" + value + "\n") }
	field("Message", e.Message)
	field("MessageId", e.MessageID)
	if e.Type == "Notification" {
		if e.Subject != "" {
			field("Subject", e.Subject)
		}
	} else {
		field("SubscribeURL", e.SubscribeURL)
	}
	field("Timestamp", e.Timestamp)
	if e.Type != "Notification" {
		field("Token", e.Token)
	}
	field("TopicArn", e.TopicARN)
	field("Type", e.Type)
	digest := sha256.Sum256([]byte(canonical.String()))
	sig, err := rsa.SignPKCS1v15(rand.Reader, g.signingKey, crypto.SHA256, digest[:])
	if err != nil {
		return err
	}
	e.SignatureVersion = "2"
	e.Signature = base64.StdEncoding.EncodeToString(sig)
	e.SigningCertURL = strings.TrimRight(g.opts.PublicURL, "/") + "/SimpleNotificationService.pem"
	return nil
}
