// Package gateway implements the SNS AWS Query API and HTTP delivery workers.
package gateway

import (
	"context"
	"crypto/rsa"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"time"

	"github.com/adrianliechti/sns-gateway/backend"
)

const namespace = "http://sns.amazonaws.com/doc/2010-03-31/"
const maxMessageSize = 256 * 1024

type Options struct {
	Region     string
	AccountID  string
	AccessKey  string
	SecretKey  string
	PublicURL  string
	HTTPClient *http.Client
	Logger     *slog.Logger
	RetryDelay time.Duration
}

type Gateway struct {
	be          backend.Backend
	opts        Options
	mu          sync.Mutex
	signingKey  *rsa.PrivateKey
	signingCert []byte
}

func New(be backend.Backend, opts Options) (*Gateway, error) {
	if be == nil {
		return nil, errors.New("backend is required")
	}
	if opts.Region == "" {
		opts.Region = "us-east-1"
	}
	if opts.AccountID == "" {
		opts.AccountID = "000000000000"
	}
	if !regexp.MustCompile(`^[a-z0-9-]+$`).MatchString(opts.Region) || !regexp.MustCompile(`^[0-9]{12}$`).MatchString(opts.AccountID) {
		return nil, errors.New("invalid region or account ID")
	}
	if opts.AccessKey == "" || opts.SecretKey == "" {
		return nil, errors.New("SNS_ACCESS_KEY and SNS_SECRET_KEY are required")
	}
	u, err := url.Parse(opts.PublicURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("public URL must be an absolute HTTP(S) URL without path, credentials, query or fragment")
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{}
	}
	client := *opts.HTTPClient
	client.Timeout = 15 * time.Second
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	opts.HTTPClient = &client
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.RetryDelay == 0 {
		opts.RetryDelay = 5 * time.Second
	}
	if opts.RetryDelay < 0 {
		return nil, errors.New("retry delay must be positive")
	}
	g := &Gateway{be: be, opts: opts}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := g.initSigning(ctx); err != nil {
		return nil, err
	}
	return g, nil
}

type apiError struct {
	Code    string
	Message string
	Status  int
}

func (e *apiError) Error() string  { return e.Message }
func invalid(message string) error { return &apiError{"InvalidParameter", message, 400} }
func unsupported(message string) error {
	return &apiError{"InvalidParameter", "Unsupported: " + message, 400}
}
func notFound() error { return &apiError{"NotFound", "Resource does not exist", 404} }

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := backend.NewID()
	ctx := context.WithValue(r.Context(), requestIDKey{}, id)
	started := time.Now()
	response := &statusWriter{ResponseWriter: w}
	w = response
	action := ""
	defer func() {
		status := response.status
		if status == 0 {
			status = http.StatusOK
		}
		g.event(ctx, "api.response", "method", r.Method, "action", action, "status", status, "duration", time.Since(started))
	}()
	w.Header().Set("x-amzn-RequestId", id)
	if r.URL.Path == "/healthz" && r.Method == http.MethodGet {
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "ok\n")
		return
	}
	if r.URL.Path == "/SimpleNotificationService.pem" && r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "application/x-pem-file")
		_, _ = w.Write(g.signingCert)
		return
	}
	if r.URL.Path != "/" {
		g.writeError(w, id, notFound())
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		g.writeError(w, id, &apiError{"InvalidParameter", "Use GET or POST", 405})
		return
	}
	requestBody := r.Body
	if requestBody == nil {
		requestBody = http.NoBody
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, requestBody, 2*1024*1024))
	if err != nil {
		g.writeError(w, id, invalid("Request body is too large or unreadable"))
		return
	}
	if r.Method == http.MethodGet && len(body) != 0 {
		g.writeError(w, id, invalid("GET must not contain a body"))
		return
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		g.writeError(w, id, invalid("Malformed query"))
		return
	}
	if r.Method == http.MethodPost {
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/x-www-form-urlencoded" {
			g.writeError(w, id, invalid("Expected application/x-www-form-urlencoded"))
			return
		}
		form, err := url.ParseQuery(string(body))
		if err != nil {
			g.writeError(w, id, invalid("Malformed form"))
			return
		}
		for k, values := range form {
			q[k] = append(q[k], values...)
		}
	}
	for _, values := range q {
		if len(values) != 1 {
			g.writeError(w, id, invalid("Duplicate parameter"))
			return
		}
	}
	action = q.Get("Action")
	// Confirmation URLs carry an unguessable token and can be opened by the endpoint.
	confirmation := r.Method == http.MethodGet && (q.Get("Action") == "ConfirmSubscription" || (q.Get("Action") == "Unsubscribe" && q.Get("Token") != "")) && r.Header.Get("Authorization") == "" && q.Get("X-Amz-Algorithm") == ""
	if !confirmation {
		if err := g.authenticate(r, body); err != nil {
			g.writeError(w, id, err)
			return
		}
	}
	if q.Get("Version") != "2010-03-31" {
		g.writeError(w, id, invalid("Version must be 2010-03-31"))
		return
	}
	result, err := g.dispatch(ctx, q)
	if err != nil {
		g.writeError(w, id, err)
		return
	}
	g.logOperation(ctx, q, result)
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	enc := xml.NewEncoder(w)
	root := xml.StartElement{Name: xml.Name{Local: q.Get("Action") + "Response"}, Attr: []xml.Attr{{Name: xml.Name{Local: "xmlns"}, Value: namespace}}}
	_ = enc.EncodeToken(root)
	if result != nil {
		_ = enc.EncodeElement(result, xml.StartElement{Name: xml.Name{Local: q.Get("Action") + "Result"}})
	}
	_ = enc.EncodeElement(struct {
		ID string `xml:"RequestId"`
	}{id}, xml.StartElement{Name: xml.Name{Local: "ResponseMetadata"}})
	_ = enc.EncodeToken(root.End())
	_ = enc.Flush()
}

func (g *Gateway) writeError(w http.ResponseWriter, id string, err error) {
	e := &apiError{"InternalError", "Internal service error", 500}
	if !errors.As(err, &e) {
		e = &apiError{"InternalError", "Internal service error", 500}
		g.opts.Logger.Error("request failed", "request_id", id, "error", err)
	}
	if errors.Is(err, backend.ErrConflict) {
		e = &apiError{"InternalError", "Concurrent state change; retry request", 503}
	}
	g.opts.Logger.Debug("SNS event", "event", "api.error", "request_id", id, "code", e.Code, "status", e.Status)
	kind := "Sender"
	if e.Status >= 500 {
		kind = "Receiver"
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(e.Status)
	_ = xml.NewEncoder(w).Encode(struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Error   struct {
			Type    string
			Code    string
			Message string
		}
		RequestID string `xml:"RequestId"`
	}{XMLNS: namespace, Error: struct {
		Type    string
		Code    string
		Message string
	}{kind, e.Code, e.Message}, RequestID: id})
}

func (g *Gateway) topicARN(name string) string {
	return fmt.Sprintf("arn:aws:sns:%s:%s:%s", g.opts.Region, g.opts.AccountID, name)
}
