package gateway

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adrianliechti/sns-gateway/backend"
	"github.com/adrianliechti/sns-gateway/backend/memory"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newTestGateway(t *testing.T, be backend.Backend, transport http.RoundTripper) *Gateway {
	t.Helper()
	g, err := New(be, Options{AccessKey: "test-access", SecretKey: "test-secret", PublicURL: "http://gateway", RetryDelay: 10 * time.Millisecond, HTTPClient: &http.Client{Transport: transport}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func signedRequest(t *testing.T, method string, q url.Values, at time.Time) *http.Request {
	t.Helper()
	q.Set("Version", "2010-03-31")
	endpoint := "http://gateway/"
	body := ""
	if method == http.MethodPost {
		body = q.Encode()
	} else {
		endpoint += "?" + q.Encode()
	}
	r, err := http.NewRequest(method, endpoint, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if method == http.MethodPost {
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	}
	sum := sha256.Sum256([]byte(body))
	err = v4.NewSigner().SignHTTP(context.Background(), aws.Credentials{AccessKeyID: "test-access", SecretAccessKey: "test-secret"}, r, hex.EncodeToString(sum[:]), "sns", "us-east-1", at)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func call(t *testing.T, g *Gateway, action string, q url.Values) *httptest.ResponseRecorder {
	t.Helper()
	if q == nil {
		q = url.Values{}
	}
	q.Set("Action", action)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, signedRequest(t, http.MethodPost, q, time.Now()))
	return w
}
func success(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
}
func xmlValue(t *testing.T, w *httptest.ResponseRecorder, name string) string {
	t.Helper()
	decoder := xml.NewDecoder(strings.NewReader(w.Body.String()))
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return ""
		}
		if err != nil {
			t.Fatal(err)
		}
		if start, ok := token.(xml.StartElement); ok && start.Name.Local == name {
			var value string
			if err := decoder.DecodeElement(&value, &start); err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
}
func createTopic(t *testing.T, g *Gateway, name string) string {
	t.Helper()
	w := call(t, g, "CreateTopic", url.Values{"Name": {name}})
	success(t, w)
	return xmlValue(t, w, "TopicArn")
}
func startWorkers(t *testing.T, g *Gateway) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = g.Run(ctx); close(done) }()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("workers did not stop")
		}
	}
	t.Cleanup(stop)
	return stop
}
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for delivery")
		var zero T
		return zero
	}
}

func TestAWSQueryLifecycle(t *testing.T) {
	g := newTestGateway(t, memory.New(), nil)
	arn := createTopic(t, g, "orders")
	if arn != "arn:aws:sns:us-east-1:000000000000:orders" {
		t.Fatal(arn)
	}
	if again := createTopic(t, g, "orders"); again != arn {
		t.Fatal("create is not idempotent")
	}
	w := call(t, g, "SetTopicAttributes", url.Values{"TopicArn": {arn}, "AttributeName": {"DisplayName"}, "AttributeValue": {"Orders & invoices"}})
	success(t, w)
	w = call(t, g, "GetTopicAttributes", url.Values{"TopicArn": {arn}})
	success(t, w)
	var attr struct {
		Result attributesResult `xml:"GetTopicAttributesResult"`
	}
	if err := xml.Unmarshal(w.Body.Bytes(), &attr); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range attr.Result.Entries {
		if a.Key == "DisplayName" && a.Value == "Orders & invoices" {
			found = true
		}
	}
	if !found {
		t.Fatal(w.Body.String())
	}
	w = call(t, g, "TagResource", url.Values{"ResourceArn": {arn}, "Tags.member.1.Key": {"team"}, "Tags.member.1.Value": {"payments"}})
	success(t, w)
	w = call(t, g, "ListTagsForResource", url.Values{"ResourceArn": {arn}})
	success(t, w)
	if xmlValue(t, w, "Value") != "payments" {
		t.Fatal(w.Body.String())
	}
	w = call(t, g, "UntagResource", url.Values{"ResourceArn": {arn}, "TagKeys.member.1": {"team"}})
	success(t, w)
	w = call(t, g, "DeleteTopic", url.Values{"TopicArn": {arn}})
	success(t, w)
	w = call(t, g, "DeleteTopic", url.Values{"TopicArn": {arn}})
	success(t, w)
	w = call(t, g, "Publish", url.Values{"TopicArn": {arn}, "Message": {"hello"}})
	if w.Code != 404 || xmlValue(t, w, "Code") != "NotFound" {
		t.Fatal(w.Body.String())
	}
}

func TestAuthentication(t *testing.T) {
	g := newTestGateway(t, memory.New(), nil)
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		t.Run(method, func(t *testing.T) {
			q := url.Values{"Action": {"CreateTopic"}, "Name": {"signed"}}
			r := signedRequest(t, method, q, time.Now())
			w := httptest.NewRecorder()
			g.ServeHTTP(w, r)
			success(t, w)
		})
	}
	for _, test := range []struct {
		name   string
		mutate func(*http.Request)
	}{
		{"unsigned", func(r *http.Request) { r.Header.Del("Authorization") }},
		{"tampered body", func(r *http.Request) {
			r.Body = io.NopCloser(strings.NewReader("Action=ListTopics&Version=2010-03-31"))
		}},
		{"tampered region", func(r *http.Request) {
			r.Header.Set("Authorization", strings.ReplaceAll(r.Header.Get("Authorization"), "us-east-1", "eu-west-1"))
		}},
		{"tampered host", func(r *http.Request) { r.Host = "other" }},
		{"session token", func(r *http.Request) { r.Header.Set("X-Amz-Security-Token", "invalid") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := signedRequest(t, http.MethodPost, url.Values{"Action": {"CreateTopic"}, "Name": {"rejected"}}, time.Now())
			test.mutate(r)
			w := httptest.NewRecorder()
			g.ServeHTTP(w, r)
			if w.Code != 403 {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
		})
	}
	r := signedRequest(t, http.MethodPost, url.Values{"Action": {"ListTopics"}}, time.Now().Add(-10*time.Minute))
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
	r = signedRequest(t, http.MethodPost, url.Values{"Action": {"ListTopics", "CreateTopic"}}, time.Now())
	w = httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}

	r, _ = http.NewRequest(http.MethodGet, "http://gateway/?Action=ListTopics&Version=2010-03-31&X-Amz-Expires=60", nil)
	endpoint, headers, err := v4.NewSigner().PresignHTTP(context.Background(), aws.Credentials{AccessKeyID: "test-access", SecretAccessKey: "test-secret"}, r, hash(nil), "sns", "us-east-1", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	r, _ = http.NewRequest(http.MethodGet, endpoint, nil)
	r.Header = headers
	w = httptest.NewRecorder()
	g.ServeHTTP(w, r)
	success(t, w)
}

type received struct {
	body    []byte
	headers http.Header
}

func TestConfirmationFanoutRetryAndRawDelivery(t *testing.T) {
	wrapped, raw := make(chan received, 10), make(chan received, 10)
	var attempts atomic.Int32
	transport := roundTrip(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		status := 200
		if r.URL.Host == "wrapped" {
			wrapped <- received{body, r.Header.Clone()}
			if r.Header.Get("x-amz-sns-message-type") == "Notification" && attempts.Add(1) == 1 {
				status = 503
			}
		} else {
			raw <- received{body, r.Header.Clone()}
		}
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	g := newTestGateway(t, memory.New(), transport)
	startWorkers(t, g)
	arn := createTopic(t, g, "events")
	for _, endpoint := range []string{"http://wrapped/", "https://raw/"} {
		q := url.Values{"TopicArn": {arn}, "Protocol": {strings.Split(endpoint, ":")[0]}, "Endpoint": {endpoint}, "ReturnSubscriptionArn": {"true"}}
		if strings.Contains(endpoint, "raw") {
			q.Set("Attributes.entry.1.key", "RawMessageDelivery")
			q.Set("Attributes.entry.1.value", "true")
		}
		w := call(t, g, "Subscribe", q)
		success(t, w)
		if xmlValue(t, w, "SubscriptionArn") == "pending confirmation" {
			t.Fatal("ReturnSubscriptionArn ignored")
		}
		ch := wrapped
		if strings.Contains(endpoint, "raw") {
			ch = raw
		}
		confirmation := receive(t, ch)
		var e envelope
		if err := json.Unmarshal(confirmation.body, &e); err != nil {
			t.Fatal(err)
		}
		if e.Type != "SubscriptionConfirmation" {
			t.Fatal(e.Type)
		}
		verifyEnvelope(t, g, e)
		w = call(t, g, "Publish", url.Values{"TopicArn": {arn}, "Message": {"before confirmation"}})
		success(t, w)
		r, _ := http.NewRequest(http.MethodGet, e.SubscribeURL, nil)
		w = httptest.NewRecorder()
		g.ServeHTTP(w, r)
		success(t, w)
		// Idempotent confirmation is valid after success.
		r, _ = http.NewRequest(http.MethodGet, e.SubscribeURL, nil)
		w = httptest.NewRecorder()
		g.ServeHTTP(w, r)
		success(t, w)
		// Drain any delivery to the already-confirmed wrapped subscriber from
		// creating the second, still-pending subscription.
		if ch == raw {
			for j := 0; j < 2; j++ {
				d := receive(t, wrapped)
				var before envelope
				_ = json.Unmarshal(d.body, &before)
				if before.Message != "before confirmation" {
					t.Fatal(before.Message)
				}
			}
		}
	}
	w := call(t, g, "Publish", url.Values{"TopicArn": {arn}, "Message": {"hello <world>"}, "Subject": {"Greeting"}, "MessageAttributes.entry.1.Name": {"kind"}, "MessageAttributes.entry.1.Value.DataType": {"String"}, "MessageAttributes.entry.1.Value.StringValue": {"created"}})
	success(t, w)
	id := xmlValue(t, w, "MessageId")
	d := receive(t, wrapped)
	var e envelope
	if err := json.Unmarshal(d.body, &e); err != nil {
		t.Fatal(err)
	}
	verifyEnvelope(t, g, e)
	if e.MessageID != id || e.Message != "hello <world>" || e.MessageAttributes["kind"].Value != "created" {
		t.Fatalf("%+v", e)
	}
	d = receive(t, raw)
	if string(d.body) != "hello <world>" || d.headers.Get("x-amz-sns-rawdelivery") != "true" {
		t.Fatalf("%s %+v", d.body, d.headers)
	}
	r, _ := http.NewRequest(http.MethodGet, e.UnsubscribeURL, nil)
	w = httptest.NewRecorder()
	g.ServeHTTP(w, r)
	success(t, w)
	w = call(t, g, "ListSubscriptionsByTopic", url.Values{"TopicArn": {arn}})
	success(t, w)
	var list struct {
		Result subscriptionList `xml:"ListSubscriptionsByTopicResult"`
	}
	if err := xml.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Result.Subscriptions) != 1 {
		t.Fatal(w.Body.String())
	}
}

func verifyEnvelope(t *testing.T, g *Gateway, e envelope) {
	t.Helper()
	block, _ := pem.Decode(g.signingCert)
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{"Message": e.Message, "MessageId": e.MessageID, "Subject": e.Subject, "SubscribeURL": e.SubscribeURL, "Timestamp": e.Timestamp, "Token": e.Token, "TopicArn": e.TopicARN, "Type": e.Type}
	names := []string{"Message", "MessageId"}
	if e.Type == "Notification" {
		if e.Subject != "" {
			names = append(names, "Subject")
		}
	} else {
		names = append(names, "SubscribeURL")
	}
	names = append(names, "Timestamp")
	if e.Type != "Notification" {
		names = append(names, "Token")
	}
	names = append(names, "TopicArn", "Type")
	canonical := ""
	for _, name := range names {
		canonical += name + "\n" + values[name] + "\n"
	}
	digest := sha256.Sum256([]byte(canonical))
	signature, err := base64.StdEncoding.DecodeString(e.Signature)
	if err != nil {
		t.Fatal(err)
	}
	if e.SignatureVersion != "2" || e.SigningCertURL != "http://gateway/SimpleNotificationService.pem" {
		t.Fatal("incorrect signature metadata")
	}
	if err := rsa.VerifyPKCS1v15(cert.PublicKey.(*rsa.PublicKey), crypto.SHA256, digest[:], signature); err != nil {
		t.Fatal(err)
	}
}

func TestPaginationAndValidation(t *testing.T) {
	g := newTestGateway(t, memory.New(), nil)
	for i := 0; i < 102; i++ {
		createTopic(t, g, fmt.Sprintf("topic-%03d", i))
	}
	w := call(t, g, "ListTopics", nil)
	success(t, w)
	var first struct {
		Result topicList `xml:"ListTopicsResult"`
	}
	if err := xml.Unmarshal(w.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if len(first.Result.Topics) != 100 || first.Result.Next == "" {
		t.Fatal(w.Body.String())
	}
	w = call(t, g, "ListTopics", url.Values{"NextToken": {first.Result.Next}})
	success(t, w)
	var second struct {
		Result topicList `xml:"ListTopicsResult"`
	}
	_ = xml.Unmarshal(w.Body.Bytes(), &second)
	if len(second.Result.Topics) != 2 || second.Result.Next != "" {
		t.Fatal(w.Body.String())
	}
	w = call(t, g, "ListSubscriptions", url.Values{"NextToken": {first.Result.Next}})
	if w.Code != 400 {
		t.Fatal("cross-operation token accepted")
	}
	arn := first.Result.Topics[0].ARN
	for _, q := range []url.Values{
		{"Action": {"CreateTopic"}, "Name": {"orders.fifo"}},
		{"Action": {"CreateTopic"}, "Name": {"bad"}, "Attributes.entry.1.key": {"FifoTopic"}, "Attributes.entry.1.value": {"true"}},
		{"Action": {"Subscribe"}, "TopicArn": {arn}, "Protocol": {"sqs"}, "Endpoint": {"queue"}},
		{"Action": {"Subscribe"}, "TopicArn": {arn}, "Protocol": {"https"}, "Endpoint": {"http://wrong/"}},
		{"Action": {"Publish"}, "TopicArn": {arn}, "Message": {""}},
		{"Action": {"Publish"}, "TopicArn": {arn}, "Message": {strings.Repeat("x", maxMessageSize+1)}},
		{"Action": {"Publish"}, "TopicArn": {arn}, "Message": {"hello"}, "MessageStructure": {"json"}},
		{"Action": {"Publish"}, "TopicArn": {arn}, "Message": {"hello"}, "MessageAttributes.entry.2.Name": {"missing-first"}},
		{"Action": {"ConfirmSubscription"}, "TopicArn": {arn}, "Token": {"bad"}},
	} {
		w := call(t, g, q.Get("Action"), q)
		if w.Code != 400 {
			t.Fatalf("%s: %d %s", q.Get("Action"), w.Code, w.Body.String())
		}
	}
	w = call(t, g, "PublishBatch", url.Values{"TopicArn": {arn}, "PublishBatchRequestEntries.member.1.Id": {"ok"}, "PublishBatchRequestEntries.member.1.Message": {"message"}, "PublishBatchRequestEntries.member.2.Id": {"bad"}, "PublishBatchRequestEntries.member.2.Message": {""}})
	success(t, w)
	var batch struct {
		Result batchResult `xml:"PublishBatchResult"`
	}
	_ = xml.Unmarshal(w.Body.Bytes(), &batch)
	if len(batch.Result.Successful) != 1 || len(batch.Result.Failed) != 1 || !batch.Result.Failed[0].SenderFault {
		t.Fatal(w.Body.String())
	}
}

func TestRestartKeepsSubscriptionsMessagesAndSigningIdentity(t *testing.T) {
	be := memory.New()
	g := newTestGateway(t, be, nil)
	arn := createTopic(t, g, "restart")
	w := call(t, g, "Subscribe", url.Values{"TopicArn": {arn}, "Protocol": {"http"}, "Endpoint": {"http://endpoint/"}, "ReturnSubscriptionArn": {"true"}})
	success(t, w)
	s, _, _ := be.Load(context.Background())
	sub := s.Subscriptions[xmlValue(t, w, "SubscriptionArn")]
	w = call(t, g, "ConfirmSubscription", url.Values{"TopicArn": {arn}, "Token": {sub.Token}})
	success(t, w)
	w = call(t, g, "Publish", url.Values{"TopicArn": {arn}, "Message": {"queued while stopped"}})
	success(t, w)
	deliveries := make(chan received, 2)
	next := newTestGateway(t, be, roundTrip(func(r *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		deliveries <- received{body, r.Header.Clone()}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(""))}, nil
	}))
	if string(next.signingCert) != string(g.signingCert) {
		t.Fatal("signing identity changed")
	}
	startWorkers(t, next)
	d := receive(t, deliveries)
	var e envelope
	_ = json.Unmarshal(d.body, &e)
	if e.Message != "queued while stopped" {
		t.Fatal(string(d.body))
	}
	verifyEnvelope(t, next, e)
	w = call(t, next, "DeleteTopic", url.Values{"TopicArn": {arn}})
	success(t, w)
	createTopic(t, next, "restart")
	state, _, _ := be.Load(context.Background())
	if state.Topics[arn].ID == s.Topics[arn].ID {
		t.Fatal("topic generation reused")
	}
}

func TestHTTPDeliveryStatusPolicy(t *testing.T) {
	for _, status := range []int{200, 204, 301, 400, 403, 404, 408, 429, 500, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			g := newTestGateway(t, memory.New(), roundTrip(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(""))}, nil
			}))
			retry, _ := g.deliver(context.Background(), backend.Subscription{Endpoint: "http://webhook/", Raw: true}, backend.Message{Type: "Notification", Body: "hello"})
			if retry != (status == 429 || status >= 500) {
				t.Fatalf("status %d: retry %t", status, retry)
			}
		})
	}
}
