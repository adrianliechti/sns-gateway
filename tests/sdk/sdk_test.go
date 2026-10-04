package sdk_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/adrianliechti/sns-gateway/backend/memory"
	"github.com/adrianliechti/sns-gateway/gateway"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
)

type transport struct{ handler http.Handler }

func (t transport) RoundTrip(r *http.Request) (*http.Response, error) {
	// Convert the SDK's outbound request to the shape net/http presents to a
	// server. In particular, SDK requests can leave Host empty and use URL.Host.
	req := r.Clone(r.Context())
	req.RequestURI = req.URL.RequestURI()
	if req.Host == "" {
		req.Host = req.URL.Host
	}
	if req.Body == nil {
		req.Body = http.NoBody
	}
	w := httptest.NewRecorder()
	t.handler.ServeHTTP(w, req)
	response := w.Result()
	response.Request = r
	return response, nil
}

func TestSNSClient(t *testing.T) {
	g, err := gateway.New(memory.New(), gateway.Options{AccessKey: "test-access", SecretKey: "test-secret", PublicURL: "http://gateway"})
	if err != nil {
		t.Fatal(err)
	}
	c := sns.New(sns.Options{Region: "us-east-1", BaseEndpoint: aws.String("http://gateway"), Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "test-access", SecretAccessKey: "test-secret"}, nil
	}), HTTPClient: &http.Client{Transport: transport{g}}})
	ctx := context.Background()
	created, err := c.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("sdk-test"), Attributes: map[string]string{"DisplayName": "SDK test"}, Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("test")}}})
	if err != nil {
		t.Fatal(err)
	}
	arn := created.TopicArn
	listed, err := c.ListTopics(ctx, &sns.ListTopicsInput{})
	if err != nil || len(listed.Topics) != 1 || aws.ToString(listed.Topics[0].TopicArn) != aws.ToString(arn) {
		t.Fatalf("%+v %v", listed, err)
	}
	attrs, err := c.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: arn})
	if err != nil || attrs.Attributes["DisplayName"] != "SDK test" {
		t.Fatalf("%+v %v", attrs, err)
	}
	sub, err := c.Subscribe(ctx, &sns.SubscribeInput{TopicArn: arn, Protocol: aws.String("https"), Endpoint: aws.String("https://subscriber.example/"), ReturnSubscriptionArn: true, Attributes: map[string]string{"RawMessageDelivery": "true"}})
	if err != nil {
		t.Fatal(err)
	}
	subattrs, err := c.GetSubscriptionAttributes(ctx, &sns.GetSubscriptionAttributesInput{SubscriptionArn: sub.SubscriptionArn})
	if err != nil || subattrs.Attributes["PendingConfirmation"] != "true" {
		t.Fatalf("%+v %v", subattrs, err)
	}
	published, err := c.Publish(ctx, &sns.PublishInput{TopicArn: arn, Message: aws.String("hello"), MessageAttributes: map[string]types.MessageAttributeValue{"kind": {DataType: aws.String("String"), StringValue: aws.String("test")}}})
	if err != nil || aws.ToString(published.MessageId) == "" {
		t.Fatalf("%+v %v", published, err)
	}
	batch, err := c.PublishBatch(ctx, &sns.PublishBatchInput{TopicArn: arn, PublishBatchRequestEntries: []types.PublishBatchRequestEntry{{Id: aws.String("one"), Message: aws.String("first")}, {Id: aws.String("two"), Message: aws.String("second")}}})
	if err != nil || len(batch.Successful) != 2 || len(batch.Failed) != 0 {
		t.Fatalf("%+v %v", batch, err)
	}
	if _, err := c.Unsubscribe(ctx, &sns.UnsubscribeInput{SubscriptionArn: sub.SubscriptionArn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: arn}); err != nil {
		t.Fatal(err)
	}
}
