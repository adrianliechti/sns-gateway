package gateway

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/adrianliechti/sns-gateway/backend"
	"github.com/adrianliechti/sns-gateway/backend/memory"
)

func TestFilterBodyMatching(t *testing.T) {
	for _, test := range []struct {
		name, policy, body string
		want               bool
	}{
		{"empty policy", `{}`, `not json`, true},
		{"exact", `{"event":["created"]}`, `{"event":"created"}`, true},
		{"case sensitive", `{"event":["created"]}`, `{"event":"Created"}`, false},
		{"AND", `{"event":["created"],"region":["eu"]}`, `{"event":"created","region":"us"}`, false},
		{"OR values", `{"event":["created","updated"]}`, `{"event":"updated"}`, true},
		{"array intersection", `{"event":["created"]}`, `{"event":["ignored","created"]}`, true},
		{"extra fields", `{"event":["created"]}`, `{"event":"created","ignored":42}`, true},
		{"missing", `{"event":["created"]}`, `{"other":"created"}`, false},
		{"prefix", `{"event":[{"prefix":"order."}]}`, `{"event":"order.created"}`, true},
		{"prefix requires string", `{"event":[{"prefix":"1"}]}`, `{"event":123}`, false},
		{"numeric range", `{"price":[{"numeric":[">",0,"<=",150]}]}`, `{"price":150}`, true},
		{"numeric lower boundary", `{"price":[{"numeric":[">",0,"<=",150]}]}`, `{"price":0}`, false},
		{"numeric upper boundary", `{"price":[{"numeric":[">",0,"<=",150]}]}`, `{"price":150.00001}`, false},
		{"numeric exponent", `{"price":[{"numeric":["=",301.5]}]}`, `{"price":3.015e2}`, true},
		{"numeric exact scalar", `{"price":[301.5]}`, `{"price":3.015e2}`, true},
		{"number string distinction", `{"price":[301.5]}`, `{"price":"301.5"}`, false},
		{"numeric negative", `{"price":[{"numeric":["<",0]}]}`, `{"price":-0.00001}`, true},
		{"numeric conjunction per element", `{"price":[{"numeric":[">",0,"<",10]}]}`, `{"price":[-1,11]}`, false},
		{"anything but", `{"event":[{"anything-but":["deleted","cancelled"]}]}`, `{"event":"created"}`, true},
		{"anything but excluded", `{"event":[{"anything-but":["deleted","cancelled"]}]}`, `{"event":"deleted"}`, false},
		{"anything but missing", `{"event":[{"anything-but":"deleted"}]}`, `{"other":1}`, false},
		{"anything but type", `{"event":[{"anything-but":"deleted"}]}`, `{"event":42}`, false},
		{"anything but null", `{"event":[{"anything-but":"deleted"}]}`, `{"event":null}`, false},
		{"anything but array", `{"event":[{"anything-but":"deleted"}]}`, `{"event":["deleted","created"]}`, true},
		{"anything but numeric", `{"price":[{"anything-but":[100,500]}]}`, `{"price":[100,50]}`, true},
		{"anything but prefix", `{"event":[{"anything-but":{"prefix":"order."}}]}`, `{"event":"invoice.created"}`, true},
		{"anything but prefix excluded", `{"event":[{"anything-but":{"prefix":"order."}}]}`, `{"event":"order.created"}`, false},
		{"exists", `{"event":[{"exists":true}]}`, `{"event":false}`, true},
		{"exists zero", `{"event":[{"exists":true}]}`, `{"event":0}`, true},
		{"exists null", `{"event":[{"exists":true}]}`, `{"event":null}`, false},
		{"exists empty string", `{"event":[{"exists":true}]}`, `{"event":""}`, false},
		{"exists empty array", `{"event":[{"exists":true}]}`, `{"event":[]}`, false},
		{"exists intermediate object", `{"event":[{"exists":true}]}`, `{"event":{"name":"created"}}`, false},
		{"absent", `{"event":[{"exists":false}]}`, `{"other":1}`, true},
		{"absent with no properties", `{"event":[{"exists":false}]}`, `{}`, false},
		{"absent null is present", `{"event":[{"exists":false}]}`, `{"event":null}`, false},
		{"null equality", `{"event":[null]}`, `{"event":null}`, true},
		{"null does not match missing", `{"event":[null]}`, `{"other":1}`, false},
		{"boolean", `{"active":[true]}`, `{"active":true}`, true},
		{"boolean type", `{"active":[true]}`, `{"active":"true"}`, false},
		{"nested", `{"order":{"event":["created"]}}`, `{"order":{"event":"created"}}`, true},
		{"nested array", `{"Records":{"event":["created"]}}`, `{"Records":[{"event":"ignored"},{"event":"created"}]}`, true},
		{"nested array no match", `{"Records":{"event":["created"]}}`, `{"Records":[{"event":"ignored"}]}`, false},
		{"nested array same object", `{"Records":{"event":["created"],"region":["eu"]}}`, `{"Records":[{"event":"created","region":"us"},{"event":"ignored","region":"eu"}]}`, false},
		{"nested missing parent", `{"order":{"event":[{"exists":false}]}}`, `{"other":1}`, false},
		{"nested missing leaf", `{"order":{"event":[{"exists":false}]}}`, `{"order":{"other":1}}`, true},
		{"literal dotted key", `{"order.event":["created"]}`, `{"order":{"event":"created"}}`, false},
		{"invalid JSON", `{"event":["created"]}`, `{"event":`, false},
		{"JSON array root", `{"event":["created"]}`, `[{"event":"created"}]`, false},
		{"JSON null root", `{"event":["created"]}`, `null`, false},
		{"trailing JSON", `{"event":["created"]}`, `{"event":"created"} {}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			p, err := compileFilter(test.policy, filterBody)
			if err != nil {
				t.Fatal(err)
			}
			if got := p.matches(backend.Message{Body: test.body}, filterBody); got != test.want {
				t.Fatalf("matched %v, want %v", got, test.want)
			}
		})
	}
}

func TestFilterAttributeMatching(t *testing.T) {
	for _, test := range []struct {
		name, policy, typ, value string
		want                     bool
	}{
		{"string", `{"value":["created"]}`, "String", "created", true},
		{"custom string type", `{"value":["created"]}`, "String.custom", "created", true},
		{"array", `{"value":["created"]}`, "String.Array", `["ignored","created"]`, true},
		{"array bool", `{"value":[true]}`, "String.Array", `[false,true]`, true},
		{"array number", `{"value":[{"numeric":[">",5]}]}`, "String.Array", `[1,6]`, true},
		{"array type distinction", `{"value":[6]}`, "String.Array", `["6"]`, false},
		{"malformed array", `{"value":[{"exists":true}]}`, "String.Array", `{}`, false},
		{"number", `{"value":[{"numeric":["=",301.5]}]}`, "Number", "3.015e2", true},
		{"custom number type", `{"value":[301.5]}`, "Number.currency", "301.5", true},
		{"number string", `{"value":["301.5"]}`, "Number", "301.5", false},
		{"string number", `{"value":[301.5]}`, "String", "301.5", false},
		{"binary ignored", `{"value":["YQ=="]}`, "Binary", "YQ==", false},
		{"binary existence ignored", `{"value":[{"exists":true}]}`, "Binary", "YQ==", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			p, err := compileFilter(test.policy, "")
			if err != nil {
				t.Fatal(err)
			}
			m := backend.Message{Body: `{"value":"unrelated"}`, Attributes: map[string]backend.MessageAttribute{"value": {Type: test.typ, Value: test.value}}}
			if got := p.matches(m, ""); got != test.want {
				t.Fatalf("matched %v, want %v", got, test.want)
			}
		})
	}
}

func TestFilterValidation(t *testing.T) {
	for _, policy := range []string{
		`null`, `[]`, `true`, `{"event":[]}`, `{"event":"created"}`, `{"event":[{}]}`,
		`{"event":[["created"]]}`, `{"event":[{"prefix":42}]}`, `{"event":[{"exists":"true"}]}`,
		`{"event":[{"prefix":"order","exists":true}]}`, `{"event":[{"wildcard":"*"}]}`,
		`{"event":[{"numeric":[]}]}`, `{"event":[{"numeric":[">",0,"<"]}]}`,
		`{"event":[{"numeric":["!=",0]}]}`, `{"event":[{"numeric":["=","1"]}]}`,
		`{"event":[{"numeric":[">",1000000001]}]}`, `{"event":[0.000001]}`, `{"event":[1e9999]}`,
		`{"event":[0.10000000000000001]}`, `{"event":[999999999.000001]}`, `{"event":[1e-999999999]}`,
		`{"event":[{"anything-but":[]}]}`, `{"event":[{"anything-but":[1,"two"]}]}`,
		`{"event":[{"anything-but":null}]}`, `{"event":[{"anything-but":{"exists":true}}]}`,
		`{"event":{}}`, `{"$or":[{"event":["one"]},{"event":["two"]}]}`,
		`{"a":[1],"b":[1],"c":[1],"d":[1],"e":[1],"f":[1]}`,
		`{"a":[1,2,3,4,5,6],"b":[1,2,3,4,5,6],"c":[1,2,3,4,5]}`,
		`{"a":{"b":{"c":[1,2,3,4,5,6,7,8,9,10]}},"d":{"e":[1,2,3]}}`,
		`{"event":["created"]} {}`, `{"event":`, "{\"event\":[\"\xff\"]}",
		`{"event":["` + strings.Repeat("x", maxFilterSize) + `"]}`,
	} {
		if _, err := compileFilter(policy, filterBody); err == nil {
			t.Errorf("accepted invalid policy %.200s", policy)
		}
	}
	if _, err := compileFilter(`{"order":{"event":["created"]}}`, filterAttributes); err == nil {
		t.Error("accepted nested attribute policy")
	}
	if _, err := compileFilter(`{}`, "invalid"); err == nil {
		t.Error("accepted invalid scope")
	}
	for _, policy := range []string{
		`{}`, `{"value":[-1000000000,1000000000,0.00001,301.50000]}`,
		`{"a":[1,2,3,4,5],"b":[1,2,3,4,5],"c":[1,2,3,4,5,6]}`,
		`{"a":{"b":{"c":[1,2,3,4]}},"d":{"e":[1,2,3]}}`,
	} {
		if _, err := compileFilter(policy, filterBody); err != nil {
			t.Errorf("rejected valid policy %s: %v", policy, err)
		}
	}
}

func getFilterAttributes(t *testing.T, g *Gateway, arn string) map[string]string {
	t.Helper()
	w := call(t, g, "GetSubscriptionAttributes", url.Values{"SubscriptionArn": {arn}})
	success(t, w)
	var response struct {
		Result attributesResult `xml:"GetSubscriptionAttributesResult"`
	}
	if err := xml.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	result := map[string]string{}
	for _, entry := range response.Result.Entries {
		result[entry.Key] = entry.Value
	}
	return result
}

func TestFilterAttributesAPI(t *testing.T) {
	be := memory.New()
	g := newTestGateway(t, be, nil)
	topic := createTopic(t, g, "filters")
	policy := `{"order":{"event":["created"]}}`
	q := url.Values{"TopicArn": {topic}, "Protocol": {"http"}, "Endpoint": {"http://endpoint/"}, "ReturnSubscriptionArn": {"true"}, "Attributes.entry.1.key": {"FilterPolicy"}, "Attributes.entry.1.value": {policy}}
	w := call(t, g, "Subscribe", q)
	if w.Code != 400 {
		t.Fatalf("nested attribute policy accepted: %s", w.Body.String())
	}
	state, _, err := be.Load(context.Background())
	if err != nil || len(state.Subscriptions) != 0 {
		t.Fatalf("invalid subscribe changed metadata: %+v %v", state, err)
	}
	q.Set("Attributes.entry.2.key", "FilterPolicyScope")
	q.Set("Attributes.entry.2.value", filterBody)
	w = call(t, g, "Subscribe", q)
	success(t, w)
	arn := xmlValue(t, w, "SubscriptionArn")
	attributes := getFilterAttributes(t, g, arn)
	if attributes["FilterPolicy"] != policy || attributes["FilterPolicyScope"] != filterBody {
		t.Fatal(attributes)
	}
	// Invalid changes must preserve both the policy and its scope.
	for _, change := range []struct{ key, value string }{
		{"FilterPolicyScope", filterAttributes}, {"FilterPolicyScope", ""},
		{"FilterPolicyScope", "invalid"}, {"FilterPolicy", ""}, {"FilterPolicy", "null"},
		{"FilterPolicy", `{"event":[{"unknown":true}]}`},
	} {
		w := call(t, g, "SetSubscriptionAttributes", url.Values{"SubscriptionArn": {arn}, "AttributeName": {change.key}, "AttributeValue": {change.value}})
		if w.Code != 400 || xmlValue(t, w, "Code") != "InvalidParameter" {
			t.Fatalf("accepted %+v: %s", change, w.Body.String())
		}
		attributes = getFilterAttributes(t, g, arn)
		if attributes["FilterPolicy"] != policy || attributes["FilterPolicyScope"] != filterBody {
			t.Fatal("invalid update changed policy", attributes)
		}
	}
	// Repeating Subscribe preserves the original settings, like RawMessageDelivery.
	q.Del("Attributes.entry.1.key")
	q.Del("Attributes.entry.1.value")
	q.Del("Attributes.entry.2.key")
	q.Del("Attributes.entry.2.value")
	w = call(t, g, "Subscribe", q)
	success(t, w)
	if xmlValue(t, w, "SubscriptionArn") != arn || getFilterAttributes(t, g, arn)["FilterPolicy"] != policy {
		t.Fatal("repeated Subscribe changed policy or identity")
	}
	// Persisted policies are available to a new gateway instance.
	g = newTestGateway(t, be, nil)
	if getFilterAttributes(t, g, arn)["FilterPolicy"] != policy {
		t.Fatal("policy lost after restart")
	}
	w = call(t, g, "SetSubscriptionAttributes", url.Values{"SubscriptionArn": {arn}, "AttributeName": {"FilterPolicy"}, "AttributeValue": {`{}`}})
	success(t, w)
	w = call(t, g, "SetSubscriptionAttributes", url.Values{"SubscriptionArn": {arn}, "AttributeName": {"FilterPolicyScope"}, "AttributeValue": {filterAttributes}})
	success(t, w)
	attributes = getFilterAttributes(t, g, arn)
	if attributes["FilterPolicy"] != "{}" || attributes["FilterPolicyScope"] != filterAttributes {
		t.Fatal(attributes)
	}
}

type filterAck struct{ messageID, subscriptionID string }
type filterObservedBackend struct {
	backend.Backend
	acks chan filterAck
}
type filterObservedConsumer struct {
	backend.Consumer
	subscriptionID string
	acks           chan filterAck
}
type filterObservedDelivery struct {
	backend.Delivery
	subscriptionID string
	acks           chan filterAck
}

func (b *filterObservedBackend) Consumer(ctx context.Context, sub backend.Subscription) (backend.Consumer, error) {
	c, err := b.Backend.Consumer(ctx, sub)
	return &filterObservedConsumer{c, sub.ID, b.acks}, err
}
func (c *filterObservedConsumer) Next(ctx context.Context) (backend.Delivery, error) {
	d, err := c.Consumer.Next(ctx)
	return &filterObservedDelivery{d, c.subscriptionID, c.acks}, err
}
func (d *filterObservedDelivery) Ack() error {
	if err := d.Delivery.Ack(); err != nil {
		return err
	}
	d.acks <- filterAck{d.Message().ID, d.subscriptionID}
	return nil
}

func TestFilteringDeliveryAndLiveUpdates(t *testing.T) {
	be := &filterObservedBackend{Backend: memory.New(), acks: make(chan filterAck, 100)}
	deliveries := make(chan received, 100)
	transport := roundTrip(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		deliveries <- received{body, r.Header.Clone()}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	g := newTestGateway(t, be, transport)
	stop := startWorkers(t, g)
	topic := createTopic(t, g, "filtered-delivery")
	arns := map[string]string{}
	for _, name := range []string{"wrapped", "raw"} {
		q := url.Values{"TopicArn": {topic}, "Protocol": {"http"}, "Endpoint": {"http://" + name + "/"}, "ReturnSubscriptionArn": {"true"}, "Attributes.entry.1.key": {"FilterPolicy"}, "Attributes.entry.1.value": {`{"event":["created"]}`}}
		if name == "raw" {
			q.Set("Attributes.entry.1.value", `{"order":{"event":["cancelled"]}}`)
			q.Set("Attributes.entry.2.key", "FilterPolicyScope")
			q.Set("Attributes.entry.2.value", filterBody)
			q.Set("Attributes.entry.3.key", "RawMessageDelivery")
			q.Set("Attributes.entry.3.value", "true")
		}
		w := call(t, g, "Subscribe", q)
		success(t, w)
		arns[name] = xmlValue(t, w, "SubscriptionArn")
		confirmation := receive(t, deliveries)
		var e envelope
		if err := json.Unmarshal(confirmation.body, &e); err != nil || e.Type != "SubscriptionConfirmation" {
			t.Fatalf("confirmation was filtered: %s %v", confirmation.body, err)
		}
		verifyEnvelope(t, g, e)
		success(t, call(t, g, "ConfirmSubscription", url.Values{"TopicArn": {topic}, "Token": {e.Token}}))
	}
	check := func(body, attribute string, want ...string) {
		t.Helper()
		q := url.Values{"TopicArn": {topic}, "Message": {body}}
		if attribute != "" {
			q.Set("MessageAttributes.entry.1.Name", "event")
			q.Set("MessageAttributes.entry.1.Value.DataType", "String")
			q.Set("MessageAttributes.entry.1.Value.StringValue", attribute)
		}
		w := call(t, g, "Publish", q)
		success(t, w)
		id := xmlValue(t, w, "MessageId")
		// Wait for both consumers to acknowledge, including nonmatching messages.
		acked := map[string]bool{}
		for len(acked) < len(arns) {
			ack := receive(t, be.acks)
			if ack.messageID == id {
				acked[ack.subscriptionID] = true
			}
		}
		wanted := map[string]bool{}
		for _, name := range want {
			wanted[arns[name]] = true
		}
		for len(deliveries) > 0 {
			d := <-deliveries
			arn := d.headers.Get("x-amz-sns-subscription-arn")
			if !wanted[arn] || d.headers.Get("x-amz-sns-message-id") != id {
				t.Fatalf("unexpected webhook delivery: %s %s", arn, d.body)
			}
			delete(wanted, arn)
			if arn == arns["raw"] {
				if string(d.body) != body || d.headers.Get("x-amz-sns-rawdelivery") != "true" {
					t.Fatal("raw payload changed", string(d.body))
				}
			} else {
				var e envelope
				if err := json.Unmarshal(d.body, &e); err != nil {
					t.Fatal(err)
				}
				verifyEnvelope(t, g, e)
				if e.Message != body {
					t.Fatal("wrapped payload changed", e.Message)
				}
			}
		}
		if len(wanted) != 0 {
			t.Fatal("missing deliveries", wanted)
		}
	}
	check(`{"order":{"event":"cancelled"}}`, "created", "wrapped", "raw")
	check("not JSON", "ignored")
	check(`{"order":{"event":"created"}}`, "created", "wrapped")
	success(t, call(t, g, "SetSubscriptionAttributes", url.Values{"SubscriptionArn": {arns["wrapped"]}, "AttributeName": {"FilterPolicy"}, "AttributeValue": {`{"event":["updated"]}`}}))
	check(`{}`, "created")
	check(`{}`, "updated", "wrapped")
	// Restart with the same backend and verify that filtering still applies.
	stop()
	g = newTestGateway(t, be, transport)
	startWorkers(t, g)
	check(`{}`, "created")
	check(`{"order":{"event":"cancelled"}}`, "updated", "wrapped", "raw")
	success(t, call(t, g, "SetSubscriptionAttributes", url.Values{"SubscriptionArn": {arns["raw"]}, "AttributeName": {"FilterPolicy"}, "AttributeValue": {`{}`}}))
	check("plain text after removing policy", "", "raw")
	// Changing only the scope also invalidates the worker's compiled-policy cache.
	success(t, call(t, g, "SetSubscriptionAttributes", url.Values{"SubscriptionArn": {arns["wrapped"]}, "AttributeName": {"FilterPolicyScope"}, "AttributeValue": {filterBody}}))
	check(`{"event":"updated"}`, "ignored", "wrapped", "raw")
	check(`{}`, "updated", "raw")
}
