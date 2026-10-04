package gateway

import (
	"context"
	"crypto/hmac"
	"encoding/base64"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/adrianliechti/sns-gateway/backend"
)

var topicName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)
var tagText = regexp.MustCompile(`^[\p{L}\p{N}\p{Z}_.:/=+@-]*$`)

type arnResult struct {
	ARN string `xml:"TopicArn"`
}
type subscriptionResult struct {
	ARN string `xml:"SubscriptionArn"`
}
type publishResult struct {
	ID string `xml:"MessageId"`
}
type entry struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}
type attributesResult struct {
	Entries []entry `xml:"Attributes>entry"`
}
type topicList struct {
	Topics []arnResult `xml:"Topics>member"`
	Next   string      `xml:"NextToken,omitempty"`
}
type subscriptionMember struct {
	ARN      string `xml:"SubscriptionArn"`
	Owner    string `xml:"Owner"`
	Protocol string `xml:"Protocol"`
	Endpoint string `xml:"Endpoint"`
	TopicARN string `xml:"TopicArn"`
}
type subscriptionList struct {
	Subscriptions []subscriptionMember `xml:"Subscriptions>member"`
	Next          string               `xml:"NextToken,omitempty"`
}

func attrs(values map[string]string) attributesResult {
	result := attributesResult{}
	for key, value := range values {
		result.Entries = append(result.Entries, entry{key, value})
	}
	sort.Slice(result.Entries, func(i, j int) bool { return result.Entries[i].Key < result.Entries[j].Key })
	return result
}

func (g *Gateway) dispatch(ctx context.Context, q url.Values) (any, error) {
	// Serialize operations with publish so durable subscriptions exist before a
	// successful publish, and removed/recreated topics never share a generation.
	g.mu.Lock()
	defer g.mu.Unlock()
	s, revision, err := g.be.Load(ctx)
	if err != nil {
		return nil, err
	}
	save := func(result any) (any, error) { return result, g.be.Save(ctx, s, revision) }
	action := q.Get("Action")
	switch action {
	case "CreateTopic":
		name := q.Get("Name")
		if !topicName.MatchString(name) {
			return nil, invalid("Name must contain 1–256 letters, digits, underscores or hyphens; FIFO topics are unsupported")
		}
		attributes, err := parseMap(q, "Attributes.entry.")
		if err != nil {
			return nil, err
		}
		for key, value := range attributes {
			if key != "DisplayName" {
				return nil, unsupported("topic attribute " + key)
			}
			if err := validateDisplayName(value); err != nil {
				return nil, err
			}
		}
		tags, err := parseTags(q)
		if err != nil {
			return nil, err
		}
		arn := g.topicARN(name)
		if _, exists := s.Topics[arn]; exists {
			return arnResult{arn}, nil
		}
		s.Topics[arn] = backend.Topic{ARN: arn, ID: backend.NewID(), Name: name, DisplayName: attributes["DisplayName"], Tags: tags}
		return save(arnResult{arn})
	case "ListTopics":
		keys := make([]string, 0, len(s.Topics))
		for key := range s.Topics {
			keys = append(keys, key)
		}
		page, next, err := g.page(keys, q.Get("NextToken"), action)
		if err != nil {
			return nil, err
		}
		result := topicList{Next: next}
		for _, key := range page {
			result.Topics = append(result.Topics, arnResult{key})
		}
		return result, nil
	case "DeleteTopic":
		arn := q.Get("TopicArn")
		if !g.validTopicARN(arn) {
			return nil, invalid("Invalid TopicArn")
		}
		if _, exists := s.Topics[arn]; !exists {
			return nil, nil
		}
		if s.PendingDeletes == nil {
			s.PendingDeletes = map[string]backend.Subscription{}
		}
		for key, sub := range s.Subscriptions {
			if sub.TopicARN == arn {
				s.PendingDeletes[key] = sub
				delete(s.Subscriptions, key)
			}
		}
		delete(s.Topics, arn)
		return save(nil)
	case "GetTopicAttributes", "SetTopicAttributes", "Subscribe", "Publish", "PublishBatch", "ListSubscriptionsByTopic", "TagResource", "UntagResource", "ListTagsForResource":
		arn := q.Get("TopicArn")
		if strings.Contains(action, "Tag") || action == "UntagResource" {
			arn = q.Get("ResourceArn")
		}
		topic, exists := s.Topics[arn]
		if !exists {
			return nil, notFound()
		}
		switch action {
		case "GetTopicAttributes":
			confirmed, pending := 0, 0
			for _, sub := range s.Subscriptions {
				if sub.TopicARN == arn {
					if sub.ConfirmedAt.IsZero() {
						pending++
					} else {
						confirmed++
					}
				}
			}
			return attrs(map[string]string{"TopicArn": arn, "Owner": g.opts.AccountID, "DisplayName": topic.DisplayName, "SubscriptionsConfirmed": strconv.Itoa(confirmed), "SubscriptionsPending": strconv.Itoa(pending), "SubscriptionsDeleted": "0", "SignatureVersion": "2"}), nil
		case "SetTopicAttributes":
			if q.Get("AttributeName") != "DisplayName" {
				return nil, unsupported("topic attribute " + q.Get("AttributeName"))
			}
			if err := validateDisplayName(q.Get("AttributeValue")); err != nil {
				return nil, err
			}
			topic.DisplayName = q.Get("AttributeValue")
			s.Topics[arn] = topic
			return save(nil)
		case "Subscribe":
			return g.subscribe(ctx, q, &s, revision, topic)
		case "Publish":
			message, err := parseMessage(q, topic)
			if err != nil {
				return nil, err
			}
			if err := g.publish(ctx, message); err != nil {
				return nil, err
			}
			return publishResult{message.ID}, nil
		case "PublishBatch":
			return g.publishBatch(ctx, q, topic)
		case "ListSubscriptionsByTopic":
			return g.listSubscriptions(s, q, arn)
		case "TagResource":
			tags, err := parseTags(q)
			if err != nil {
				return nil, err
			}
			if topic.Tags == nil {
				topic.Tags = map[string]string{}
			}
			for key, value := range tags {
				topic.Tags[key] = value
			}
			if len(topic.Tags) > 50 {
				return nil, invalid("At most 50 tags are allowed")
			}
			s.Topics[arn] = topic
			return save(nil)
		case "UntagResource":
			indices, err := members(q, "TagKeys.member.")
			if err != nil {
				return nil, err
			}
			for _, index := range indices {
				delete(topic.Tags, q.Get("TagKeys.member."+index))
			}
			s.Topics[arn] = topic
			return save(nil)
		case "ListTagsForResource":
			type tag struct {
				Key   string
				Value string
			}
			result := struct {
				Tags []tag `xml:"Tags>member"`
			}{}
			keys := make([]string, 0, len(topic.Tags))
			for key := range topic.Tags {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				result.Tags = append(result.Tags, tag{key, topic.Tags[key]})
			}
			return result, nil
		}
	case "ListSubscriptions":
		return g.listSubscriptions(s, q, "")
	case "ConfirmSubscription":
		if value := q.Get("AuthenticateOnUnsubscribe"); value != "" && value != "false" {
			return nil, unsupported("AuthenticateOnUnsubscribe")
		}
		for arn, sub := range s.Subscriptions {
			if sub.TopicARN != q.Get("TopicArn") || !hmac.Equal([]byte(sub.Token), []byte(q.Get("Token"))) {
				continue
			}
			if !sub.ConfirmedAt.IsZero() {
				return subscriptionResult{arn}, nil
			}
			if time.Since(sub.CreatedAt) > 48*time.Hour {
				return nil, invalid("Confirmation token has expired; subscribe again")
			}
			sub.ConfirmedAt = time.Now().UTC()
			s.Subscriptions[arn] = sub
			return save(subscriptionResult{arn})
		}
		return nil, invalid("Invalid confirmation token")
	case "Unsubscribe", "GetSubscriptionAttributes", "SetSubscriptionAttributes":
		arn := q.Get("SubscriptionArn")
		sub, exists := s.Subscriptions[arn]
		if !exists {
			if action == "Unsubscribe" {
				if !strings.HasPrefix(arn, "arn:aws:sns:"+g.opts.Region+":"+g.opts.AccountID+":") || strings.Count(arn, ":") != 6 {
					return nil, invalid("Invalid SubscriptionArn")
				}
				return nil, nil
			}
			return nil, notFound()
		}
		switch action {
		case "Unsubscribe":
			if q.Has("Token") && !hmac.Equal([]byte(sub.Token), []byte(q.Get("Token"))) {
				return nil, invalid("Invalid unsubscribe token")
			}
			if s.PendingDeletes == nil {
				s.PendingDeletes = map[string]backend.Subscription{}
			}
			s.PendingDeletes[arn] = sub
			delete(s.Subscriptions, arn)
			return save(nil)
		case "GetSubscriptionAttributes":
			return attrs(map[string]string{"SubscriptionArn": arn, "TopicArn": sub.TopicARN, "Owner": g.opts.AccountID, "Protocol": sub.Protocol, "Endpoint": sub.Endpoint, "PendingConfirmation": strconv.FormatBool(sub.ConfirmedAt.IsZero()), "RawMessageDelivery": strconv.FormatBool(sub.Raw), "ConfirmationWasAuthenticated": "false"}), nil
		case "SetSubscriptionAttributes":
			if q.Get("AttributeName") != "RawMessageDelivery" {
				return nil, unsupported("subscription attribute " + q.Get("AttributeName"))
			}
			value, err := strictBool(q.Get("AttributeValue"))
			if err != nil {
				return nil, err
			}
			sub.Raw = value
			s.Subscriptions[arn] = sub
			return save(nil)
		}
	}
	return nil, &apiError{"InvalidAction", "Action is not supported: " + action, 400}
}

func (g *Gateway) validTopicARN(arn string) bool {
	prefix := "arn:aws:sns:" + g.opts.Region + ":" + g.opts.AccountID + ":"
	return strings.HasPrefix(arn, prefix) && topicName.MatchString(strings.TrimPrefix(arn, prefix))
}

func (g *Gateway) subscribe(ctx context.Context, q url.Values, s *backend.State, revision uint64, topic backend.Topic) (any, error) {
	protocol, endpoint := q.Get("Protocol"), q.Get("Endpoint")
	if protocol != "http" && protocol != "https" {
		return nil, unsupported("protocol " + protocol)
	}
	u, err := url.Parse(endpoint)
	if err != nil || !utf8.ValidString(endpoint) || u.Scheme != protocol || u.Hostname() == "" || u.User != nil || u.Fragment != "" || len(endpoint) > 2048 {
		return nil, invalid("Endpoint must be a matching HTTP(S) URL without credentials or fragment")
	}
	attributes, err := parseMap(q, "Attributes.entry.")
	if err != nil {
		return nil, err
	}
	raw := false
	for key, value := range attributes {
		if key != "RawMessageDelivery" {
			return nil, unsupported("subscription attribute " + key)
		}
		raw, err = strictBool(value)
		if err != nil {
			return nil, err
		}
	}
	returnARN := false
	if value := q.Get("ReturnSubscriptionArn"); value != "" {
		returnARN, err = strictBool(value)
		if err != nil {
			return nil, err
		}
	}
	var sub backend.Subscription
	for _, existing := range s.Subscriptions {
		if existing.TopicARN == topic.ARN && existing.Protocol == protocol && existing.Endpoint == endpoint {
			sub = existing
			break
		}
	}
	if sub.ARN == "" {
		id := backend.NewID()
		sub = backend.Subscription{ARN: topic.ARN + ":" + id, ID: id, TopicARN: topic.ARN, TopicID: topic.ID, Protocol: protocol, Endpoint: endpoint, Token: backend.NewID() + backend.NewID(), CreatedAt: time.Now().UTC(), Raw: raw}
	} else if sub.ConfirmedAt.IsZero() && time.Since(sub.CreatedAt) > 48*time.Hour {
		sub.Token = backend.NewID() + backend.NewID()
		sub.CreatedAt = time.Now().UTC()
	}
	if err := g.be.EnsureSubscription(ctx, sub); err != nil {
		return nil, err
	}
	s.Subscriptions[sub.ARN] = sub
	if err := g.be.Save(ctx, *s, revision); err != nil {
		return nil, err
	}
	if sub.ConfirmedAt.IsZero() {
		message := backend.Message{ID: backend.NewID(), TopicARN: topic.ARN, TopicID: topic.ID, SubscriptionID: sub.ID, Type: "SubscriptionConfirmation", Body: "You have chosen to subscribe to the topic " + topic.ARN + ". Confirm the subscription using SubscribeURL.", Timestamp: time.Now().UTC()}
		if err := g.publish(ctx, message); err != nil {
			return nil, err
		}
	}
	arn := sub.ARN
	if sub.ConfirmedAt.IsZero() && !returnARN {
		arn = "pending confirmation"
	}
	return subscriptionResult{arn}, nil
}

func (g *Gateway) listSubscriptions(s backend.State, q url.Values, topicARN string) (any, error) {
	var keys []string
	for key, sub := range s.Subscriptions {
		if topicARN == "" || sub.TopicARN == topicARN {
			keys = append(keys, key)
		}
	}
	page, next, err := g.page(keys, q.Get("NextToken"), q.Get("Action")+topicARN)
	if err != nil {
		return nil, err
	}
	result := subscriptionList{Next: next}
	for _, key := range page {
		sub := s.Subscriptions[key]
		arn := sub.ARN
		if sub.ConfirmedAt.IsZero() {
			arn = "PendingConfirmation"
		}
		result.Subscriptions = append(result.Subscriptions, subscriptionMember{arn, g.opts.AccountID, sub.Protocol, sub.Endpoint, sub.TopicARN})
	}
	return result, nil
}

func (g *Gateway) page(keys []string, token, scope string) ([]string, string, error) {
	sort.Strings(keys)
	last := ""
	if token != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil || len(decoded) < 32 {
			return nil, "", invalid("Invalid NextToken")
		}
		payload, signature := decoded[:len(decoded)-32], decoded[len(decoded)-32:]
		if !hmac.Equal(signature, mac([]byte(g.opts.SecretKey), string(payload))) || !strings.HasPrefix(string(payload), scope+"\n") {
			return nil, "", invalid("Invalid NextToken")
		}
		last = strings.TrimPrefix(string(payload), scope+"\n")
	}
	start := sort.Search(len(keys), func(i int) bool { return keys[i] > last })
	keys = keys[start:]
	if len(keys) <= 100 {
		return keys, "", nil
	}
	payload := []byte(scope + "\n" + keys[99])
	token = base64.RawURLEncoding.EncodeToString(append(payload, mac([]byte(g.opts.SecretKey), string(payload))...))
	return keys[:100], token, nil
}

func members(q url.Values, prefix string) ([]string, error) {
	set := map[int]bool{}
	for key := range q {
		if strings.HasPrefix(key, prefix) {
			i := strings.SplitN(strings.TrimPrefix(key, prefix), ".", 2)[0]
			n, err := strconv.Atoi(i)
			if err != nil || n < 1 || strconv.Itoa(n) != i || n > 1000 {
				return nil, invalid("Invalid member index")
			}
			set[n] = true
		}
	}
	indices := make([]string, len(set))
	for i := 1; i <= len(set); i++ {
		if !set[i] {
			return nil, invalid("Member indices must be contiguous")
		}
		indices[i-1] = strconv.Itoa(i)
	}
	return indices, nil
}

func parseMap(q url.Values, prefix string) (map[string]string, error) {
	indices, err := members(q, prefix)
	if err != nil {
		return nil, err
	}
	result := map[string]string{}
	for _, i := range indices {
		key := q.Get(prefix + i + ".key")
		if key == "" {
			return nil, invalid("Attribute key is required")
		}
		if _, exists := result[key]; exists {
			return nil, invalid("Duplicate attribute")
		}
		if !q.Has(prefix + i + ".value") {
			return nil, invalid("Attribute value is required")
		}
		result[key] = q.Get(prefix + i + ".value")
	}
	return result, nil
}

func parseTags(q url.Values) (map[string]string, error) {
	indices, err := members(q, "Tags.member.")
	if err != nil {
		return nil, err
	}
	if len(indices) > 50 {
		return nil, invalid("At most 50 tags are allowed")
	}
	result := map[string]string{}
	for _, i := range indices {
		key, value := q.Get("Tags.member."+i+".Key"), q.Get("Tags.member."+i+".Value")
		if !utf8.ValidString(key) || !utf8.ValidString(value) || !tagText.MatchString(key) || !tagText.MatchString(value) || len([]rune(key)) < 1 || len([]rune(key)) > 128 || len([]rune(value)) > 256 || strings.HasPrefix(strings.ToLower(key), "aws:") {
			return nil, invalid("Invalid tag")
		}
		if _, exists := result[key]; exists {
			return nil, invalid("Duplicate tag key")
		}
		result[key] = value
	}
	return result, nil
}

func strictBool(value string) (bool, error) {
	if value == "true" {
		return true, nil
	}
	if value == "false" {
		return false, nil
	}
	return false, invalid("Expected true or false")
}
func validateDisplayName(value string) error {
	if !utf8.ValidString(value) || strings.IndexFunc(value, unicode.IsControl) >= 0 || len([]rune(value)) > 100 {
		return invalid("DisplayName is limited to 100 UTF-8 characters without control characters")
	}
	return nil
}
