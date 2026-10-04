package gateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/adrianliechti/sns-gateway/backend"
)

var attributeName = regexp.MustCompile(`^[A-Za-z0-9_-]+(?:[.][A-Za-z0-9_-]+)*$`)
var batchID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)

func parseMessage(q url.Values, topic backend.Topic) (backend.Message, error) {
	m := backend.Message{ID: backend.NewID(), TopicARN: topic.ARN, TopicID: topic.ID, Type: "Notification", Body: q.Get("Message"), Subject: q.Get("Subject"), Timestamp: time.Now().UTC(), Attributes: map[string]backend.MessageAttribute{}}
	if m.Body == "" || !utf8.ValidString(m.Body) {
		return m, invalid("Message must be non-empty UTF-8")
	}
	if len([]rune(m.Subject)) >= 100 || !utf8.ValidString(m.Subject) || strings.IndexFunc(m.Subject, unicode.IsControl) >= 0 {
		return m, invalid("Subject must be shorter than 100 characters and contain no control characters")
	}
	for _, key := range []string{"MessageStructure", "MessageGroupId", "MessageDeduplicationId", "PhoneNumber", "TargetArn"} {
		if q.Has(key) {
			return m, unsupported(key)
		}
	}
	indices, err := members(q, "MessageAttributes.entry.")
	if err != nil {
		return m, err
	}
	for _, i := range indices {
		prefix := "MessageAttributes.entry." + i + "."
		name, typ := q.Get(prefix+"Name"), q.Get(prefix+"Value.DataType")
		if len(name) > 256 || !attributeName.MatchString(name) || strings.HasPrefix(strings.ToLower(name), "aws.") || strings.HasPrefix(strings.ToLower(name), "amazon.") {
			return m, invalid("Invalid message attribute name")
		}
		if _, exists := m.Attributes[name]; exists {
			return m, invalid("Duplicate message attribute name")
		}
		base := strings.SplitN(typ, ".", 2)[0]
		value := q.Get(prefix + "Value.StringValue")
		switch base {
		case "Binary":
			value = q.Get(prefix + "Value.BinaryValue")
			data, err := base64.StdEncoding.DecodeString(value)
			if err != nil || len(data) == 0 || q.Has(prefix+"Value.StringValue") {
				return m, invalid("Invalid binary message attribute")
			}
		case "String", "Number":
			if value == "" || !utf8.ValidString(value) || q.Has(prefix+"Value.BinaryValue") {
				return m, invalid("Invalid string message attribute")
			}
			if base == "Number" {
				n, err := strconv.ParseFloat(value, 64)
				if err != nil || math.IsInf(n, 0) || math.IsNaN(n) || math.Abs(n) > 1e9 {
					return m, invalid("Invalid numeric message attribute")
				}
			}
			if typ == "String.Array" {
				var array []any
				if json.Unmarshal([]byte(value), &array) != nil || array == nil {
					return m, invalid("String.Array must be a JSON array")
				}
				for _, v := range array {
					switch v.(type) {
					case nil, string, bool, float64:
					default:
						return m, invalid("String.Array must contain scalar values")
					}
				}
			}
		default:
			return m, invalid("Unsupported message attribute data type")
		}
		m.Attributes[name] = backend.MessageAttribute{Type: typ, Value: value}
	}
	if messageSize(m) > maxMessageSize {
		return m, invalid("Message and attributes exceed 256 KiB")
	}
	return m, nil
}

func messageSize(m backend.Message) int {
	size := len(m.Body) + len(m.Subject)
	for name, a := range m.Attributes {
		n := len(a.Value)
		if strings.SplitN(a.Type, ".", 2)[0] == "Binary" {
			decoded, _ := base64.StdEncoding.DecodeString(a.Value)
			n = len(decoded)
		}
		size += len(name) + len(a.Type) + n
	}
	return size
}

type batchSuccess struct {
	ID        string `xml:"Id"`
	MessageID string `xml:"MessageId"`
}
type batchFailure struct {
	ID          string `xml:"Id"`
	Code        string
	Message     string
	SenderFault bool
}
type batchResult struct {
	Successful []batchSuccess `xml:"Successful>member"`
	Failed     []batchFailure `xml:"Failed>member"`
}

func (g *Gateway) publishBatch(ctx context.Context, q url.Values, topic backend.Topic) (any, error) {
	indices, err := members(q, "PublishBatchRequestEntries.member.")
	if err != nil {
		return nil, err
	}
	if len(indices) == 0 {
		return nil, &apiError{"EmptyBatchRequest", "Batch must contain at least one entry", 400}
	}
	if len(indices) > 10 {
		return nil, &apiError{"TooManyEntriesInBatchRequest", "At most ten entries are allowed", 400}
	}
	type parsed struct {
		id  string
		m   backend.Message
		err error
	}
	items := make([]parsed, 0, len(indices))
	seen := map[string]bool{}
	size := 0
	for _, i := range indices {
		prefix := "PublishBatchRequestEntries.member." + i + "."
		entry := url.Values{}
		for key, value := range q {
			if strings.HasPrefix(key, prefix) {
				entry[strings.TrimPrefix(key, prefix)] = value
			}
		}
		id := entry.Get("Id")
		if !batchID.MatchString(id) {
			return nil, &apiError{"InvalidBatchEntryId", "Invalid batch entry ID", 400}
		}
		if seen[id] {
			return nil, &apiError{"BatchEntryIdsNotDistinct", "Batch entry IDs must be unique", 400}
		}
		seen[id] = true
		m, err := parseMessage(entry, topic)
		size += messageSize(m)
		items = append(items, parsed{id, m, err})
	}
	if size > maxMessageSize {
		return nil, &apiError{"BatchRequestTooLong", "Batch exceeds 256 KiB", 400}
	}
	result := batchResult{}
	for _, item := range items {
		if item.err != nil {
			result.Failed = append(result.Failed, batchFailure{item.id, "InvalidParameter", item.err.Error(), true})
			continue
		}
		if err := g.publish(ctx, item.m); err != nil {
			g.opts.Logger.Error("batch publish failed", "error", err)
			result.Failed = append(result.Failed, batchFailure{item.id, "InternalError", "Unable to persist message", false})
			continue
		}
		result.Successful = append(result.Successful, batchSuccess{item.id, item.m.ID})
	}
	return result, nil
}
