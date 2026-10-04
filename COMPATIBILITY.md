# SNS compatibility

Initial scope: AWS SNS Query API version `2010-03-31`, standard topics, one account/region, HTTP/HTTPS subscriptions. The implementation follows a subset of the [SNS API reference](https://docs.aws.amazon.com/sns/latest/api/Welcome.html).

| Operation / feature | Implemented scope |
| --- | --- |
| CreateTopic, DeleteTopic | Standard topics; idempotent create/delete; deletion removes subscriptions |
| ListTopics | Sorted, signed opaque pagination tokens; 100 items per page |
| GetTopicAttributes, SetTopicAttributes | DisplayName; computed ARN, owner, subscription counts, signature version |
| Subscribe, ConfirmSubscription, Unsubscribe | HTTP/HTTPS, confirmation tokens, idempotency, capability links |
| ListSubscriptions, ListSubscriptionsByTopic | Sorted pagination and pending status |
| GetSubscriptionAttributes, SetSubscriptionAttributes | Identity/status fields, RawMessageDelivery, FilterPolicy, FilterPolicyScope |
| Subscription filtering | Attribute or JSON-body scope; exact values, prefix, numeric comparisons, anything-but, exists; nested body objects and arrays |
| Publish | TopicArn target, UTF-8 message, subject, message attributes, 256 KiB limit |
| PublishBatch | Up to ten distinct IDs; aggregate size limit; per-entry successes/failures |
| TagResource, UntagResource, ListTagsForResource | Topic tags, up to 50 |
| Authentication | Header and query SigV4, static credentials, region/scope/date/body verification |
| HTTP delivery | Confirmation, JSON envelope or raw body, SNS headers, RSA/SHA-256 signing |
| Durability | NATS file storage, independent durable consumers, metadata CAS, deletion tombstones |

Explicitly unsupported: FIFO topics, deduplication/group parameters, IAM/resource policies, KMS, configurable delivery policies, dead-letter queues, protocol-specific JSON messages, SQS/Lambda/email/SMS/mobile/Firehose subscriptions, platform applications/endpoints, phone-number operations, STS/session credentials, Signature V2, and multi-account/multi-region routing. Unsupported actions or supported-resource attributes are rejected. Exact AWS validation and error-code parity is not claimed.

Filtering implements a subset of [SNS subscription filter policies](https://docs.aws.amazon.com/sns/latest/dg/sns-subscription-filter-policies.html). Supported operators are exact scalar values, `prefix`, `numeric`, `anything-but` (homogeneous strings/numbers or a nested prefix), and `exists`. `$or`, `suffix`, `equals-ignore-case`, `cidr`, and `wildcard` are rejected. `exists` applies to leaf values; `exists:false` requires a nonempty input object/attribute set. Empty-object policies disable filtering. Body policies match nested objects and arrays of objects, with nested rules evaluated within a single array object. Policies enforce the 256 KiB, five-leaf-key and 150-combination limits, plus a gateway limit of 32 nesting levels. AWS account/topic filter-count quotas and attribute double-escaping quirks are not emulated.

The gateway evaluates the latest persisted policy before each notification delivery attempt, including queued messages and retries, rather than emulating AWS's filter-update propagation delay. Nonmatches are acknowledged and cannot be recovered by later changing a policy. Confirmation messages bypass filtering. Invalid JSON bodies do not match an active body policy. Policy persistence uses the existing backend metadata store and its capacity limit. See [subscription filtering](README.md#subscription-filtering) for examples.

HTTP retry classification follows [SNS HTTP delivery behavior](https://docs.aws.amazon.com/sns/latest/dg/sns-message-delivery-retries.html), but this gateway uses a fixed five-second delay with unlimited retries. AWS delivery-policy schedules and exhaustion behavior are not implemented. Raw HTTP messages omit attributes, consistent with [SNS raw delivery](https://docs.aws.amazon.com/sns/latest/dg/sns-large-payload-raw-message-delivery.html).

Envelope certificates are gateway-owned and require explicit client trust. Signing covers the SNS canonical fields; as with SNS, message attributes are outside that signature. HTTPS protects the complete transport payload. Notification signing and incoming SigV4 authentication are separate mechanisms.

## Verification status

| Check | Initial status |
| --- | --- |
| AWS SDK SigV4 signer → gateway Query API | Passed |
| HTTP confirmation, fan-out, retries, raw delivery, RSA verification | Passed |
| Pagination, validation, tags, batch partial failures | Passed |
| Gateway restart over retained backend state | Passed with memory backend |
| Memory CAS/snapshot isolation and independent delivery cursors | Passed |
| Core race detector and vet | Passed |
| Debug event correlation, level filtering, and secret redaction | Covered by core suite |
| Subscription filter matching, validation, live updates, acknowledgements and restart persistence | Covered by core suite |
| NATS adapter compilation and live JetStream test | Passed |
| Complete SNS SDK serialization test (`tests/sdk`) | Passed |
| Go client/server examples against NATS-backed gateway | Passed: confirmation, signed delivery, unsigned-message rejection |
| Gateway container image build | Not yet verified |

The container build is not covered by the passing Go checks. Operating and recovery limits are described in [README.md](README.md#configuration). No full AWS SNS conformance or production-readiness claim is made.
