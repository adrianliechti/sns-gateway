package gateway

import (
	"encoding/json"
	"io"
	"math"
	"math/big"
	"strings"
	"unicode/utf8"

	"github.com/adrianliechti/sns-gateway/backend"
)

const (
	filterAttributes = "MessageAttributes"
	filterBody       = "MessageBody"
	maxFilterSize    = 256 * 1024
)

// Policies are compiled once per worker and recompiled when their persisted
// policy or scope changes. The compiled values are immutable and backend-neutral.
type filterPolicy map[string]filterField

type filterField struct {
	nested filterPolicy
	rules  []filterRule
}

type filterRule struct {
	exists *bool
	match  func(any) bool
}

func effectiveFilterScope(scope string) string {
	if scope == "" {
		return filterAttributes
	}
	return scope
}

func subscriptionAttributes(sub backend.Subscription, attributes map[string]string) (backend.Subscription, error) {
	for key, value := range attributes {
		switch key {
		case "RawMessageDelivery":
			raw, err := strictBool(value)
			if err != nil {
				return sub, err
			}
			sub.Raw = raw
		case "FilterPolicy":
			if value == "" {
				return sub, invalid("FilterPolicy must be a JSON object; use {} to remove filtering")
			}
			sub.FilterPolicy = value
		case "FilterPolicyScope":
			if value != filterAttributes && value != filterBody {
				return sub, invalid("FilterPolicyScope must be MessageAttributes or MessageBody")
			}
			sub.FilterPolicyScope = value
		default:
			return sub, unsupported("subscription attribute " + key)
		}
	}
	// Validate the complete proposed state, independent of attribute map order.
	// This also rejects changing a nested body policy to attribute scope.
	_, err := compileFilter(sub.FilterPolicy, sub.FilterPolicyScope)
	return sub, err
}

func compileFilter(raw, scope string) (filterPolicy, error) {
	scope = effectiveFilterScope(scope)
	if scope != filterAttributes && scope != filterBody {
		return nil, invalid("Invalid FilterPolicyScope")
	}
	if raw == "" {
		return nil, nil
	}
	if len(raw) > maxFilterSize || !utf8.ValidString(raw) {
		return nil, invalid("FilterPolicy must be UTF-8 and at most 256 KiB")
	}
	value, err := decodeFilterJSON(raw)
	if err != nil {
		return nil, invalid("FilterPolicy must be a valid JSON object")
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, invalid("FilterPolicy must be a JSON object")
	}
	keys, combinations := 0, 1
	var compileObject func(map[string]any, int) (filterPolicy, error)
	compileObject = func(object map[string]any, depth int) (filterPolicy, error) {
		if depth > 32 {
			return nil, invalid("FilterPolicy nesting exceeds 32 levels")
		}
		policy := make(filterPolicy, len(object))
		for key, value := range object {
			if key == "$or" {
				return nil, unsupported("FilterPolicy $or")
			}
			field := filterField{}
			switch v := value.(type) {
			case map[string]any:
				if scope != filterBody || len(v) == 0 {
					return nil, invalid("Nested FilterPolicy objects require MessageBody scope and at least one property")
				}
				field.nested, err = compileObject(v, depth+1)
				if err != nil {
					return nil, err
				}
			case []any:
				keys++
				if keys > 5 || len(v) == 0 || len(v) > 150 || combinations > 150/(len(v)*depth) {
					return nil, invalid("FilterPolicy requires nonempty rule arrays, at most five leaf keys and at most 150 combinations")
				}
				combinations *= len(v) * depth
				for _, value := range v {
					rule, err := compileFilterRule(value)
					if err != nil {
						return nil, err
					}
					field.rules = append(field.rules, rule)
				}
			default:
				return nil, invalid("FilterPolicy properties must contain rule arrays or nested objects")
			}
			policy[key] = field
		}
		return policy, nil
	}
	return compileObject(object, 1)
}

// UseNumber preserves the distinction between strings and JSON numbers.
func decodeFilterJSON(raw string) (any, error) {
	d := json.NewDecoder(strings.NewReader(raw))
	d.UseNumber()
	var value any
	if err := d.Decode(&value); err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, invalid("Expected one JSON value")
	}
	return value, nil
}

func compileFilterRule(value any) (filterRule, error) {
	object, operator := value.(map[string]any)
	if !operator {
		if !validFilterScalar(value) {
			return filterRule{}, invalid("Invalid FilterPolicy value")
		}
		return filterRule{match: func(candidate any) bool { return equalFilterValues(candidate, value) }}, nil
	}
	if len(object) != 1 {
		return filterRule{}, invalid("FilterPolicy rules must contain exactly one operator")
	}
	for op, operand := range object {
		switch op {
		case "exists":
			value, ok := operand.(bool)
			if !ok {
				return filterRule{}, invalid("FilterPolicy exists requires a boolean")
			}
			return filterRule{exists: &value}, nil
		case "prefix":
			prefix, ok := operand.(string)
			if !ok {
				return filterRule{}, invalid("FilterPolicy prefix requires a string")
			}
			return filterRule{match: func(candidate any) bool {
				value, ok := candidate.(string)
				return ok && strings.HasPrefix(value, prefix)
			}}, nil
		case "numeric":
			return compileNumericFilter(operand)
		case "anything-but":
			return compileAnythingButFilter(operand)
		default:
			return filterRule{}, unsupported("FilterPolicy operator " + op)
		}
	}
	return filterRule{}, invalid("Invalid FilterPolicy operator")
}

func compileNumericFilter(operand any) (filterRule, error) {
	values, ok := operand.([]any)
	if !ok || len(values) == 0 || len(values)%2 != 0 {
		return filterRule{}, invalid("FilterPolicy numeric requires operator/number pairs")
	}
	type comparison struct {
		op    string
		value float64
	}
	var comparisons []comparison
	for i := 0; i < len(values); i += 2 {
		op, _ := values[i].(string)
		n, ok := filterNumber(values[i+1])
		if !ok || !validFilterScalar(values[i+1]) || (op != "=" && op != "<" && op != "<=" && op != ">" && op != ">=") {
			return filterRule{}, invalid("Invalid FilterPolicy numeric comparison")
		}
		comparisons = append(comparisons, comparison{op, n})
	}
	return filterRule{match: func(candidate any) bool {
		n, ok := filterNumber(candidate)
		if !ok {
			return false
		}
		for _, c := range comparisons {
			if !(c.op == "=" && n == c.value || c.op == "<" && n < c.value || c.op == "<=" && n <= c.value || c.op == ">" && n > c.value || c.op == ">=" && n >= c.value) {
				return false
			}
		}
		return true
	}}, nil
}

func compileAnythingButFilter(operand any) (filterRule, error) {
	if object, ok := operand.(map[string]any); ok {
		if _, ok := object["prefix"]; !ok || len(object) != 1 {
			return filterRule{}, unsupported("FilterPolicy anything-but supports only a nested prefix operator")
		}
		rule, err := compileFilterRule(object)
		if err != nil {
			return filterRule{}, err
		}
		return filterRule{match: func(candidate any) bool {
			_, ok := candidate.(string)
			return ok && !rule.match(candidate)
		}}, nil
	}
	values, ok := operand.([]any)
	if !ok {
		values = []any{operand}
	}
	if len(values) == 0 {
		return filterRule{}, invalid("FilterPolicy anything-but requires values")
	}
	_, stringsOnly := values[0].(string)
	for _, value := range values {
		_, isString := value.(string)
		_, isNumber := filterNumber(value)
		if !validFilterScalar(value) || (!isString && !isNumber) || isString != stringsOnly {
			return filterRule{}, invalid("FilterPolicy anything-but requires strings or numbers of the same type")
		}
	}
	return filterRule{match: func(candidate any) bool {
		_, isString := candidate.(string)
		_, isNumber := filterNumber(candidate)
		if (!isString && !isNumber) || isString != stringsOnly {
			return false
		}
		for _, value := range values {
			if equalFilterValues(candidate, value) {
				return false
			}
		}
		return true
	}}, nil
}

func filterNumber(value any) (float64, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	n, err := number.Float64()
	return n, err == nil && !math.IsNaN(n) && !math.IsInf(n, 0)
}

func validFilterScalar(value any) bool {
	switch value := value.(type) {
	case nil, bool, string:
		return true
	case json.Number:
		n, ok := filterNumber(value)
		if !ok || math.Abs(n) > 1e9 || n != 0 && math.Abs(n) < 1e-5 {
			return false
		}
		if n == 0 {
			// Reject nonzero values that underflowed Float64 without parsing an
			// arbitrarily large negative exponent as a rational number.
			mantissa := strings.FieldsFunc(string(value), func(r rune) bool { return r == 'e' || r == 'E' })[0]
			return strings.Trim(mantissa, "-0.") == ""
		}
		// Validate decimal precision exactly, without floating-point rounding.
		r, ok := new(big.Rat).SetString(string(value))
		return ok && r.Mul(r, big.NewRat(100000, 1)).IsInt()
	default:
		return false
	}
}

func equalFilterValues(left, right any) bool {
	if n, ok := filterNumber(right); ok {
		m, ok := filterNumber(left)
		return ok && n == m
	}
	switch right := right.(type) {
	case nil:
		return left == nil
	case string:
		value, ok := left.(string)
		return ok && value == right
	case bool:
		value, ok := left.(bool)
		return ok && value == right
	}
	return false
}

func (p filterPolicy) matches(message backend.Message, scope string) bool {
	if len(p) == 0 {
		return true
	}
	var values map[string]any
	if effectiveFilterScope(scope) == filterBody {
		value, err := decodeFilterJSON(message.Body)
		if err != nil {
			return false
		}
		values, _ = value.(map[string]any)
	} else {
		values = make(map[string]any, len(message.Attributes))
		for key, attribute := range message.Attributes {
			switch strings.SplitN(attribute.Type, ".", 2)[0] {
			case "String":
				if attribute.Type == "String.Array" {
					value, err := decodeFilterJSON(attribute.Value)
					if array, ok := value.([]any); err == nil && ok {
						values[key] = array
					}
				} else {
					values[key] = attribute.Value
				}
			case "Number":
				values[key] = json.Number(attribute.Value)
			}
		}
	}
	return p.matchesObject(values)
}

func (p filterPolicy) matchesObject(values map[string]any) bool {
	if len(values) == 0 {
		return false
	}
	for key, field := range p {
		value, present := values[key]
		if !field.matches(value, present) {
			return false
		}
	}
	return true
}

func (f filterField) matches(value any, present bool) bool {
	values, array := value.([]any)
	if !array {
		values = []any{value}
	}
	if f.nested != nil {
		for _, value := range values {
			if object, ok := value.(map[string]any); ok && f.nested.matchesObject(object) {
				return true
			}
		}
		return false
	}
	for _, rule := range f.rules {
		if rule.exists != nil && !*rule.exists && !present {
			return true
		}
		if !present {
			continue
		}
		for _, candidate := range values {
			if rule.exists != nil {
				if *rule.exists && candidate != nil && candidate != "" {
					switch candidate.(type) {
					case string, json.Number, bool:
						return true
					}
				}
			} else if rule.match(candidate) {
				return true
			}
		}
	}
	return false
}
