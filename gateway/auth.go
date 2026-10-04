package gateway

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

func hash(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func mac(key []byte, value string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(value))
	return h.Sum(nil)
}
func awsEncode(s string) string { return strings.ReplaceAll(url.QueryEscape(s), "+", "%20") }
func canonicalQuery(q url.Values) string {
	var parts []string
	for key, values := range q {
		if key != "X-Amz-Signature" {
			for _, value := range values {
				parts = append(parts, awsEncode(key)+"="+awsEncode(value))
			}
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "&")
}

func (g *Gateway) authenticate(r *http.Request, body []byte) error {
	fail := func(message string) error { return &apiError{"SignatureDoesNotMatch", message, 403} }
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return fail("Malformed query")
	}
	auth := r.Header.Get("Authorization")
	date, credential, signed, signature := r.Header.Get("X-Amz-Date"), "", "", ""
	presigned := q.Get("X-Amz-Algorithm") != ""
	if presigned {
		if auth != "" || q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" {
			return fail("Invalid authentication method")
		}
		date, credential, signed, signature = q.Get("X-Amz-Date"), q.Get("X-Amz-Credential"), q.Get("X-Amz-SignedHeaders"), q.Get("X-Amz-Signature")
	} else {
		if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
			return &apiError{"AuthorizationError", "AWS Signature Version 4 is required", 403}
		}
		fields := map[string]string{}
		for _, part := range strings.Split(strings.TrimPrefix(auth, "AWS4-HMAC-SHA256 "), ",") {
			kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
			if len(kv) != 2 || fields[kv[0]] != "" {
				return fail("Malformed authorization")
			}
			fields[kv[0]] = kv[1]
		}
		credential, signed, signature = fields["Credential"], fields["SignedHeaders"], fields["Signature"]
	}
	scope := strings.Split(credential, "/")
	if len(scope) != 5 || scope[0] != g.opts.AccessKey || scope[2] != g.opts.Region || scope[3] != "sns" || scope[4] != "aws4_request" {
		return fail("Invalid credential scope")
	}
	if r.Header.Get("X-Amz-Security-Token") != "" || q.Get("X-Amz-Security-Token") != "" {
		return &apiError{"InvalidClientTokenId", "Session credentials are not supported", 403}
	}
	t, err := time.Parse("20060102T150405Z", date)
	if err != nil || scope[1] != t.Format("20060102") {
		return fail("Invalid signing date")
	}
	now := time.Now()
	if presigned {
		seconds, err := strconv.ParseInt(q.Get("X-Amz-Expires"), 10, 64)
		if err != nil || seconds < 1 || seconds > 604800 || now.Before(t.Add(-5*time.Minute)) || now.After(t.Add(time.Duration(seconds)*time.Second)) {
			return fail("Request has expired or invalid expiry")
		}
	} else if now.Sub(t) > 5*time.Minute || t.Sub(now) > 5*time.Minute {
		return fail("Request time is too skewed")
	}
	names := strings.Split(signed, ";")
	seen := map[string]bool{}
	var headers strings.Builder
	for i, name := range names {
		if name == "" || name != strings.ToLower(name) || (i > 0 && name <= names[i-1]) {
			return fail("Invalid signed headers")
		}
		values := append([]string(nil), r.Header.Values(name)...)
		if name == "host" {
			values = []string{r.Host}
		}
		if name == "content-length" && len(values) == 0 {
			values = []string{strconv.FormatInt(r.ContentLength, 10)}
		}
		if len(values) == 0 {
			return fail("A signed header is missing")
		}
		for j := range values {
			values[j] = strings.Join(strings.Fields(values[j]), " ")
		}
		headers.WriteString(name + ":" + strings.Join(values, ",") + "\n")
		seen[name] = true
	}
	if !seen["host"] || (!presigned && !seen["x-amz-date"]) {
		return fail("Host and signing date must be signed")
	}
	payload := hash(body)
	if declared := r.Header.Get("X-Amz-Content-Sha256"); declared != "" && declared != payload {
		return fail("Payload hash mismatch")
	}
	// The Query API is only exposed at /, avoiding ambiguous path normalization.
	canonical := r.Method + "\n/\n" + canonicalQuery(q) + "\n" + headers.String() + "\n" + signed + "\n" + payload
	key := mac([]byte("AWS4"+g.opts.SecretKey), scope[1])
	key = mac(key, scope[2])
	key = mac(key, "sns")
	key = mac(key, "aws4_request")
	expected := mac(key, "AWS4-HMAC-SHA256\n"+date+"\n"+strings.Join(scope[1:], "/")+"\n"+hash([]byte(canonical)))
	provided, err := hex.DecodeString(signature)
	if err != nil || !hmac.Equal(expected, provided) {
		return fail("The request signature does not match")
	}
	return nil
}
