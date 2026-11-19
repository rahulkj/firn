package egress

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// S3Sink uploads each rendered batch partition to S3 (or S3-compatible
// endpoints such as MinIO) using a hand-rolled AWS Signature Version 4
// signer — no SDK dependency. PUT, path-style or virtual-hosted addressing.
// Retries are idempotent: the object key is a pure function of the offset
// range, and the payload carries no wall-clock data (except the informational
// record ts, which changes on redelivery — safe: same name, same content shape).
// Verified against a conformance stub in tests; validate against your
// endpoint before relying on it (see docs/integrations.md).

type s3Sink struct {
	bucket         string
	prefix         string
	region         string
	endpoint       string // optional override (MinIO etc.)
	forcePathStyle bool
	accessKey      string
	secretKey      string
	sessionToken   string
	hc             *http.Client
	now            func() time.Time
}

func newS3Sink(r Route) (Sink, error) {
	region := r.Sink.Region
	return &s3Sink{
		bucket:         r.Sink.Bucket,
		prefix:         strings.TrimPrefix(strings.TrimSuffix(r.Sink.Prefix, "/"), "/"),
		region:         region,
		endpoint:       strings.TrimRight(r.Sink.Endpoint, "/"),
		forcePathStyle: r.Sink.ForcePathStyle,
		accessKey:      os.Getenv("AWS_ACCESS_KEY_ID"),
		secretKey:      os.Getenv("AWS_SECRET_ACCESS_KEY"),
		sessionToken:   os.Getenv("AWS_SESSION_TOKEN"),
		hc:             &http.Client{Timeout: 60 * time.Second},
		now:            time.Now,
	}, nil
}

func (s *s3Sink) Write(ctx context.Context, route string, batch []Delivery) error {
	parts, err := renderPartitions(route, batch)
	if err != nil {
		return err
	}
	for _, p := range parts {
		for _, file := range []struct{ name string; data []byte }{
			{p.filename, p.jsonl},
			{p.filename + ".manifest.json", p.manifest},
		} {
			key := route + "/" + file.name
			if s.prefix != "" {
				key = s.prefix + "/" + key
			}
			req, err := s.putObjectRequest(ctx, key, file.data)
			if err != nil {
				return err
			}
			s.sign(req, file.data, s.now())
			resp, err := s.hc.Do(req)
			if err != nil {
				return fmt.Errorf("s3 sink PUT %s: %w", key, err) // retryable
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			switch {
			case resp.StatusCode >= 200 && resp.StatusCode < 300:
			case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
				return fmt.Errorf("s3 sink PUT %s: HTTP %d", key, resp.StatusCode)
			default:
				return &TerminalError{Err: fmt.Errorf("s3 sink PUT %s: HTTP %d: %.200s", key, resp.StatusCode, body)}
			}
		}
	}
	return nil
}

func (s *s3Sink) putObjectRequest(ctx context.Context, key string, payload []byte) (*http.Request, error) {
	escapedKey := uriEncodePath(key)
	var host, path string
	switch {
	case s.endpoint != "":
		if s.forcePathStyle {
			path = "/" + s.bucket + "/" + escapedKey
			return http.NewRequestWithContext(ctx, http.MethodPut, s.endpoint+path, bytes.NewReader(payload))
		}
		host = s.bucket + "." + strings.TrimPrefix(strings.TrimPrefix(s.endpoint, "https://"), "http://")
		path = "/" + escapedKey
		return http.NewRequestWithContext(ctx, http.MethodPut, s.endpointScheme(s.endpoint)+"://"+host+path, bytes.NewReader(payload))
	case s.forcePathStyle:
		host = "s3." + s.region + ".amazonaws.com"
		path = "/" + s.bucket + "/" + escapedKey
		return http.NewRequestWithContext(ctx, http.MethodPut, "https://"+host+path, bytes.NewReader(payload))
	default:
		host = s.bucket + ".s3." + s.region + ".amazonaws.com"
		path = "/" + escapedKey
		return http.NewRequestWithContext(ctx, http.MethodPut, "https://"+host+path, bytes.NewReader(payload))
	}
}

func (s *s3Sink) endpointScheme(ep string) string {
	if strings.HasPrefix(ep, "https://") {
		return "https"
	}
	return "http"
}

// sign applies AWS Signature Version 4 to req (PUT object).
func (s *s3Sink) sign(req *http.Request, payload []byte, t time.Time) {
	utc := t.UTC()
	amzDate := utc.Format("20060102T150405Z")
	dateStamp := utc.Format("20060102")
	payloadHash := hexSHA256(payload)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("x-amz-date", amzDate)
	host := req.URL.Host

	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonHeaders := "host:" + host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	if s.sessionToken != "" {
		req.Header.Set("x-amz-security-token", s.sessionToken)
		signedHeaders += ";x-amz-security-token"
		canonHeaders += "x-amz-security-token:" + s.sessionToken + "\n"
	}
	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		"", // canonical query string (empty)
		canonHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
	scope := dateStamp + "/" + s.region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hexSHA256([]byte(canonicalRequest)),
	}, "\n")
	kDate := hmacSHA256([]byte("AWS4"+s.secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(s.region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	sig := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.accessKey+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+sig)
}

func (s *s3Sink) IsTerminal(err error) bool {
	var te *TerminalError
	return errorsAs(err, &te)
}

func (s *s3Sink) Close() error { return nil }

func hexSHA256(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key, msg []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(msg)
	return h.Sum(nil)
}

func uriEncodePath(p string) string {
	parts := strings.Split(p, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

