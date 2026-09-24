package asyncworker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	s3store "github.com/llm-d/llm-d-async/pkg/resultstore/s3"
)

func TestReturnedInline(t *testing.T) {
	for contentType, inline := range map[string]bool{
		"":                                true,
		"application/json":                true,
		"application/json; charset=utf-8": true,
		"application/problem+json":        true,
		"audio/mpeg":                      false,
		"audio/wav":                       false,
		"text/event-stream":               false,
		"application/octet-stream":        false,
		"not a media type;;":              false,
	} {
		assert.Equal(t, inline, returnedInline(contentType), contentType)
	}
}

func TestResultObjectKeyIsPerDispatchGeneration(t *testing.T) {
	msg := newEmbR(asyncapi.InternalRouting{RequestToken: "tok/1"}, asyncapi.RequestMessage{ID: "batch 7/line 3"}, "", nil)
	assert.Equal(t, "batch%207%2Fline%203/tok%2F1", resultObjectKey(msg))

	untokened := newEmb(asyncapi.RequestMessage{ID: "req-9"}, "", nil)
	assert.Equal(t, "req-9", resultObjectKey(untokened))
}

var (
	s3BucketOnce sync.Once
	s3Bucket     string
	s3BucketErr  error
)

// s3Fixture returns a store on the S3 endpoint in TEST_S3_ENDPOINT, keyed under this test's
// name, plus a client to read back what the worker wrote. It skips when the endpoint is unset.
func s3Fixture(t *testing.T) (*s3store.Store, *awss3.Client, string) {
	t.Helper()
	endpoint := os.Getenv("TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("TEST_S3_ENDPOINT not set")
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}
	ctx := context.Background()
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
	require.NoError(t, err)
	client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	s3BucketOnce.Do(func() {
		s3Bucket = fmt.Sprintf("worker-results-%d", time.Now().UnixNano())
		_, s3BucketErr = client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(s3Bucket)})
	})
	require.NoError(t, s3BucketErr)
	store, err := s3store.New(ctx, s3store.Config{Endpoint: endpoint, Bucket: s3Bucket, Prefix: t.Name(), PathStyle: true})
	require.NoError(t, err)
	return store, client, s3Bucket
}

func respondWith(status int, contentType string, body []byte) *http.Client {
	return NewTestClient(func(*http.Request) (*http.Response, error) {
		h := make(http.Header)
		h.Set("Content-Type", contentType)
		return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(bytes.NewReader(body))}, nil
	})
}

func runOne(t *testing.T, store ResultStore, client *http.Client, msg pipeline.EmbelishedRequestMessage) (chan asyncapi.ResultMessage, chan pipeline.RetryMessage) {
	t.Helper()
	requestChannel := make(chan pipeline.EmbelishedRequestMessage, 1)
	retryChannel := make(chan pipeline.RetryMessage, 1)
	resultChannel := make(chan asyncapi.ResultMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go Worker(ctx, WithResultStore(ctx, store), pipeline.Characteristics{HasExternalBackoff: false}, NewHTTPInferenceClient(client), requestChannel, retryChannel, resultChannel, defaultRequestTimeout, nil)
	requestChannel <- msg
	return resultChannel, retryChannel
}

func speechRequest(id, token string) pipeline.EmbelishedRequestMessage {
	return newEmbR(asyncapi.InternalRouting{RequestToken: token}, asyncapi.RequestMessage{
		ID:       id,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(100 * time.Second).Unix(),
		Payload:  map[string]any{"model": "tts", "input": "hello", "response_format": "mp3"},
	}, "http://localhost:30800/v1/audio/speech", map[string]string{})
}

func TestWorkerStoresANonJSONBodyByReference(t *testing.T) {
	store, client, bucket := s3Fixture(t)
	audio := bytes.Repeat([]byte{0xff, 0xfb, 0x90, 0x00, 0xc3, 0x28}, 40_000)
	results, retries := runOne(t, store, respondWith(http.StatusOK, "audio/mpeg", audio), speechRequest("speech-1", "gen-a"))

	select {
	case r := <-results:
		sum := sha256.Sum256(audio)
		key := t.Name() + "/speech-1/gen-a"
		assert.Equal(t, http.StatusOK, r.StatusCode)
		assert.Empty(t, r.Payload, "a body stored by reference must not also travel inline")
		assert.Equal(t, "s3://"+bucket+"/"+key, r.PayloadRef)
		assert.Equal(t, "audio/mpeg", r.ContentType)
		assert.EqualValues(t, len(audio), r.PayloadSize)
		assert.Equal(t, hex.EncodeToString(sum[:]), r.PayloadSHA256)

		out, err := client.GetObject(context.Background(), &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		require.NoError(t, err)
		defer func() { _ = out.Body.Close() }()
		stored, err := io.ReadAll(out.Body)
		require.NoError(t, err)
		assert.Equal(t, audio, stored)
		assert.Equal(t, "audio/mpeg", aws.ToString(out.ContentType))
	case r := <-retries:
		t.Fatalf("stored response was retried: %+v", r)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for the result")
	}
}

func TestWorkerKeepsJSONInlineWithAStore(t *testing.T) {
	store, _, _ := s3Fixture(t)
	body := []byte(`{"choices":[{"text":"hi"}]}`)
	results, _ := runOne(t, store, respondWith(http.StatusOK, "application/json", body), speechRequest("json-1", "gen-a"))

	select {
	case r := <-results:
		assert.Equal(t, string(body), r.Payload)
		assert.Empty(t, r.PayloadRef)
		assert.Empty(t, r.ContentType)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for the result")
	}
}

func TestWorkerKeepsErrorBodiesInline(t *testing.T) {
	store, _, _ := s3Fixture(t)
	results, _ := runOne(t, store, respondWith(http.StatusBadRequest, "text/plain", []byte("bad voice")), speechRequest("bad-1", "gen-a"))

	select {
	case r := <-results:
		assert.Equal(t, http.StatusBadRequest, r.StatusCode)
		assert.Equal(t, "bad voice", r.Payload)
		assert.Empty(t, r.PayloadRef)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for the result")
	}
}

func TestWorkerRetriesWhenTheBodyCannotBeStored(t *testing.T) {
	_, _, _ = s3Fixture(t)
	missing, err := s3store.New(context.Background(), s3store.Config{
		Endpoint: os.Getenv("TEST_S3_ENDPOINT"), Bucket: "no-such-bucket-" + strings.ToLower(fmt.Sprint(time.Now().UnixNano())), PathStyle: true,
	})
	require.NoError(t, err)
	results, retries := runOne(t, missing, respondWith(http.StatusOK, "audio/wav", []byte("RIFF....WAVE")), speechRequest("retry-1", "gen-a"))

	select {
	case r := <-retries:
		assert.Equal(t, "retry-1", r.PublicRequest.ReqID())
	case r := <-results:
		t.Fatalf("an unstored body produced a final result: %+v", r)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for the retry")
	}
}

func TestWorkerWithoutAStoreKeepsTheBodyInline(t *testing.T) {
	results, _ := runOne(t, nil, respondWith(http.StatusOK, "audio/wav", []byte("RIFF....WAVE")), speechRequest("inline-1", "gen-a"))

	select {
	case r := <-results:
		assert.Equal(t, "RIFF....WAVE", r.Payload)
		assert.Empty(t, r.PayloadRef)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for the result")
	}
}

type readFunc func(p []byte) (int, error)

func (f readFunc) Read(p []byte) (int, error) { return f(p) }

// observedStore wraps a real store and closes started the first time the upload reads the body.
type observedStore struct {
	inner   ResultStore
	started chan struct{}
	once    sync.Once
}

func (s *observedStore) Put(ctx context.Context, key, contentType string, body io.Reader) (string, error) {
	return s.inner.Put(ctx, key, contentType, readFunc(func(p []byte) (int, error) {
		s.once.Do(func() { close(s.started) })
		return body.Read(p)
	}))
}

func TestWorkerStreamsTheBodyIntoTheStoreAsItArrives(t *testing.T) {
	store, client, bucket := s3Fixture(t)
	observed := &observedStore{inner: store, started: make(chan struct{})}

	chunk := bytes.Repeat([]byte{0x52, 0x49, 0x46, 0x46, 0x10, 0x00}, 1<<17)
	const chunks = 12
	sum := sha256.New()
	pr, pw := io.Pipe()
	go func() {
		for i := range chunks {
			if i == 1 {
				select {
				case <-observed.started:
				case <-time.After(5 * time.Second):
					_ = pw.CloseWithError(fmt.Errorf("the body was not streamed: the upload never started while the response was still arriving"))
					return
				}
			}
			_, _ = sum.Write(chunk)
			if _, err := pw.Write(chunk); err != nil {
				return
			}
		}
		_ = pw.Close()
	}()
	httpClient := NewTestClient(func(*http.Request) (*http.Response, error) {
		h := make(http.Header)
		h.Set("Content-Type", "audio/wav")
		return &http.Response{StatusCode: http.StatusOK, Header: h, Body: pr}, nil
	})

	results, retries := runOne(t, observed, httpClient, speechRequest("stream-1", "gen-a"))
	select {
	case r := <-results:
		assert.Empty(t, r.Payload)
		assert.EqualValues(t, chunks*len(chunk), r.PayloadSize)
		assert.Equal(t, hex.EncodeToString(sum.Sum(nil)), r.PayloadSHA256)
		out, err := client.GetObject(context.Background(), &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(t.Name() + "/stream-1/gen-a")})
		require.NoError(t, err)
		defer func() { _ = out.Body.Close() }()
		stored, err := io.ReadAll(out.Body)
		require.NoError(t, err)
		got := sha256.Sum256(stored)
		assert.Equal(t, r.PayloadSHA256, hex.EncodeToString(got[:]))
	case r := <-retries:
		t.Fatalf("streamed response was retried: %+v", r)
	case <-time.After(20 * time.Second):
		t.Fatal("timeout waiting for the result")
	}
}
