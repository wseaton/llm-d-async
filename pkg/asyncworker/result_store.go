package asyncworker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"net/url"
	"strings"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
)

// ResultStore holds response bodies that are returned by reference instead of inline.
type ResultStore interface {
	// Put streams body under key and returns a reference to it (for example s3://bucket/key).
	Put(ctx context.Context, key, contentType string, body io.Reader) (string, error)
}

type resultStoreKey struct{}

// WithResultStore returns a context whose workers store non-JSON response bodies in store.
func WithResultStore(ctx context.Context, store ResultStore) context.Context {
	if store == nil {
		return ctx
	}
	return context.WithValue(ctx, resultStoreKey{}, store)
}

func resultStoreFromContext(ctx context.Context) ResultStore {
	store, _ := ctx.Value(resultStoreKey{}).(ResultStore)
	return store
}

// returnedInline reports whether a response body of this content type stays in the result
// message: JSON (and +json) bodies, and bodies that declare no type, keep today's inline path.
func returnedInline(contentType string) bool {
	if contentType == "" {
		return true
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	return mediaType == "application/json" || strings.HasSuffix(mediaType, "+json")
}

// resultObjectKey is <request id>/<request token>, so every dispatch generation gets its own
// object and a duplicate dispatch never overwrites a delivered result.
func resultObjectKey(msg pipeline.EmbelishedRequestMessage) string {
	key := url.PathEscape(msg.PublicRequest.ReqID())
	if token := msg.RequestToken; token != "" {
		key += "/" + url.PathEscape(token)
	}
	return key
}

// storeBody streams body into store, counting and hashing it on the way, and builds the
// reference result for it.
func storeBody(ctx context.Context, store ResultStore, msg pipeline.EmbelishedRequestMessage, statusCode int, contentType string, body io.Reader) (asyncapi.ResultMessage, error) {
	hash := sha256.New()
	counted := &countingReader{r: io.TeeReader(body, hash)}
	ref, err := store.Put(ctx, resultObjectKey(msg), contentType, counted)
	if err != nil {
		return asyncapi.ResultMessage{}, fmt.Errorf("store response body: %w", err)
	}
	return asyncapi.NewHTTPRefResult(msg.PublicRequest, msg.InternalRouting, statusCode, ref, contentType, counted.n, hex.EncodeToString(hash.Sum(nil))), nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err //nolint:wrapcheck // io.Reader returns io.EOF unwrapped
}

// sendStoring sends the request. With a store, a successful non-JSON body goes into the store
// and stored is the reference result for it; the HTTP client streams it there as it arrives,
// other clients store their buffered body. A failed store is a retryable server error.
func sendStoring(ctx context.Context, client asyncapi.InferenceClient, store ResultStore, msg pipeline.EmbelishedRequestMessage, url string, headers map[string]string, payload []byte) (*asyncapi.InferenceResponse, *asyncapi.ResultMessage, error) {
	if store == nil {
		resp, err := client.SendRequest(ctx, url, headers, payload)
		return resp, nil, err //nolint:wrapcheck // the worker classifies the client's ClientError
	}
	var stored *asyncapi.ResultMessage
	sink := func(statusCode int, contentType string, body io.Reader) (bool, error) {
		if returnedInline(contentType) {
			return false, nil
		}
		result, err := storeBody(ctx, store, msg, statusCode, contentType, body)
		if err != nil {
			return false, err
		}
		stored = &result
		return true, nil
	}
	if hc, ok := client.(*HTTPInferenceClient); ok {
		resp, err := hc.sendRequest(ctx, url, headers, payload, sink)
		return resp, stored, err
	}
	resp, err := client.SendRequest(ctx, url, headers, payload)
	if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp, nil, err //nolint:wrapcheck // the worker classifies the client's ClientError
	}
	if _, serr := sink(resp.StatusCode, resp.ContentType, bytes.NewReader(resp.Body)); serr != nil {
		return resp, nil, &asyncapi.ClientError{ErrorCategory: asyncapi.ErrCategoryServer, Message: "failed to store response body", RawError: serr, StatusCode: resp.StatusCode}
	}
	return resp, stored, nil
}
