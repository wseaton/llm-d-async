package asyncworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"mime"
	"net/url"
	"strings"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
)

// ResultStore holds response bodies that are returned by reference instead of inline.
type ResultStore interface {
	// Put stores body under key and returns a reference to it (for example s3://bucket/key).
	Put(ctx context.Context, key, contentType string, body []byte) (string, error)
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

// storeResult puts resp's body in store and builds the reference result for it.
func storeResult(ctx context.Context, store ResultStore, msg pipeline.EmbelishedRequestMessage, resp *asyncapi.InferenceResponse) (asyncapi.ResultMessage, error) {
	sum := sha256.Sum256(resp.Body)
	ref, err := store.Put(ctx, resultObjectKey(msg), resp.ContentType, resp.Body)
	if err != nil {
		return asyncapi.ResultMessage{}, fmt.Errorf("store response body: %w", err)
	}
	return asyncapi.NewHTTPRefResult(msg.PublicRequest, msg.InternalRouting, resp.StatusCode, ref, resp.ContentType, int64(len(resp.Body)), hex.EncodeToString(sum[:])), nil
}
