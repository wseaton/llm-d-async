package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testBucketOnce sync.Once
	testBucket     string
	testBucketErr  error
)

// newTestStore returns a Store on the S3 endpoint in TEST_S3_ENDPOINT (any S3-compatible
// server, e.g. SeaweedFS `weed server -s3`), skipping when it is unset. Tests share one bucket
// created per run and each gets its own key prefix.
func newTestStore(t *testing.T, prefix string) *Store {
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
	testBucketOnce.Do(func() {
		testBucket = fmt.Sprintf("results-%d", time.Now().UnixNano())
		s, err := New(ctx, Config{Endpoint: endpoint, Bucket: testBucket, PathStyle: true})
		if err == nil {
			_, err = s.client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(testBucket)})
		}
		testBucketErr = err
	})
	require.NoError(t, testBucketErr)
	s, err := New(ctx, Config{Endpoint: endpoint, Bucket: testBucket, Prefix: path.Join(t.Name(), prefix), PathStyle: true})
	require.NoError(t, err)
	return s
}

// getObject reads back an object the store wrote, with its stored Content-Type.
func (s *Store) getObject(t *testing.T, objectKey string) ([]byte, string) {
	t.Helper()
	out, err := s.client.GetObject(context.Background(), &awss3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(objectKey)})
	require.NoError(t, err)
	defer func() { _ = out.Body.Close() }()
	body, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	return body, aws.ToString(out.ContentType)
}

func TestPutStoresTheBodyUnderThePrefix(t *testing.T) {
	s := newTestStore(t, "llm-d-async/results")
	body := bytes.Repeat([]byte{0xff, 0xfb, 0x90, 0x00, 0x80}, 50_000)

	ref, err := s.Put(context.Background(), "req-1/tok-a", "audio/mpeg", body)
	require.NoError(t, err)

	assert.Equal(t, "s3://"+s.bucket+"/"+s.prefix+"/req-1/tok-a", ref)
	assert.Contains(t, s.prefix, "llm-d-async/results")
	got, contentType := s.getObject(t, s.prefix+"/req-1/tok-a")
	assert.Equal(t, body, got, "binary body must survive byte for byte")
	assert.Equal(t, "audio/mpeg", contentType)
}

func TestPutWithoutAPrefix(t *testing.T) {
	s := newTestStore(t, "")
	s.prefix = ""
	key := t.Name() + "-req-2"
	ref, err := s.Put(context.Background(), key, "audio/wav", []byte("RIFF"))
	require.NoError(t, err)
	assert.Equal(t, "s3://"+s.bucket+"/"+key, ref)
	got, _ := s.getObject(t, key)
	assert.Equal(t, []byte("RIFF"), got)
}

func TestPutFailsOnAMissingBucket(t *testing.T) {
	s := newTestStore(t, "")
	s.bucket = "no-such-bucket-" + fmt.Sprint(time.Now().UnixNano())
	_, err := s.Put(context.Background(), "req-3", "audio/wav", []byte("RIFF"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "s3://"+s.bucket+"/"+s.prefix+"/req-3")
}

func TestNewRequiresABucket(t *testing.T) {
	_, err := New(context.Background(), Config{Endpoint: "http://127.0.0.1:1"})
	assert.Error(t, err)
}
