// Package s3 stores response bodies in any S3-compatible object store.
package s3

import (
	"context"
	"fmt"
	"io"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// partSize is the S3 minimum; with one part in flight an upload holds at most this much of a
// body in memory, however long the body is.
const partSize = 5 << 20

// Config selects the bucket and endpoint. Credentials come from the standard AWS
// environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, or a profile).
type Config struct {
	Endpoint  string
	Region    string
	Bucket    string
	Prefix    string
	PathStyle bool
}

// Store writes each body to <prefix>/<key> in one bucket.
type Store struct {
	client   *awss3.Client
	uploader *manager.Uploader //nolint:staticcheck // transfermanager is not yet 1.0
	bucket   string
	prefix   string
}

// New builds a Store for cfg.
func New(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 result store: bucket is required")
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("s3 result store: load AWS config: %w", err)
	}
	client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.PathStyle
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	uploader := manager.NewUploader(client, func(u *manager.Uploader) { //nolint:staticcheck // transfermanager is not yet 1.0
		u.PartSize = partSize
		u.Concurrency = 1
	})
	return &Store{client: client, uploader: uploader, bucket: cfg.Bucket, prefix: cfg.Prefix}, nil
}

// Put streams body under the store's prefix and returns its s3:// reference. Bodies up to one
// part go up in a single PutObject, longer ones as a multipart upload one part at a time.
func (s *Store) Put(ctx context.Context, key, contentType string, body io.Reader) (string, error) {
	objectKey := path.Join(s.prefix, key)
	in := &awss3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey),
		Body:   body,
	}
	if contentType != "" {
		in.ContentType = aws.String(contentType)
	}
	if _, err := s.uploader.Upload(ctx, in); err != nil { //nolint:staticcheck // transfermanager is not yet 1.0
		return "", fmt.Errorf("put s3://%s/%s: %w", s.bucket, objectKey, err)
	}
	return "s3://" + s.bucket + "/" + objectKey, nil
}
