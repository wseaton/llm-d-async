// Package s3 stores response bodies in any S3-compatible object store.
package s3

import (
	"bytes"
	"context"
	"fmt"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

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
	client *awss3.Client
	bucket string
	prefix string
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
	return &Store{client: client, bucket: cfg.Bucket, prefix: cfg.Prefix}, nil
}

// Put writes body under the store's prefix and returns its s3:// reference.
func (s *Store) Put(ctx context.Context, key, contentType string, body []byte) (string, error) {
	objectKey := path.Join(s.prefix, key)
	in := &awss3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(objectKey),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	}
	if contentType != "" {
		in.ContentType = aws.String(contentType)
	}
	if _, err := s.client.PutObject(ctx, in); err != nil {
		return "", fmt.Errorf("put s3://%s/%s: %w", s.bucket, objectKey, err)
	}
	return "s3://" + s.bucket + "/" + objectKey, nil
}
