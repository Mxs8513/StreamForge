// Package storage wraps S3-compatible object storage (MinIO locally, S3 in
// cloud) and Parquet encoding. The rest of the engine only sees Put/Get/List
// on opaque object keys.
package storage

import (
	"bytes"
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config holds connection parameters for the object store.
type Config struct {
	Endpoint  string // e.g. http://localhost:9000 ; empty => real AWS S3
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
}

// Client is a thin S3 wrapper with MinIO-friendly defaults (path-style).
type Client struct {
	s3     *s3.Client
	bucket string
}

// New builds an S3 client. When Endpoint is set it targets a MinIO/S3-compatible
// server with path-style addressing; otherwise it falls back to standard AWS
// resolution.
func New(ctx context.Context, cfg Config) (*Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	if cfg.AccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	s3c := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true // MinIO requires path-style
		}
	})
	return &Client{s3: s3c, bucket: cfg.Bucket}, nil
}

// EnsureBucket creates the bucket if it does not already exist.
func (c *Client) EnsureBucket(ctx context.Context) error {
	_, err := c.s3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(c.bucket)})
	if err == nil {
		return nil
	}
	_, err = c.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(c.bucket)})
	return err
}

// Put writes bytes to an object key.
func (c *Client) Put(ctx context.Context, key string, data []byte) error {
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

// Get reads an object's bytes.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// List returns object keys under a prefix.
func (c *Client) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	p := s3.NewListObjectsV2Paginator(c.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	return keys, nil
}
