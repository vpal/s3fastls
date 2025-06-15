package s3fastls

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func MakeTestS3Client(cfg aws.Config, endpoint string) *s3.Client {
	customRetryer := retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = DefaultRetryConfig().MaxAttempts
		o.MaxBackoff = DefaultRetryConfig().MaxBackoff
	})

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = cfg.Region
		o.Credentials = cfg.Credentials
		o.Retryer = customRetryer
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
}

func TestList_EndToEnd(t *testing.T) {
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bucket := "test-bucket"
	backend.CreateBucket(bucket)

	testObjects := []struct {
		key     string
		content string
	}{
		{"file1.txt", "content1"},
		{"file2.txt", "content2"},
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		t.Fatal(err)
	}

	client := MakeTestS3Client(cfg, ts.URL)

	for _, obj := range testObjects {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(obj.key),
			Body:   strings.NewReader(obj.content),
		})
		if err != nil {
			t.Fatalf("failed to put object %s: %v", obj.key, err)
		}
	}

	listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("failed to list objects to verify upload: %v", err)
	}
	for _, obj := range listOut.Contents {
		_ = obj.Key // just to avoid unused variable warning if you want to keep this loop
	}

	var buf bytes.Buffer
	params := S3FastLSParams{
		Bucket:       bucket,
		Prefix:       "",
		OutputFields: []Field{FieldKey},
		OutputFormat: OutputTSV,
		Workers:      1,
		Debug:        false,
	}

	if err := List(ctx, params, client, &buf); err != nil {
		t.Fatalf("listing failed: %v", err)
	}

	output := buf.String()
	for _, obj := range testObjects {
		if !strings.Contains(output, obj.key) {
			t.Errorf("output missing object %s", obj.key)
		}
	}
}

func TestList_WriterError(t *testing.T) {
	ctx := context.Background()
	client := &slowPagingMockS3Client{}
	params := S3FastLSParams{
		Bucket:       "mock-bucket",
		Prefix:       "",
		OutputFields: []Field{FieldKey, FieldSize},
		OutputFormat: OutputTSV,
		Workers:      1,
		Debug:        false,
	}
	w := &errorWriter{}
	_ = time.Now()
	err := List(ctx, params, client, w)
	if err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Errorf("expected disk full error, got %v", err)
	}
}

func TestList_PagingError(t *testing.T) {
	ctx := context.Background()
	failOnPage := 3
	client := &errorPagingMockS3Client{failPage: failOnPage}
	params := S3FastLSParams{
		Bucket:       "mock-bucket",
		Prefix:       "",
		OutputFields: []Field{FieldKey, FieldSize},
		OutputFormat: OutputTSV,
		Workers:      1,
		Debug:        false,
	}
	var buf bytes.Buffer
	err := List(ctx, params, client, &buf)
	if err == nil || !strings.Contains(err.Error(), "simulated paging error") {
		t.Errorf("expected paging error, got %v", err)
	}
}

func TestList_ContextTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client := &slowPagingMockS3Client{}
	params := S3FastLSParams{
		Bucket:       "mock-bucket",
		Prefix:       "",
		OutputFields: []Field{FieldKey, FieldSize},
		OutputFormat: OutputTSV,
		Workers:      1,
		Debug:        false,
	}
	var buf bytes.Buffer
	err := List(ctx, params, client, &buf)
	if err == nil || !(strings.Contains(err.Error(), "context deadline exceeded") || strings.Contains(err.Error(), "canceled")) {
		t.Errorf("expected context cancellation error, got %v", err)
	}
}

func TestList_ContextExplicitCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &slowPagingMockS3Client{}
	params := S3FastLSParams{
		Bucket:       "mock-bucket",
		Prefix:       "",
		OutputFields: []Field{FieldKey, FieldSize},
		OutputFormat: OutputTSV,
		Workers:      1,
		Debug:        false,
	}
	var buf bytes.Buffer
	go func() {
		time.Sleep(2 * time.Second)
		cancel()
	}()
	err := List(ctx, params, client, &buf)
	if err == nil || !(strings.Contains(err.Error(), "context canceled") || strings.Contains(err.Error(), "canceled")) {
		t.Errorf("expected context cancellation error, got %v", err)
	}
}

// Mock S3ListObjectsV2API for testing

type mockS3Client struct{}

func (m *mockS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{
		Contents: []types.Object{
			{
				Key:  aws.String("mockfile.txt"),
				Size: aws.Int64(123),
			},
		},
	}, nil
}

type errorWriter struct {
	wroteOnce bool
}

func (e *errorWriter) Write(p []byte) (int, error) {
	if e.wroteOnce {
		return 0, fmt.Errorf("simulated disk full")
	}
	e.wroteOnce = true
	return len(p), nil
}

type slowPagingMockS3Client struct {
	call int
}

func (m *slowPagingMockS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.call++
	time.Sleep(time.Duration(m.call) * time.Second)
	return &s3.ListObjectsV2Output{
		Contents:              []types.Object{{Key: aws.String(fmt.Sprintf("file%d.txt", m.call)), Size: aws.Int64(int64(m.call * 100))}},
		IsTruncated:           aws.Bool(true),
		NextContinuationToken: aws.String(fmt.Sprintf("token%d", m.call+1)),
	}, nil
}

type errorPagingMockS3Client struct {
	call     int
	failPage int
}

func (m *errorPagingMockS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.call++
	if m.call == m.failPage {
		return nil, fmt.Errorf("simulated paging error on page %d", m.call)
	}
	return &s3.ListObjectsV2Output{
		Contents:              []types.Object{{Key: aws.String(fmt.Sprintf("file%d.txt", m.call)), Size: aws.Int64(int64(m.call * 100))}},
		IsTruncated:           aws.Bool(true),
		NextContinuationToken: aws.String(fmt.Sprintf("token%d", m.call+1)),
	}, nil
}
