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
		Formatter:    FormatTSV,
		Workers:      1,
	}

	stats, err := List(ctx, params, client, &buf)
	if err != nil {
		t.Fatalf("listing failed: %v", err)
	}

	output := buf.String()
	for _, obj := range testObjects {
		if !strings.Contains(output, obj.key) {
			t.Errorf("output missing object %s", obj.key)
		}
	}
	if stats.Objects != int64(len(testObjects)) {
		t.Errorf("expected %d objects, got %d", len(testObjects), stats.Objects)
	}
	if stats.Prefixes != 1 {
		t.Errorf("expected 1 prefix, got %d", stats.Prefixes)
	}
	if stats.Pages < 1 {
		t.Errorf("expected at least 1 page, got %d", stats.Pages)
	}
}

func TestList_Basic(t *testing.T) {
	testObjects := []string{"file1.txt", "file2.txt"}
	client := &flexibleMockS3Client{objects: testObjects}
	params := S3FastLSParams{
		Bucket:       "mock-bucket",
		Prefix:       "",
		OutputFields: []Field{FieldKey, FieldSize},
		Formatter:    FormatTSV,
		Workers:      1,
	}
	var buf bytes.Buffer
	stats, err := List(context.Background(), params, client, &buf)
	if err != nil {
		t.Fatalf("listing failed: %v", err)
	}
	output := buf.String()
	for _, obj := range testObjects {
		if !strings.Contains(output, obj) {
			t.Errorf("output missing object %s", obj)
		}
	}
	if stats.Objects != int64(len(testObjects)) {
		t.Errorf("expected %d objects, got %d", len(testObjects), stats.Objects)
	}
	if stats.Prefixes != 1 {
		t.Errorf("expected 1 prefix, got %d", stats.Prefixes)
	}
	if stats.Pages < 1 {
		t.Errorf("expected at least 1 page, got %d", stats.Pages)
	}
}

func TestList_WriterError(t *testing.T) {
	ctx := context.Background()
	client := &slowPagingMockS3Client{}
	params := S3FastLSParams{
		Bucket:       "mock-bucket",
		Prefix:       "",
		OutputFields: []Field{FieldKey, FieldSize},
		Formatter:    FormatTSV,
		Workers:      1,
	}
	w := &errorWriter{}
	_, err := List(ctx, params, client, w)
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
		Formatter:    FormatTSV,
		Workers:      1,
	}
	var buf bytes.Buffer
	_, err := List(ctx, params, client, &buf)
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
		Formatter:    FormatTSV,
		Workers:      1,
	}
	var buf bytes.Buffer
	_, err := List(ctx, params, client, &buf)
	if err == nil || (!strings.Contains(err.Error(), "context deadline exceeded") && !strings.Contains(err.Error(), "canceled")) {
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
		Formatter:    FormatTSV,
		Workers:      1,
	}
	var buf bytes.Buffer
	go func() {
		time.Sleep(2 * time.Second)
		cancel()
	}()
	_, err := List(ctx, params, client, &buf)
	if err == nil || (!strings.Contains(err.Error(), "context canceled") && !strings.Contains(err.Error(), "canceled")) {
		t.Errorf("expected context cancellation error, got %v", err)
	}
}

func TestList_Prefixes(t *testing.T) {
	// Objects: 2 in root, 3 in a/b, 2 in a/c, 2 in d, 2 in e/f, 4 in g
	// Prefixes: "" (root), "a/", "a/b/", "a/c/", "d/", "e/", "e/f/", "g/" => 8
	objects := []string{
		"file1.txt", "file2.txt", // root
		"a/b/file3.txt", "a/b/file4.txt", "a/b/file5.txt",
		"a/c/file6.txt", "a/c/file7.txt",
		"d/file8.txt", "d/file9.txt",
		"e/f/file10.txt", "e/f/file11.txt",
		"g/file12.txt", "g/file13.txt", "g/file14.txt", "g/file15.txt",
	}
	client := &flexibleMockS3Client{objects: objects}
	params := S3FastLSParams{
		Bucket:       "mock-bucket",
		Prefix:       "",
		OutputFields: []Field{FieldKey, FieldSize},
		Formatter:    FormatTSV,
		Workers:      2,
	}
	var buf bytes.Buffer
	stats, err := List(context.Background(), params, client, &buf)
	if err != nil {
		t.Fatalf("listing failed: %v", err)
	}
	output := buf.String()
	for _, obj := range objects {
		if !strings.Contains(output, obj) {
			t.Errorf("output missing object %s", obj)
		}
	}
	if stats.Objects != int64(len(objects)) {
		t.Errorf("expected %d objects, got %d", len(objects), stats.Objects)
	}
	// Prefixes: root ("") + a/ + a/b/ + a/c/ + d/ + e/ + e/f/ + g/ = 8
	if stats.Prefixes != 8 {
		t.Errorf("expected 8 prefixes, got %d", stats.Prefixes)
	}
	if stats.Pages < 1 {
		t.Errorf("expected at least 1 page, got %d", stats.Pages)
	}
}

func TestList_PrefixesNoRoot(t *testing.T) {
	// Objects: 3 in a/b, 2 in a/c, 2 in d, 2 in e/f, 4 in g
	// Prefixes: "" (root), "a/", "a/b/", "a/c/", "d/", "e/", "e/f/", "g/" => 8
	objects := []string{
		"a/b/file3.txt", "a/b/file4.txt", "a/b/file5.txt",
		"a/c/file6.txt", "a/c/file7.txt",
		"d/file8.txt", "d/file9.txt",
		"e/f/file10.txt", "e/f/file11.txt",
		"g/file12.txt", "g/file13.txt", "g/file14.txt", "g/file15.txt",
	}
	client := &flexibleMockS3Client{objects: objects}
	params := S3FastLSParams{
		Bucket:       "mock-bucket",
		Prefix:       "",
		OutputFields: []Field{FieldKey, FieldSize},
		Formatter:    FormatTSV,
		Workers:      2,
	}
	var buf bytes.Buffer
	stats, err := List(context.Background(), params, client, &buf)
	if err != nil {
		t.Fatalf("listing failed: %v", err)
	}
	output := buf.String()
	for _, obj := range objects {
		if !strings.Contains(output, obj) {
			t.Errorf("output missing object %s", obj)
		}
	}
	if stats.Objects != int64(len(objects)) {
		t.Errorf("expected %d objects, got %d", len(objects), stats.Objects)
	}
	// Prefixes: root ("") + a/ + a/b/ + a/c/ + d/ + e/ + e/f/ + g/ = 8
	if stats.Prefixes != 8 {
		t.Errorf("expected 8 prefixes, got %d", stats.Prefixes)
	}
	if stats.Pages < 1 {
		t.Errorf("expected at least 1 page, got %d", stats.Pages)
	}
}

// Mock S3ListObjectsV2API for testing

type mockS3Client struct{}

func (m *mockS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	objs := []types.Object{
		{Key: aws.String("file1.txt"), Size: aws.Int64(123)},
		{Key: aws.String("file2.txt"), Size: aws.Int64(456)},
	}
	return &s3.ListObjectsV2Output{
		Contents: objs,
		// CommonPrefixes: []types.CommonPrefix{
		// 	{Prefix: aws.String("someprefix/")},
		// },
		IsTruncated: aws.Bool(false),
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

// flexibleMockS3Client is a mock S3 client that returns objects and common prefixes based on a provided list of keys.
type flexibleMockS3Client struct {
	objects []string
}

func (m *flexibleMockS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	prefix := aws.ToString(params.Prefix)
	delimiter := aws.ToString(params.Delimiter)
	contents := []types.Object{}
	prefixSet := make(map[string]struct{})

	for _, key := range m.objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		rest := strings.TrimPrefix(key, prefix)
		if delimiter != "" {
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				subPrefix := key[:len(prefix)+idx+1]
				prefixSet[subPrefix] = struct{}{}
				continue
			}
		}
		contents = append(contents, types.Object{
			Key:  aws.String(key),
			Size: aws.Int64(123),
		})
	}

	commonPrefixes := []types.CommonPrefix{}
	for p := range prefixSet {
		commonPrefixes = append(commonPrefixes, types.CommonPrefix{Prefix: aws.String(p)})
	}

	return &s3.ListObjectsV2Output{
		Contents:       contents,
		CommonPrefixes: commonPrefixes,
		IsTruncated:    aws.Bool(false),
	}, nil
}
