package s3fastls

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"
)

type Field string
type OutputFormat string
type OutputFormatter func([]string) string

const (
	// bufferSize is the size of channel buffer per worker
	bufferSize = 1024

	FieldKey          Field = "Key"
	FieldSize         Field = "Size"
	FieldLastModified Field = "LastModified"
	FieldETag         Field = "ETag"
	FieldStorageClass Field = "StorageClass"

	OutputTSV OutputFormat = "tsv"
)

var formatters = map[OutputFormat]OutputFormatter{
	OutputTSV: func(fields []string) string { return strings.Join(fields, "\t") },
}

type s3FastLS struct {
	ctx                 context.Context
	client              s3.ListObjectsV2APIClient
	bucket              string
	prefix              string
	outputFields        []Field
	outputFormat        OutputFormat
	outputWriter        io.Writer
	formatter           OutputFormatter
	debug               bool
	listPrefixWorkers   int
	processPagesWorkers int
	pageContentCh       chan []types.Object
	outputWriterCh      chan [][]string
	listPrefixSem       chan struct{}
	listPrefixEg        *errgroup.Group
	processPagesEg      *errgroup.Group
	writeOutputEg       *errgroup.Group
}

type RetryConfig struct {
	MaxAttempts int
	MaxBackoff  time.Duration
	MinBackoff  time.Duration
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts: 10,
		MaxBackoff:  30 * time.Second,
		MinBackoff:  1 * time.Second,
	}
}

func MakeS3Client(cfg aws.Config, endpoint string, retryConfig RetryConfig) *s3.Client {
	customRetryer := retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = retryConfig.MaxAttempts
		o.MaxBackoff = retryConfig.MaxBackoff
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

func (s *s3FastLS) processPages() error {
	ctx := s.ctx
	for objs := range s.pageContentCh {
		if err := ctx.Err(); err != nil {
			return err
		}
		pageOutFields := make([][]string, len(objs))
		for i, obj := range objs {
			outFields := make([]string, len(s.outputFields))
			for j, field := range s.outputFields {
				switch field {
				case FieldKey:
					outFields[j] = aws.ToString(obj.Key)
				case FieldSize:
					outFields[j] = fmt.Sprintf("%d", aws.ToInt64(obj.Size))
				case FieldLastModified:
					outFields[j] = aws.ToTime(obj.LastModified).Format(time.RFC3339)
				case FieldETag:
					outFields[j] = aws.ToString(obj.ETag)
				case FieldStorageClass:
					outFields[j] = string(obj.StorageClass)
				}
			}
			pageOutFields[i] = outFields
		}
		s.outputWriterCh <- pageOutFields
	}
	return nil
}

func (s *s3FastLS) writeOutput() error {
	ctx := s.ctx
	for pageOutFields := range s.outputWriterCh {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, outFields := range pageOutFields {
			if _, err := fmt.Fprintln(s.outputWriter, s.formatter(outFields)); err != nil {
				return fmt.Errorf("failed to write output: %w", err)
			}
		}
	}
	return nil
}

func (s *s3FastLS) listPrefix(prefix string) error {
	ctx := s.ctx
	eg := s.listPrefixEg
	if err := ctx.Err(); err != nil {
		return err
	}
	s.listPrefixSem <- struct{}{}
	defer func() { <-s.listPrefixSem }()
	if s.debug {
		log.Printf("Listing prefix: %q", prefix)
	}
	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}
	paginator := s3.NewListObjectsV2Paginator(s.client, params)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list objects for prefix %q: %w", prefix, err)
		}
		if len(page.Contents) > 0 {
			s.pageContentCh <- page.Contents
		}
		for _, cp := range page.CommonPrefixes {
			eg.Go(func() error { return s.listPrefix(*cp.Prefix) })
		}
	}
	return nil
}

func (s *s3FastLS) list() error {
	ctx := s.ctx
	errCh := make(chan error, 3)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(s.pageContentCh)
		s.listPrefixEg, _ = errgroup.WithContext(ctx)
		s.listPrefixEg.Go(func() error { return s.listPrefix(s.prefix) })
		if err := s.listPrefixEg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("s3fastls.listPrefix: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(s.outputWriterCh)
		s.writeOutputEg, _ = errgroup.WithContext(ctx)
		s.writeOutputEg.Go(s.writeOutput)
		if err := s.writeOutputEg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("s3fastls.writeOutput: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.processPagesEg, _ = errgroup.WithContext(ctx)
		for i := 0; i < s.processPagesWorkers; i++ {
			s.processPagesEg.Go(s.processPages)
		}
		if err := s.processPagesEg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("s3fastls.processPages: %w", err)
		}
	}()

	wg.Wait()
	close(errCh)

	var errs error
	for err := range errCh {
		errs = errors.Join(errs, err)
	}
	return errs
}

type S3FastLSParams struct {
	Bucket       string
	Prefix       string
	OutputFields []Field
	OutputFormat OutputFormat
	Workers      int
	Debug        bool
}

// List lists S3 objects with the given parameters and writes output to writer.
func List(ctx context.Context, params S3FastLSParams, client s3.ListObjectsV2APIClient, writer io.Writer) error {
	if writer == nil {
		return fmt.Errorf("output writer must not be nil")
	}

	processPagesWorkers := min(params.Workers, runtime.NumCPU())

	s3ls := &s3FastLS{
		ctx:                 ctx,
		client:              client,
		bucket:              params.Bucket,
		prefix:              params.Prefix,
		outputFields:        params.OutputFields,
		outputFormat:        params.OutputFormat,
		outputWriter:        writer,
		formatter:           formatters[params.OutputFormat],
		debug:               params.Debug,
		listPrefixWorkers:   params.Workers,
		processPagesWorkers: processPagesWorkers,
		pageContentCh:       make(chan []types.Object, params.Workers*bufferSize),
		outputWriterCh:      make(chan [][]string, processPagesWorkers*bufferSize),
		listPrefixSem:       make(chan struct{}, params.Workers),
	}

	return s3ls.list()
}
