package s3fastls

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"
)

type Field string
type Formatter func([]string) string

const (
	// bufferSize is the size of channel buffer per worker
	bufferSize = 1024

	FieldKey          Field = "Key"
	FieldSize         Field = "Size"
	FieldLastModified Field = "LastModified"
	FieldETag         Field = "ETag"
	FieldStorageClass Field = "StorageClass"
)

// FormatTSV is a predefined formatter for TSV output.
func FormatTSV(fields []string) string {
	return strings.Join(fields, "\t")
}

type S3ListObjectsV2API interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type Stats struct {
	Objects  int64
	Prefixes int64
	Pages    int64
}

type s3FastLS struct {
	ctx            context.Context
	cancel         context.CancelFunc
	client         S3ListObjectsV2API
	bucket         string
	prefix         string
	fields         []Field
	writer         io.Writer
	formatter      Formatter
	listWorkers    int
	processWorkers int
	objsCh         chan []types.Object
	recordsCh      chan [][]string
	sem            chan struct{}
	listEg         *errgroup.Group
	stats          struct {
		objects  atomic.Int64
		prefixes atomic.Int64
		pages    atomic.Int64
	}
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

func (s *s3FastLS) list(prefix string) error {
	eg := s.listEg
	s.sem <- struct{}{}
	defer func() { <-s.sem }()

	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}
	s.stats.prefixes.Add(1)
	paginator := s3.NewListObjectsV2Paginator(s.client, params)
	for paginator.HasMorePages() {
		s.stats.pages.Add(1)
		if s.ctx.Err() != nil {
			return s.ctx.Err()
		}
		page, err := paginator.NextPage(s.ctx)
		if err != nil {
			s.cancel()
			return fmt.Errorf("failed to list objects for prefix %q: %w", prefix, err)
		}
		if len(page.Contents) > 0 {
			s.stats.objects.Add(int64(len(page.Contents)))
			select {
			case s.objsCh <- page.Contents:
			case <-s.ctx.Done():
				return s.ctx.Err()
			}
		}
		for _, cp := range page.CommonPrefixes {
			if s.ctx.Err() != nil {
				return s.ctx.Err()
			}
			eg.Go(func() error { return s.list(*cp.Prefix) })
		}
	}
	return nil
}

func (s *s3FastLS) process() error {
	for objs := range s.objsCh {
		records := make([][]string, len(objs))
		for i, obj := range objs {
			record := make([]string, len(s.fields))
			for j, field := range s.fields {
				switch field {
				case FieldKey:
					record[j] = aws.ToString(obj.Key)
				case FieldSize:
					record[j] = fmt.Sprintf("%d", aws.ToInt64(obj.Size))
				case FieldLastModified:
					record[j] = aws.ToTime(obj.LastModified).Format(time.RFC3339)
				case FieldETag:
					record[j] = aws.ToString(obj.ETag)
				case FieldStorageClass:
					record[j] = string(obj.StorageClass)
				}
			}
			records[i] = record
		}
		select {
		case s.recordsCh <- records:
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
	return nil
}

func (s *s3FastLS) write() error {
	for records := range s.recordsCh {
		for _, record := range records {
			if s.ctx.Err() != nil {
				return s.ctx.Err()
			}
			if _, err := fmt.Fprintln(s.writer, s.formatter(record)); err != nil {
				s.cancel()
				return fmt.Errorf("failed to write output: %w", err)
			}
		}
	}
	return nil
}

func (s *s3FastLS) statsResult() Stats {
	return Stats{
		Objects:  s.stats.objects.Load(),
		Prefixes: s.stats.prefixes.Load(),
		Pages:    s.stats.pages.Load(),
	}
}

func (s *s3FastLS) run() error {
	s.ctx, s.cancel = context.WithCancel(s.ctx)
	errCh := make(chan error, 3)

	s.listEg = &errgroup.Group{}
	s.listEg.Go(func() error {
		return s.list(s.prefix)
	})

	processEg := &errgroup.Group{}
	for range s.processWorkers {
		processEg.Go(s.process)
	}

	writeEg := &errgroup.Group{}
	writeEg.Go(s.write)

	errCh <- s.listEg.Wait()
	close(s.objsCh)
	errCh <- processEg.Wait()
	close(s.recordsCh)
	errCh <- writeEg.Wait()

	close(errCh)

	var errs []error
	var ctxErr error
	for err := range errCh {
		if err == nil {
			continue
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			if ctxErr == nil {
				ctxErr = err
			}
			continue
		}
		errs = append(errs, err)
	}
	switch {
	case len(errs) > 0:
		return errors.Join(errs...)
	case ctxErr != nil:
		return ctxErr
	default:
		return nil
	}
}

type S3FastLSParams struct {
	Bucket       string
	Prefix       string
	OutputFields []Field
	Formatter    Formatter
	Workers      int
}

func List(
	ctx context.Context,
	params S3FastLSParams,
	client S3ListObjectsV2API,
	writer io.Writer,
) (Stats, error) {
	processPagesWorkers := min(params.Workers, runtime.NumCPU())

	s3ls := &s3FastLS{
		ctx:            ctx,
		client:         client,
		bucket:         params.Bucket,
		prefix:         params.Prefix,
		formatter:      params.Formatter,
		fields:         params.OutputFields,
		writer:         writer,
		listWorkers:    params.Workers,
		processWorkers: processPagesWorkers,
		objsCh:         make(chan []types.Object, params.Workers*bufferSize),
		recordsCh:      make(chan [][]string, processPagesWorkers*bufferSize),
		sem:            make(chan struct{}, params.Workers),
	}

	err := s3ls.run()
	return s3ls.statsResult(), err
}
