package s3fastls

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Field string

const (
	FieldKey          Field = "Key"
	FieldSize         Field = "Size"
	FieldLastModified Field = "LastModified"
	FieldETag         Field = "ETag"
	FieldStorageClass Field = "StorageClass"
)

type OutputFormat string

const (
	OutputTSV OutputFormat = "tsv"
)

type OutputFormatter func([]string) string

var formatters = map[OutputFormat]OutputFormatter{
	OutputTSV: func(fields []string) string { return strings.Join(fields, "\t") },
}

type s3FastLS struct {
	client              *s3.Client
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
	listPrefixWg        *sync.WaitGroup
	processPagesWg      *sync.WaitGroup
	writeOutputWg       *sync.WaitGroup
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

func (s *s3FastLS) processPages() {
	defer s.processPagesWg.Done()
	for objs := range s.pageContentCh {
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
}

func (s *s3FastLS) writeOutput() {
	defer s.writeOutputWg.Done()
	for pageOutFields := range s.outputWriterCh {
		for _, outFields := range pageOutFields {
			if _, err := fmt.Fprintln(s.outputWriter, s.formatter(outFields)); err != nil {
				log.Fatalf("failed to write output: %v", err)
			}
		}
	}
}

func (s *s3FastLS) listPrefix(prefix string) {
	defer s.listPrefixWg.Done()
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
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Printf("failed to list objects for prefix %q: %v", prefix, err)
			return
		}
		if len(page.Contents) > 0 {
			s.pageContentCh <- page.Contents
		}
		for _, cp := range page.CommonPrefixes {
			s.listPrefixWg.Add(1)
			go s.listPrefix(*cp.Prefix)
		}
	}
}

func (s *s3FastLS) list() {
	s.writeOutputWg.Add(1)
	go s.writeOutput()

	for i := 0; i < s.processPagesWorkers; i++ {
		s.processPagesWg.Add(1)
		go s.processPages()
	}

	s.listPrefixWg.Add(1)
	go s.listPrefix(s.prefix)

	s.listPrefixWg.Wait()
	close(s.pageContentCh)
	s.processPagesWg.Wait()
	close(s.outputWriterCh)
	s.writeOutputWg.Wait()
}

type S3FastLSParams struct {
	Bucket       string
	Prefix       string
	OutputFields []Field
	OutputFormat OutputFormat
	OutputFile   string
	Workers      int
	Debug        bool
}

func List(client *s3.Client, params S3FastLSParams) {
	var writer io.Writer
	if params.OutputFile == "" {
		writer = os.Stdout
	} else {
		file, err := os.Create(params.OutputFile)
		if err != nil {
			log.Fatalf("failed to create output file: %v", err)
		}
		writer = file
		defer file.Close()
	}

	processPagesWorkers := min(params.Workers, runtime.NumCPU())

	s3ls := &s3FastLS{
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
		pageContentCh:       make(chan []types.Object, 4096),
		outputWriterCh:      make(chan [][]string, 4096),
		listPrefixSem:       make(chan struct{}, params.Workers),
		listPrefixWg:        &sync.WaitGroup{},
		processPagesWg:      &sync.WaitGroup{},
		writeOutputWg:       &sync.WaitGroup{},
	}

	s3ls.list()
}
