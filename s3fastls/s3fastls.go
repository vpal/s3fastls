package s3fastls

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// --- Types for fields and output formats ---
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

// OutputFormatter is a function that formats a slice of strings for output
type OutputFormatter func([]string) string

// Format functions for different output formats
var formatters = map[OutputFormat]OutputFormatter{
	OutputTSV: func(fields []string) string { return strings.Join(fields, "\t") },
}

// --- S3FastLS struct and methods ---
type S3FastLS struct {
	client       *s3.Client
	bucket       string
	fields       []Field
	outputFormat OutputFormat
	formatter    OutputFormatter
	debug        bool
	sem          chan struct{}
}

func NewS3FastLS(client *s3.Client, bucket string, fields []Field, outputFormat OutputFormat, debug bool, threads int) *S3FastLS {
	formatter, ok := formatters[outputFormat]
	if !ok {
		log.Fatalf("unsupported output format: %s", outputFormat)
	}
	sem := make(chan struct{}, threads)
	return &S3FastLS{
		client:       client,
		bucket:       bucket,
		fields:       fields,
		outputFormat: outputFormat,
		formatter:    formatter,
		debug:        debug,
		sem:          sem,
	}
}

func MakeS3Client(cfg aws.Config, endpoint string) *s3.Client {
	customRetryer := retry.AddWithMaxAttempts(retry.NewStandard(), 10)
	if endpoint != "" {
		return s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.Region = cfg.Region
			o.Credentials = cfg.Credentials
			o.Retryer = customRetryer
		})
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = cfg.Region
		o.Credentials = cfg.Credentials
		o.Retryer = customRetryer
	})
}

func (s *S3FastLS) processPages(pageContentCh <-chan []types.Object, outputCh chan<- []string, procWg *sync.WaitGroup) {
	defer procWg.Done()
	for objs := range pageContentCh {
		for _, obj := range objs {
			var outFields []string
			for _, field := range s.fields {
				switch field {
				case FieldKey:
					outFields = append(outFields, aws.ToString(obj.Key))
				case FieldSize:
					outFields = append(outFields, fmt.Sprintf("%d", obj.Size))
				case FieldLastModified:
					if obj.LastModified != nil {
						outFields = append(outFields, obj.LastModified.Format(time.RFC3339))
					} else {
						outFields = append(outFields, "")
					}
				case FieldETag:
					outFields = append(outFields, aws.ToString(obj.ETag))
				case FieldStorageClass:
					outFields = append(outFields, string(obj.StorageClass))
				}
			}
			outputCh <- outFields
		}
	}
}

func outputWriter(outputFile string) (io.Writer, func(), error) {
	if outputFile == "" {
		return os.Stdout, func() {}, nil
	}
	f, err := os.Create(outputFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create output file: %v", err)
	}
	return f, func() { f.Close() }, nil
}

func (s *S3FastLS) writeOutput(outputCh <-chan []string, writeWg *sync.WaitGroup, outputFile string) error {
	defer writeWg.Done()

	w, cleanup, err := outputWriter(outputFile)
	if err != nil {
		return err
	}
	defer cleanup()

	for outFields := range outputCh {
		if _, err := fmt.Fprintln(w, s.formatter(outFields)); err != nil {
			return fmt.Errorf("failed to write output: %v", err)
		}
	}
	return nil
}

func (s *S3FastLS) listPrefix(prefix string, pageContentCh chan<- []types.Object, listWg *sync.WaitGroup) {
	defer listWg.Done()
	s.sem <- struct{}{}        // acquire
	defer func() { <-s.sem }() // release

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
			log.Printf("Failed to list objects for prefix %q: %v", prefix, err)
			return
		}
		if len(page.Contents) > 0 {
			pageContentCh <- page.Contents
		}
		for _, cp := range page.CommonPrefixes {
			pfx := *cp.Prefix
			listWg.Add(1)
			go s.listPrefix(pfx, pageContentCh, listWg)
		}
	}
}

func (s *S3FastLS) Run(prefix string, threadCount int, outputFile string) error {
	pageContentCh := make(chan []types.Object, 4096)
	outputCh := make(chan []string, 4096)
	errCh := make(chan error, 1)

	var listPrefixWg sync.WaitGroup
	var processPagesWg sync.WaitGroup
	var writeOutputWg sync.WaitGroup

	for i := 0; i < 64; i++ {
		processPagesWg.Add(1)
		go s.processPages(pageContentCh, outputCh, &processPagesWg)
	}

	writeOutputWg.Add(1)
	go func() {
		if err := s.writeOutput(outputCh, &writeOutputWg, outputFile); err != nil {
			errCh <- err
		}
	}()

	listPrefixWg.Add(1)
	go s.listPrefix(prefix, pageContentCh, &listPrefixWg)

	listPrefixWg.Wait()
	close(pageContentCh)
	processPagesWg.Wait()
	close(outputCh)
	writeOutputWg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}
