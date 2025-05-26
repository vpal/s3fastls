package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3FastLS struct {
	client       *s3.Client
	bucket       string
	fields       []string
	outputFormat string
	debug        bool
	sem          chan struct{}
}

func NewS3FastLS(client *s3.Client, bucket string, fields []string, outputFormat string, debug bool, threads int) *S3FastLS {
	sem := make(chan struct{}, threads)
	return &S3FastLS{
		client:       client,
		bucket:       bucket,
		fields:       fields,
		outputFormat: outputFormat,
		debug:        debug,
		sem:          sem,
	}
}

func MakeS3Client(cfg aws.Config, endpoint string) *s3.Client {
	// Fine-tune the AWS SDK retryer for S3
	customRetryer := retry.AddWithMaxAttempts(retry.NewStandard(), 10) // Increase max attempts
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
				case "Key":
					outFields = append(outFields, aws.ToString(obj.Key))
				case "Size":
					outFields = append(outFields, fmt.Sprintf("%d", obj.Size))
				case "LastModified":
					if obj.LastModified != nil {
						outFields = append(outFields, obj.LastModified.Format(time.RFC3339))
					} else {
						outFields = append(outFields, "")
					}
				case "ETag":
					outFields = append(outFields, aws.ToString(obj.ETag))
				case "StorageClass":
					outFields = append(outFields, string(obj.StorageClass))
				}
			}
			outputCh <- outFields
		}
	}
}

func (s *S3FastLS) writeOutput(outputCh <-chan []string, writeWg *sync.WaitGroup) {
	defer writeWg.Done()
	for outFields := range outputCh {
		if s.outputFormat == "tsv" {
			fmt.Println(strings.Join(outFields, "\t"))
		}
	}
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

func (s *S3FastLS) Run(prefix string, numListPrefixThreads int) {
	pageContentCh := make(chan []types.Object, 4096)
	outputCh := make(chan []string, 4096)

	var listPrefixWg sync.WaitGroup
	var processPagesWg sync.WaitGroup
	var writeOutputWg sync.WaitGroup

	// Start processPages goroutines (fixed 64)
	for i := 0; i < 64; i++ {
		processPagesWg.Add(1)
		go s.processPages(pageContentCh, outputCh, &processPagesWg)
	}

	// Start writeOutput goroutine
	writeOutputWg.Add(1)
	go s.writeOutput(outputCh, &writeOutputWg)

	// Start listPrefix goroutine (user-controlled concurrency)
	listPrefixWg.Add(1)
	go s.listPrefix(prefix, pageContentCh, &listPrefixWg)

	listPrefixWg.Wait()
	close(pageContentCh)
	processPagesWg.Wait()
	close(outputCh)
	writeOutputWg.Wait()
}

var allowedFields = []string{"Key", "Size", "LastModified", "ETag", "StorageClass"}

func isAllowedField(field string) bool {
	for _, f := range allowedFields {
		if f == field {
			return true
		}
	}
	return false
}

func validateFields(fields []string) {
	for _, f := range fields {
		if !isAllowedField(f) {
			log.Fatalf("Invalid field in --fields: %s. Allowed fields: %s", f, strings.Join(allowedFields, ","))
		}
	}
}

func validateOutputFormat(format string) {
	allowedOutputFormats := []string{"tsv"}
	for _, fmt := range allowedOutputFormats {
		if format == fmt {
			return
		}
	}
	log.Fatalf("Invalid value for --output-format: %s. Allowed formats: %s", format, strings.Join(allowedOutputFormats, ","))
}

func parseFlags() (bucket, endpoint, region string, debug bool, fields []string, outputFormat, prefix string, numListPrefixThreads int) {
	bucketFlag := flag.String("bucket", "", "The name of the S3 bucket to list")
	endpointFlag := flag.String("endpoint", "", "The custom S3 endpoint to use (optional)")
	regionFlag := flag.String("region", "", "The AWS region of the S3 bucket")
	debugFlag := flag.Bool("debug", false, "Print debug information (current prefix)")
	fieldsFlag := flag.String("fields", "Key", fmt.Sprintf("Comma-separated list of S3 object fields to print (%s)", strings.Join(allowedFields, ",")))
	outputFormatFlag := flag.String("output-format", "tsv", "Output format: tsv (default), more can be added later")
	prefixFlag := flag.String("prefix", "", "Prefix to start listing from (default: root)")
	threadsFlag := flag.Int("threads", runtime.NumCPU(), "Number of threads for listing prefixes")
	flag.Parse()

	fields = strings.Split(*fieldsFlag, ",")
	if len(fields) == 1 && fields[0] == "" {
		fields = []string{"Key"}
	}
	validateFields(fields)
	validateOutputFormat(*outputFormatFlag)

	return *bucketFlag, *endpointFlag, *regionFlag, *debugFlag, fields, *outputFormatFlag, *prefixFlag, *threadsFlag
}

func main() {
	bucket, endpoint, region, debug, fields, outputFormat, prefix, numListPrefixThreads := parseFlags()

	if bucket == "" {
		log.Fatalf("The --bucket parameter is required")
	}
	if region == "" {
		log.Fatalf("The --region parameter is required")
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		log.Fatalf("Failed to load AWS configuration: %v", err)
	}

	client := MakeS3Client(cfg, endpoint)
	s3fastls := NewS3FastLS(client, bucket, fields, outputFormat, debug, numListPrefixThreads)
	s3fastls.Run(prefix, numListPrefixThreads)
}
