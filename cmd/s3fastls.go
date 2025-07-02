package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/vpal/s3fastls/pkg/s3fastls"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

type FieldsFlag []s3fastls.Field

func (f *FieldsFlag) String() string {
	var fields []string
	for _, field := range *f {
		fields = append(fields, string(field))
	}
	return strings.Join(fields, ",")
}

func (f *FieldsFlag) Set(value string) error {
	if value == "" {
		return fmt.Errorf("fields cannot be empty")
	}
	for s := range strings.SplitSeq(value, ",") {
		field, err := parseField(s)
		if err != nil {
			return fmt.Errorf("invalid field %q", s)
		}
		*f = append(*f, field)
	}
	return nil
}

func parseField(s string) (s3fastls.Field, error) {
	switch s {
	case string(s3fastls.FieldKey):
		return s3fastls.FieldKey, nil
	case string(s3fastls.FieldSize):
		return s3fastls.FieldSize, nil
	case string(s3fastls.FieldLastModified):
		return s3fastls.FieldLastModified, nil
	case string(s3fastls.FieldETag):
		return s3fastls.FieldETag, nil
	case string(s3fastls.FieldStorageClass):
		return s3fastls.FieldStorageClass, nil
	default:
		return "", fmt.Errorf("invalid field: %q", s)
	}
}

var (
	bucket     string
	region     string
	endpoint   string
	prefix     string
	outputFile string
	workers    int
	showStats  bool
	showCost   bool
	fields     FieldsFlag = FieldsFlag{s3fastls.FieldKey} // default value
)

func main() {
	flag.StringVar(&bucket, "bucket", "", "S3 bucket name")
	flag.StringVar(&region, "region", "", "AWS region")
	flag.StringVar(&endpoint, "endpoint", "", "S3 endpoint")
	flag.StringVar(&prefix, "prefix", "", "Prefix to list")
	flag.StringVar(&outputFile, "output", "", "Write output to file")
	flag.IntVar(&workers, "workers", runtime.NumCPU(), "Number of listing workers")
	flag.BoolVar(&showStats, "stats", false, "Print statistics after listing")
	flag.Var(&fields, "fields", "Comma-separated list of fields to print (default: Key)")
	flag.BoolVar(&showCost, "cost", false, "Estimate and print S3 LIST request costs after listing")
	flag.Parse()

	// Check required parameters ASAP
	if bucket == "" {
		log.Fatalf("--bucket is required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("received signal %v, shutting down...", sig)
		cancel()
	}()

	awsCfg, err := func() (aws.Config, error) {
		if region != "" {
			return config.LoadDefaultConfig(ctx, config.WithRegion(region))
		}
		return config.LoadDefaultConfig(ctx)
	}()

	if err != nil {
		log.Fatalf("failed to load AWS configuration: %v", err)
	}

	retryConfig := s3fastls.DefaultRetryConfig()
	client := s3fastls.MakeS3Client(awsCfg, endpoint, retryConfig)

	var writer io.Writer
	if outputFile == "" {
		writer = os.Stdout
	} else {
		file, err := os.Create(outputFile)
		if err != nil {
			log.Fatalf("failed to create output file: %v", err)
		}
		defer file.Close()
		writer = file
	}

	params := s3fastls.S3FastLSParams{
		Bucket:       bucket,
		Prefix:       prefix,
		OutputFields: fields,
		Formatter:    s3fastls.FormatTSV,
		Workers:      workers,
	}

	stats, err := s3fastls.List(ctx, params, client, writer)
	if err != nil {
		log.Fatalf("listing failed: %v", err)
	}
	if showStats {
		fmt.Printf("\nStatistics:\nObjects: %d\nPrefixes: %d\nPages: %d\n", stats.Objects, stats.Prefixes, stats.Pages)
	}
	if showCost {
		cost, err := EstimateS3ListRequestCost(ctx, region, int(stats.Pages))
		if err != nil {
			log.Printf("[warn] failed to estimate S3 LIST costs: %v", err)
		} else {
			fmt.Printf("Estimated S3 LIST request cost: $%.6f\n", cost)
		}
	}
}
