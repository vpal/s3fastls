package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"runtime"
	"strings"

	"github.com/vpal/s3fastls/s3fastls"

	"github.com/aws/aws-sdk-go-v2/config"
)

// FieldsFlag implements flag.Value interface for parsing comma-separated field list
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
	for _, s := range strings.Split(value, ",") {
		field, err := parseField(s)
		if err != nil {
			return fmt.Errorf("invalid field %q", s)
		}
		*f = append(*f, field)
	}
	return nil
}

// OutputFormatFlag implements flag.Value interface for parsing output format
type OutputFormatFlag s3fastls.OutputFormat

func (f *OutputFormatFlag) String() string {
	return string(*f)
}

func (f *OutputFormatFlag) Set(value string) error {
	format, err := parseOutputFormat(value)
	if err != nil {
		return err
	}
	*f = OutputFormatFlag(format)
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

func parseOutputFormat(s string) (s3fastls.OutputFormat, error) {
	switch s {
	case string(s3fastls.OutputTSV):
		return s3fastls.OutputTSV, nil
	default:
		return "", fmt.Errorf("invalid output format: %q", s)
	}
}

func parseFlags() (params *s3fastls.S3FastLSParams, region string, endpoint string) {
	params = new(s3fastls.S3FastLSParams)
	var fields FieldsFlag
	var format OutputFormatFlag = OutputFormatFlag(s3fastls.OutputTSV)

	flag.StringVar(&params.Bucket, "bucket", "", "The name of the S3 bucket to list")
	flag.StringVar(&region, "region", "", "The AWS region of the S3 bucket (required)")
	flag.StringVar(&endpoint, "endpoint", "", "Custom S3 endpoint (for S3-compatible storage)")
	flag.StringVar(&params.Prefix, "prefix", "", "Prefix to start listing from (default: root)")
	flag.Var(&fields, "fields", "Comma-separated list of S3 object fields to print (Key,Size,LastModified,ETag,StorageClass)")
	flag.Var(&format, "output-format", "Output format: tsv (default)")
	flag.StringVar(&params.OutputFile, "output", "", "Output file (default: stdout)")
	flag.IntVar(&params.Workers, "workers", runtime.NumCPU(), "Number of concurrent S3 listing workers")
	flag.BoolVar(&params.Debug, "debug", false, "Print debug information (current prefix)")
	flag.Parse()

	if params.Bucket == "" {
		log.Fatal("bucket parameter is required")
	}
	if region == "" {
		log.Fatal("region parameter is required")
	}

	params.OutputFields = []s3fastls.Field(fields)
	if len(params.OutputFields) == 0 {
		params.OutputFields = []s3fastls.Field{s3fastls.FieldKey}
	}
	params.OutputFormat = s3fastls.OutputFormat(format)

	return params, region, endpoint
}

func main() {
	params, region, endpoint := parseFlags()

	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		log.Fatalf("failed to load AWS configuration: %v", err)
	}

	retryConfig := s3fastls.DefaultRetryConfig()
	client := s3fastls.MakeS3Client(awsCfg, endpoint, retryConfig)
	s3fastls.List(
		client,
		*params,
	)
}
