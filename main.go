package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"runtime"
	"strings"

	"s3fastls/s3fastls"

	"github.com/aws/aws-sdk-go-v2/config"
)

// Config holds the command-line configuration for s3fastls
type Config struct {
	Bucket       string
	Endpoint     string
	Region       string
	Debug        bool
	Fields       []s3fastls.Field
	OutputFormat s3fastls.OutputFormat
	OutputFile   string
	Prefix       string
	ThreadCount  int
}

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

// parseField converts a string to a Field type
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

// parseOutputFormat converts a string to an OutputFormat type
func parseOutputFormat(s string) (s3fastls.OutputFormat, error) {
	switch s {
	case string(s3fastls.OutputTSV):
		return s3fastls.OutputTSV, nil
	default:
		return "", fmt.Errorf("invalid output format: %q", s)
	}
}

// parseFlags parses command-line flags and returns a Config
func parseFlags() *Config {
	var fields FieldsFlag
	var format OutputFormatFlag = OutputFormatFlag(s3fastls.OutputTSV)

	cfg := &Config{}
	flag.StringVar(&cfg.Bucket, "bucket", "", "The name of the S3 bucket to list")
	flag.StringVar(&cfg.Endpoint, "endpoint", "", "The custom S3 endpoint to use (optional)")
	flag.StringVar(&cfg.Region, "region", "", "The AWS region of the S3 bucket")
	flag.BoolVar(&cfg.Debug, "debug", false, "Print debug information (current prefix)")
	flag.Var(&fields, "fields", "Comma-separated list of S3 object fields to print (Key,Size,LastModified,ETag,StorageClass)")
	flag.Var(&format, "output-format", "Output format: tsv (default)")
	flag.StringVar(&cfg.OutputFile, "output", "", "Output file (default: stdout)")
	flag.StringVar(&cfg.Prefix, "prefix", "", "Prefix to start listing from (default: root)")
	flag.IntVar(&cfg.ThreadCount, "threads", runtime.NumCPU(), "Number of threads for listing prefixes")
	flag.Parse()

	if cfg.Bucket == "" {
		log.Fatal("bucket parameter is required")
	}
	if cfg.Region == "" {
		log.Fatal("region parameter is required")
	}

	cfg.Fields = []s3fastls.Field(fields)
	if len(cfg.Fields) == 0 {
		cfg.Fields = []s3fastls.Field{s3fastls.FieldKey}
	}
	cfg.OutputFormat = s3fastls.OutputFormat(format)

	return cfg
}

func main() {
	cfg := parseFlags()

	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(cfg.Region))
	if err != nil {
		log.Fatalf("failed to load AWS configuration: %v", err)
	}

	client := s3fastls.MakeS3Client(awsCfg, cfg.Endpoint)
	s3ls := s3fastls.NewS3FastLS(client, cfg.Bucket, cfg.Fields, cfg.OutputFormat, cfg.Debug, cfg.ThreadCount)
	if err := s3ls.Run(cfg.Prefix, cfg.ThreadCount, cfg.OutputFile); err != nil {
		log.Fatalf("failed to run s3fastls: %v", err)
	}
}
