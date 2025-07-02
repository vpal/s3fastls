# s3fastls

s3fastls is a command-line tool and Go library for recursively listing Amazon S3 buckets in a highly concurrent manner. It uses "/" as a delimiter to simulate directory traversal, starting a new S3 ListObjectsV2 pager for each discovered prefix (e.g., `a/`, then `a/b/`, then `a/b/c/`, etc.), enabling fast and deep exploration of S3 bucket hierarchies.

## Features
- **Recursive and concurrent listing**: Spawns a new listing goroutine for each discovered prefix, allowing for fast traversal of large and deeply nested S3 buckets.
- **Customizable output**: Choose which S3 object fields to display (e.g., Key, Size, LastModified, ETag, StorageClass) and output format (currently TSV).
- **Thread control**: The number of concurrent prefix listing workers is user-configurable via the `--workers` flag (defaults to the number of CPU cores).
- **Custom endpoint support**: Can be used with S3-compatible storage by specifying a custom endpoint.
- **File output**: Write results to a file instead of stdout using the `--output` flag.
- **Configurable retry behavior**: Control retry attempts and backoff durations for S3 operations.
- **Library and CLI**: Use as a Go library or as a standalone command-line tool.
- **Robust error handling**: All errors from S3, context cancellation, and output writers are propagated and joined as needed. Context errors are detected and surfaced as such.
- **Testability**: The library is designed for easy testing with mocks and fakes, and includes testing for paging, context cancellation, and error propagation.

## Usage as a Command-Line Tool

```
s3fastls --bucket <bucket> [options]
```

### Required Command Line Options
- `--bucket`: Name of the S3 bucket (required)

### Optional Command Line Options
- `--region`: AWS region of the bucket (can be set via environment or AWS config)
- `--endpoint`: Custom S3 endpoint (for S3-compatible storage)
- `--prefix`: Prefix to start listing from (default: root)
- `--fields`: Comma-separated list of fields to print (default: Key)
- `--output`: Write output to file instead of stdout
- `--workers`: Number of concurrent S3 listing workers (default: number of CPU cores)
- `--stats`: Print statistics after listing
- `--cost`: Estimate and print S3 LIST request costs after listing

### Output Format
- Output is always in TSV (tab-separated values) format.
- The columns/fields are printed in the order specified by the `--fields` flag.

### Example

```
export AWS_ACCESS_KEY_ID=your-access-key-id
export AWS_SECRET_ACCESS_KEY=your-secret-access-key
s3fastls --bucket my-bucket --region us-east-1 --fields Key,Size,LastModified --output results.tsv --workers 16
```

## Usage as a Go Library

### Basic Usage
```go
import "github.com/vpal/s3fastls/s3fastls"

ctx := context.Background()
cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
if err != nil {
    log.Fatalf("unable to load SDK config: %v", err)
}

retryConfig := s3fastls.DefaultRetryConfig()
client := s3fastls.MakeS3Client(cfg, "", retryConfig)

params := s3fastls.S3FastLSParams{
    Bucket:       "my-bucket",
    Prefix:       "",
    OutputFields: []s3fastls.Field{s3fastls.FieldKey, s3fastls.FieldSize},
    Formatter:    s3fastls.TSVFormatter, // Use the built-in TSV formatter
    Workers:      16,
}

var buf bytes.Buffer
stats, err := s3fastls.List(ctx, params, client, &buf)
if err != nil {
    log.Fatalf("listing failed: %v", err)
}
fmt.Print(buf.String())
fmt.Printf("Stats: %+v\n", stats)
```

### Custom Output Formatting
You can provide your own formatting function:
```go
params.Formatter = func(fields []string) string {
    return strings.Join(fields, ",") // CSV output
}
```

### Error Handling and Context Support
- The library is context-aware and propagates errors from S3, context cancellation, and output writers in a robust way.
- If all errors are context-related (canceled or deadline exceeded), only the first context error is returned; otherwise, all non-context errors are joined and returned.
- The List function will close all channels and cancel all goroutines on error or cancellation.

## Statistics
- After listing, statistics are available (number of objects, prefixes, and pages processed).
- In the CLI, use `--stats` to print these statistics after listing.

### Available Fields
```go
s3fastls.FieldKey          // Object key
s3fastls.FieldSize         // Object size in bytes
s3fastls.FieldLastModified // Last modified timestamp
s3fastls.FieldETag         // Object ETag
s3fastls.FieldStorageClass // Storage class
```

### Retry Configuration
```go
retryConfig := s3fastls.RetryConfig{
    MaxAttempts: 10,           // Maximum number of retry attempts
    MaxBackoff:  30 * time.Second, // Maximum backoff duration between retries
    MinBackoff:  1 * time.Second,  // Minimum backoff duration for first retry
}
```

## Build and Test
To build the command-line tool:
```
go build -o s3fastls ./cmd
```
To run tests (including integration and error propagation tests):
```
go test -v ./...
```

## Important Notes
- **Too many workers can cause S3 to return HTTP 503 Slow Down errors.** Tune the `--workers` parameter according to your use case and S3 limits.
- This tool is designed for speed and concurrency, but aggressive settings may impact S3 performance or cost.
- All error handling, context cancellation, and channel closing is robust and tested.
- Use this tool at your own risk. The authors are not responsible for any data loss, API throttling, or unexpected costs incurred by its use.

## AWS Authentication, Region, and Credential Resolution

s3fastls uses the standard AWS SDK for Go v2 authentication and configuration mechanisms. **There are no command-line options for credentials**; you must provide them via the standard AWS mechanisms described below.

### How authentication and region resolution works

When you run s3fastls, the AWS SDK will automatically look for credentials and region in the following order (see [AWS SDK for Go v2 documentation](https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/#specifying-credentials) and [AWS documentation](https://docs.aws.amazon.com/sdkref/latest/guide/creds-config.html)):

1. **Environment variables**
   - `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN`
   - `AWS_REGION` or `AWS_DEFAULT_REGION`
2. **Shared credentials file** (`~/.aws/credentials`)
3. **Shared config file** (`~/.aws/config`)
   - Profiles can be selected with `AWS_PROFILE`
4. **EC2/ECS Instance Metadata** (if running on AWS infrastructure)
5. **Other supported credential providers** (see AWS SDK docs)

If you do **not** specify `--region`, the region will be resolved from the above sources in the same order. **Credentials must be provided via one of the above mechanisms.**

**Best practice:**
- Use environment variables or AWS config files for credentials and region, and only override region with CLI flags if needed.
- You can use named profiles with `AWS_PROFILE=your-profile s3fastls ...`.

**References:**
- [AWS SDK for Go v2: Configuration and Credentials](https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/)
- [AWS CLI: Configuration and credential file settings](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)

## License
MIT License
