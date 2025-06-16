# s3fastls

s3fastls is a command-line tool and Go library for recursively listing Amazon S3 buckets in a highly concurrent manner. It uses "/" as a delimiter to simulate directory traversal, starting a new S3 ListObjectsV2 pager for each discovered prefix (e.g., `a/`, then `a/b/`, then `a/b/c/`, etc.), enabling fast and deep exploration of S3 bucket hierarchies.

## Features
- **Recursive and concurrent listing**: Spawns a new listing goroutine for each discovered prefix, allowing for fast traversal of large and deeply nested S3 buckets.
- **Customizable output**: Choose which S3 object fields to display (e.g., Key, Size, LastModified, ETag, StorageClass) and output format (currently TSV).
- **Thread control**: The number of concurrent prefix listing workers is user-configurable via the `--workers` flag (defaults to the number of CPU cores).
- **Debug mode**: Optional debug output to trace which prefixes are being listed.
- **Custom endpoint support**: Can be used with S3-compatible storage by specifying a custom endpoint.
- **File output**: Write results to a file instead of stdout using the `--output` flag.
- **Configurable retry behavior**: Control retry attempts and backoff durations for S3 operations.
- **Library and CLI**: Use as a Go library or as a standalone command-line tool.
- **Robust error handling**: All errors from S3, context cancellation, and output writers are propagated and joined as needed. Context errors are detected and surfaced as such.
- **Testability**: The library is designed for easy testing with mocks and fakes, and includes testing for paging, context cancellation, and error propagation.

## Usage as a Command-Line Tool
```
s3fastls --bucket <bucket> --region <region> [options]
```

### Command Line Options
- `--bucket` (required): Name of the S3 bucket.
- `--region` (required): AWS region of the bucket.
- `--endpoint`: Custom S3 endpoint (for S3-compatible storage).
- `--prefix`: Prefix to start listing from (default: root).
- `--fields`: Comma-separated list of fields to print (default: Key).
- `--output-format`: Output format (default: tsv).
- `--output`: Write output to file instead of stdout.
- `--workers`: Number of concurrent S3 listing workers (default: number of CPU cores).
- `--debug`: Print debug information about current prefixes.

### Example

```
s3fastls --bucket my-bucket --region us-east-1 --fields Key,Size,LastModified --output results.tsv --workers 16
```

## Usage as a Go Library

### Basic Usage
```go
import "github.com/vpal/s3fastls/s3fastls"

// Create AWS config and client
ctx := context.Background()
cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
if err != nil {
    log.Fatalf("unable to load SDK config: %v", err)
}

// Configure retry behavior
retryConfig := s3fastls.DefaultRetryConfig() // Or customize: MaxAttempts, MaxBackoff, MinBackoff
client := s3fastls.MakeS3Client(cfg, "", retryConfig)

// Configure listing options
params := s3fastls.S3FastLSParams{
    Bucket:       "my-bucket",
    Prefix:       "", // or any prefix
    OutputFields: []s3fastls.Field{s3fastls.FieldKey, s3fastls.FieldSize},
    OutputFormat: s3fastls.OutputTSV,
    Workers:      16,
    Debug:        false,
}

// Run listing with context and error handling
var buf bytes.Buffer
if err := s3fastls.List(ctx, params, client, &buf); err != nil {
    log.Fatalf("listing failed: %v", err)
}
```

### Error Handling and Context Support
- The library is context-aware and propagates errors from S3, context cancellation, and output writers in a robust way.
- If all errors are context-related (canceled or deadline exceeded), only the first context error is returned; otherwise, all non-context errors are joined and returned.
- The List function will close all channels and cancel all goroutines on error or cancellation.

### Testability
- The library is designed to be easy to test and mock in your own projects.

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

## License
MIT License
