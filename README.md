# s3fastls

s3fastls is a command-line tool and Go library for recursively listing Amazon S3 buckets in a highly concurrent manner. It uses "/" as a delimiter to simulate directory traversal, starting a new S3 ListObjectsV2 pager for each discovered prefix (e.g., `a/`, then `a/b/`, then `a/b/c/`, etc.), enabling fast and deep exploration of S3 bucket hierarchies.

## Features
- **Recursive and concurrent listing**: Spawns a new listing goroutine for each discovered prefix, allowing for fast traversal of large and deeply nested S3 buckets.
- **Customizable output**: Choose which S3 object fields to display (e.g., Key, Size, LastModified, ETag, StorageClass) and output format (currently TSV).
- **Thread control**: The number of concurrent prefix listing threads is user-configurable via the `--threads` flag (defaults to the number of CPU cores).
- **Debug mode**: Optional debug output to trace which prefixes are being listed.
- **Custom endpoint support**: Can be used with S3-compatible storage by specifying a custom endpoint.
- **File output**: Write results to a file instead of stdout using the `--output` flag.
- **Configurable retry behavior**: Control retry attempts and backoff durations for S3 operations.
- **Library and CLI**: Use as a Go library or as a standalone command-line tool.

## Usage as a Command-Line Tool
```
s3fastls --bucket <bucket> --region <region> [options]
```

### Command Line Options
- `--bucket` (required): Name of the S3 bucket.
- `--region` (required): AWS region of the bucket.
- `--prefix`: Prefix to start listing from (default: root).
- `--fields`: Comma-separated list of fields to print (default: Key).
- `--output-format`: Output format (default: tsv).
- `--output`: Write output to file instead of stdout.
- `--threads`: Number of concurrent prefix listing threads (default: number of CPU cores).
- `--debug`: Print debug information about current prefixes.
- `--endpoint`: Custom S3 endpoint (for S3-compatible storage).

### Example
```
s3fastls --bucket my-bucket --region us-east-1 --fields Key,Size,LastModified --output results.tsv --threads 16
```

## Usage as a Go Library

### Basic Usage
```go
import "github.com/vpal/s3fastls/s3fastls"

// Create AWS config and client
cfg, _ := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))

// Configure retry behavior
retryConfig := s3fastls.DefaultRetryConfig() // Or customize: MaxAttempts, MaxBackoff, MinBackoff
client := s3fastls.MakeS3Client(cfg, "", retryConfig)

// Configure listing options
fields := []s3fastls.Field{s3fastls.FieldKey, s3fastls.FieldSize}
s3ls := s3fastls.NewS3FastLS(client, "my-bucket", fields, s3fastls.OutputTSV, false, 16)

// Run with optional output file
if err := s3ls.Run("", 16, "output.tsv"); err != nil {
    log.Fatal(err)
}
```

### Installation
```
go get github.com/vpal/s3fastls
```

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
go build -o s3fastls ./
```

## Important Notes
- **Too many threads can cause S3 to return HTTP 503 Slow Down errors.** Tune the `--threads` parameter according to your use case and S3 limits.
- This tool is designed for speed and concurrency, but aggressive settings may impact S3 performance or cost.
- Use this tool at your own risk. The authors are not responsible for any data loss, API throttling, or unexpected costs incurred by its use.

## License
MIT License
