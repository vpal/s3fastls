# s3fastls

s3fastls is a command-line tool for recursively listing Amazon S3 buckets in a highly concurrent manner. It uses "/" as a delimiter to simulate directory traversal, starting a new S3 ListObjectsV2 pager for each discovered prefix (e.g., `a/`, then `a/b/`, then `a/b/c/`, etc.), enabling fast and deep exploration of S3 bucket hierarchies.

## Features
- **Recursive and concurrent listing**: Spawns a new listing goroutine for each discovered prefix, allowing for fast traversal of large and deeply nested S3 buckets.
- **Customizable output**: Choose which S3 object fields to display (e.g., Key, Size, LastModified, ETag, StorageClass) and output format (currently TSV).
- **Thread control**: The number of concurrent prefix listing threads is user-configurable via the `--threads` flag (defaults to the number of CPU cores).
- **Debug mode**: Optional debug output to trace which prefixes are being listed.
- **Custom endpoint support**: Can be used with S3-compatible storage by specifying a custom endpoint.

## Usage
```
s3fastls --bucket <bucket> --region <region> [--prefix <prefix>] [--fields Key,Size] [--output-format tsv] [--threads N] [--debug] [--endpoint <url>]
```

- `--bucket` (required): Name of the S3 bucket.
- `--region` (required): AWS region of the bucket.
- `--prefix`: Prefix to start listing from (default: root).
- `--fields`: Comma-separated list of fields to print (default: Key).
- `--output-format`: Output format (default: tsv).
- `--threads`: Number of concurrent prefix listing threads (default: number of CPU cores).
- `--debug`: Print debug information about current prefixes.
- `--endpoint`: Custom S3 endpoint (for S3-compatible storage).

## Example
```
s3fastls --bucket my-bucket --region us-east-1 --fields Key,Size,LastModified --output-format tsv --threads 16
```

## Important Notes
- **Too many threads can cause S3 to return HTTP 503 Slow Down errors.** Tune the `--threads` parameter according to your use case and S3 limits.
- This tool is designed for speed and concurrency, but aggressive settings may impact S3 performance or cost.
- Use this tool at your own risk. The authors are not responsible for any data loss, API throttling, or unexpected costs incurred by its use.

## License
MIT License
