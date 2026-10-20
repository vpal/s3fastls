# s3fastls

A command-line tool and Go package for recursively listing objects in an S3
bucket. It lists `/`-separated prefixes concurrently and writes object keys or
selected metadata as tab-separated text.

Each prefix is listed with `ListObjectsV2`, using `/` as the delimiter. Discovered
subprefixes are listed by separate workers, up to the `--workers` limit. Pages
within a prefix are fetched sequentially, so a flat key layout provides no
parallel listing work. Listing each prefix separately allows concurrent requests,
but can increase the total number of requests because each prefix needs at least
one request, even if it contains only a few objects.

## Build

Requires Go 1.25 or newer.

```sh
git clone https://github.com/vpal/s3fastls.git
cd s3fastls
go build -o s3fastls ./cmd
```

## Authentication

s3fastls loads credentials and configuration through the
[AWS SDK for Go v2](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-gosdk.html).
It reads the shared `~/.aws/credentials` and `~/.aws/config` files. Select a named
profile with `AWS_PROFILE`; otherwise the `default` profile is used:

```sh
AWS_PROFILE=work ./s3fastls --bucket my-bucket --region eu-west-1
```

For an existing IAM Identity Center (SSO) profile, first sign in with
`aws sso login --profile work`, then run the command above. Repeat the login when
the SSO session expires.

You can also supply credentials through environment variables:

```sh
export AWS_ACCESS_KEY_ID='<access-key-id>'
export AWS_SECRET_ACCESS_KEY='<secret-access-key>'
export AWS_SESSION_TOKEN='<session-token>' # Only for temporary credentials
export AWS_REGION=eu-west-1
./s3fastls --bucket my-bucket
```

Environment access keys take precedence over profile credentials, including when
`AWS_PROFILE` is set. The SDK also supports configured `credential_process`
providers, web identity credentials, ECS task roles, and EC2 instance roles.
Set `AWS_SHARED_CREDENTIALS_FILE` or `AWS_CONFIG_FILE` to use other file locations.

The region is taken from `--region`, then `AWS_REGION` (or `AWS_DEFAULT_REGION`),
then the selected profile's `region` setting. s3fastls does not discover the
bucket's region.

For a general purpose S3 bucket, the caller needs `s3:ListBucket` permission on the
bucket ARN, for example `arn:aws:s3:::my-bucket`. Object contents are not read, so
`s3:GetObject` is not required. See the
[ListObjectsV2 permissions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html).

## Usage

List all object keys:

```sh
./s3fastls --bucket my-bucket --region eu-west-1
```

List a prefix with size and modification time, writing results to a file:

```sh
./s3fastls --bucket my-bucket --region eu-west-1 \
  --prefix logs/2026/ --fields Key,Size,LastModified \
  --workers 16 --output objects.tsv --stats
```

`--prefix` is a literal key prefix. For example, `logs/` selects keys below that
prefix, while `logs` also matches keys such as `logs-old/file.txt`.

| Flag | Description | Default |
| --- | --- | --- |
| `--bucket` | Bucket name | Required |
| `--region` | AWS region | Environment or profile |
| `--endpoint` | Custom S3 endpoint URL | SDK endpoint resolution |
| `--prefix` | Key prefix to list recursively | Entire bucket |
| `--fields` | Comma-separated field names, in output order | `Key` |
| `--workers` | Maximum concurrent prefix listings; must be at least 1 | `runtime.NumCPU()` |
| `--output` | Output file; overwrites an existing file | Standard output |
| `--stats` | Print object, prefix, and page counts to standard error after success | `false` |

Field names are case-sensitive: `Key`, `Size`, `LastModified`, `ETag`, and
`StorageClass`. Sizes are in bytes and timestamps use RFC 3339. Output has no
header; fields are separated by tabs and records by newlines. Values are not
escaped, so tabs or newlines in keys affect the output layout.

Output order is unspecified. If a listing fails or is interrupted, the output
may be incomplete and the command exits with a nonzero status.

For an S3-compatible service, set its endpoint, credentials, and signing region:

```sh
AWS_PROFILE=storage ./s3fastls --bucket my-bucket \
  --endpoint https://s3.example.com --region us-east-1
```

The client uses the SDK's default bucket addressing behavior. The CLI has no
option to force path-style addressing.

Retryable requests use the SDK's standard retryer, configured for up to 10
attempts in total, with a maximum backoff of 30 seconds. These settings can be
changed through the Go API.

## Go package

`List` accepts an S3 client, a context, and an `io.Writer`, and returns listing
statistics and an error. Set the output fields, formatter, and worker count
explicitly:

```go
package main

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/vpal/s3fastls/pkg/s3fastls"
)

func main() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("eu-west-1"))
	if err != nil {
		log.Fatal(err)
	}
	client := s3fastls.MakeS3Client(cfg, "", s3fastls.DefaultRetryConfig())

	stats, err := s3fastls.List(ctx, s3fastls.S3FastLSParams{
		Bucket:       "my-bucket",
		Prefix:       "logs/",
		OutputFields: []s3fastls.Field{s3fastls.FieldKey, s3fastls.FieldSize},
		Formatter:    s3fastls.FormatTSV,
		Workers:      16,
	}, client, os.Stdout)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Objects: %d, prefixes: %d, pages: %d",
		stats.Objects, stats.Prefixes, stats.Pages)
}
```

Use `RetryConfig` with `MakeS3Client` to change `MaxAttempts` and `MaxBackoff`, or
pass your own client implementing `S3ListObjectsV2API` to `List`. A custom
`Formatter` receives the selected field values and returns a record; `List` adds
the trailing newline.

## Tests

The tests use mock clients and a local in-memory S3 server; no AWS account is
needed.

```sh
go test ./...
```

## License

MIT License
