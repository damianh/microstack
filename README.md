# MicroStack

A lightweight local AWS service emulator for .NET. Runs **39 AWS services** on a single port, allowing you to build and test AWS integrations without connecting to the cloud.

Ported from [MiniStack](https://github.com/damianh/ministack) (Python) to .NET 10 / C#.

## Quick Start

**Docker:**

```bash
docker run -p 4566:4566 ghcr.io/damianh/microstack:latest
```

**From source:**

```bash
dotnet run --project src/MicroStack/MicroStack.csproj
```

**Verify:**

```bash
curl http://localhost:4566/_ministack/health
```

## Usage

Point any AWS SDK client at `http://localhost:4566`:

```csharp
var config = new AmazonSQSConfig
{
    ServiceURL = "http://localhost:4566",
};

var client = new AmazonSQSClient(
    new BasicAWSCredentials("test", "test"), config);

await client.CreateQueueAsync("my-queue");
```

## Supported Services

S3, SQS, DynamoDB, Lambda, API Gateway (v1+v2), Step Functions, SNS, IAM, STS, Secrets Manager, SSM Parameter Store, KMS, CloudWatch Logs, CloudWatch Metrics, EC2, ECS, RDS, ElastiCache, ECR, RDS Data, EventBridge, Kinesis, Firehose, Glue, Athena, SES, WAF v2, EFS, EMR, AppSync, CloudFront, Route 53, ALB, ACM, Service Discovery, Cognito IdP, Cognito Identity, CloudFormation, S3Files.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_PORT` | `4566` | Port to listen on |
| `MINISTACK_HOST` | `localhost` | Hostname for URL generation |
| `MINISTACK_REGION` | `us-east-1` | Default AWS region |
| `MINISTACK_ACCOUNT_ID` | `000000000000` | Default AWS account ID |
| `PERSIST_STATE` | `0` | Set to `1` for JSON state persistence |
| `STATE_DIR` | `<temp>/ministack-state` | Directory for persisted state |
| `SERVICES` | *(all)* | Comma-separated list of services to enable |

## Integration Testing

Use `WebApplicationFactory<Program>` for fast, in-process tests — no Docker needed:

```csharp
public class MyTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly WebApplicationFactory<Program> _factory;

    public MyTests(WebApplicationFactory<Program> factory)
        => _factory = factory;

    [Fact]
    public async Task CanCreateBucket()
    {
        var handler = new CanonicalizeUriHandler(_factory.Server.CreateHandler());
        var httpClient = new HttpClient(handler) { BaseAddress = new Uri("http://localhost/") };

        var config = new AmazonS3Config
        {
            ServiceURL = "http://localhost/",
            ForcePathStyle = true,
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        var s3 = new AmazonS3Client(new BasicAWSCredentials("test", "test"), config);
        await s3.PutBucketAsync("test-bucket");

        var buckets = await s3.ListBucketsAsync();
        Assert.Contains(buckets.Buckets, b => b.BucketName == "test-bucket");
    }
}
```

## Multi-Tenancy

Use a 12-digit AWS access key to simulate separate accounts:

```csharp
var client1 = new AmazonSQSClient(new BasicAWSCredentials("111111111111", "test"), config);
var client2 = new AmazonSQSClient(new BasicAWSCredentials("222222222222", "test"), config);
// Each account has fully isolated resources
```

## License

MIT

## Acknowledgements

MicroStack is a .NET port of [MiniStack](https://github.com/damianh/ministack), a Python-based local AWS service emulator.
