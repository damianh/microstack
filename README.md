# MicroStack

[![CI](https://github.com/damianh/microstack/actions/workflows/ci.yml/badge.svg)](https://github.com/damianh/microstack/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/MicroStack.Aspire.Hosting.svg)](https://www.nuget.org/packages/MicroStack.Aspire.Hosting)
[![NuGet Downloads](https://img.shields.io/nuget/dt/MicroStack.Aspire.Hosting.svg)](https://www.nuget.org/packages/MicroStack.Aspire.Hosting)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A lightweight local AWS service emulator for .NET. Runs **39 AWS services** on a single port, allowing you to build and test AWS integrations without connecting to the cloud.

Ported from [MiniStack](https://github.com/damianh/ministack) (Python) to .NET 10 / C#.

## Packages

| Package | Description |
|---|---|
| [`MicroStack.Aspire.Hosting`](https://www.nuget.org/packages/MicroStack.Aspire.Hosting) | .NET Aspire hosting integration — adds MicroStack as a container resource to your Aspire app model |

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

## Aspire Integration

Add the NuGet package to your Aspire AppHost:

```bash
dotnet add package MicroStack.Aspire.Hosting
```

Then register MicroStack as a resource:

```csharp
var builder = DistributedApplication.CreateBuilder(args);

var microstack = builder.AddMicroStack("microstack");

builder.AddProject<Projects.MyApi>("api")
    .WithReference(microstack);

builder.Build().Run();
```

Configure optional features:

```csharp
var microstack = builder.AddMicroStack("microstack")
    .WithDataVolume()                    // persistent state across restarts
    .WithServices("s3,sqs,dynamodb")     // limit enabled services
    .WithRegion("eu-west-1");            // set AWS region
```

The connection string (e.g. `http://localhost:4566`) is injected into dependent projects
automatically via `WithReference()`.

## Integration Testing

Use .NET Aspire's `DistributedApplicationTestingBuilder` for integration tests that spin up MicroStack
as a container — matching how you run in production:

```csharp
public sealed class SqsTests : IAsyncLifetime
{
    private DistributedApplication _app = null!;
    private string _connectionString = null!;

    public async Task InitializeAsync()
    {
        var builder = await DistributedApplicationTestingBuilder
            .CreateAsync<Projects.MyAspireAppHost>();

        _app = await builder.BuildAsync();
        await _app.StartAsync();

        _connectionString = await _app.GetConnectionStringAsync("microstack")
            ?? throw new InvalidOperationException("MicroStack connection string not found");
    }

    [Fact]
    public async Task CanCreateQueueAndSendMessage()
    {
        var config = new AmazonSQSConfig { ServiceURL = _connectionString };
        using var sqs = new AmazonSQSClient(
            new BasicAWSCredentials("test", "test"), config);

        var created = await sqs.CreateQueueAsync("test-queue");
        Assert.NotEmpty(created.QueueUrl);

        await sqs.SendMessageAsync(created.QueueUrl, "hello world");

        var received = await sqs.ReceiveMessageAsync(created.QueueUrl);
        Assert.Single(received.Messages);
        Assert.Equal("hello world", received.Messages[0].Body);
    }

    public async Task DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
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
