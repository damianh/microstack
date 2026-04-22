<p align="center">
  <img src="assets/logo.png" alt="MicroStack" width="128" />
</p>

# MicroStack

[![CI](https://github.com/damianh/microstack/actions/workflows/ci.yml/badge.svg)](https://github.com/damianh/microstack/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/MicroStack.Aspire.Hosting.svg)](https://www.nuget.org/packages/MicroStack.Aspire.Hosting)
[![NuGet Downloads](https://img.shields.io/nuget/dt/MicroStack.Aspire.Hosting.svg)](https://www.nuget.org/packages/MicroStack.Aspire.Hosting)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A lightweight local AWS service emulator for .NET. Runs **39 AWS services** on a single port, allowing you to build and test AWS integrations without connecting to the cloud.

Ported from [MiniStack](https://github.com/damianh/ministack) (Python) to .NET 10 / C#.

## Why MicroStack?

- **39 AWS services** emulated on a single port (4566)
- **Native AOT** — single self-contained binary, ~30MB container image, sub-second startup
- **.NET Aspire integration** — first-class `AddMicroStack()` resource for Aspire app models
- **In-process testing** — run inside `WebApplicationFactory` for sub-millisecond test times
- **Multi-tenant** — 12-digit access keys give each team/pipeline isolated resources
- **MIT licensed** — free forever

## Packages

| Package | Description |
|---|---|
| [`MicroStack.Aspire.Hosting`](https://www.nuget.org/packages/MicroStack.Aspire.Hosting) | .NET Aspire hosting integration — adds MicroStack as a container resource |

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
curl http://localhost:4566/_microstack/health
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

See the [Getting Started guide](https://damianh.github.io/microstack/getting-started) for more examples.

## Aspire Integration

```bash
dotnet add package MicroStack.Aspire.Hosting
```

```csharp
var builder = DistributedApplication.CreateBuilder(args);

var microstack = builder.AddMicroStack("microstack")
    .WithDataVolume()                    // persistent state across restarts
    .WithServices("s3,sqs,dynamodb")     // limit enabled services
    .WithRegion("eu-west-1");            // set AWS region

builder.AddProject<Projects.MyApi>("api")
    .WithReference(microstack);

builder.Build().Run();
```

See [Integration Testing](https://damianh.github.io/microstack/testing) for Aspire-based test patterns.

## Supported Services

| Category | Services |
|---|---|
| **Compute & Serverless** | Lambda, Step Functions, ECS, EMR |
| **Storage** | S3, EFS |
| **Database** | DynamoDB, RDS, RDS Data API, ElastiCache |
| **Messaging** | SQS, SNS, EventBridge, Kinesis, Firehose |
| **Networking** | EC2, Route 53, CloudFront, ALB, API Gateway (v1+v2), Service Discovery |
| **Security** | IAM, STS, KMS, ACM, Cognito (IdP + Identity), WAF v2, Secrets Manager |
| **Management** | CloudFormation, CloudWatch (Logs + Metrics), SSM Parameter Store, AppSync |
| **Other** | SES, ECR, Glue, Athena |

See [Services Overview](https://damianh.github.io/microstack/services/overview) for the full list of supported operations per service.

## Internal API

```bash
# Health check — service status
curl http://localhost:4566/_microstack/health

# Reset all state — useful between test runs
curl -X POST http://localhost:4566/_microstack/reset

# Runtime config — change settings without restart
curl -X POST http://localhost:4566/_microstack/config \
  -H "Content-Type: application/json" \
  -d '{"stepfunctions._sfn_mock_config": "{...}"}'
```

See [Internal API](https://damianh.github.io/microstack/internal-api) for full details.

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

See [Configuration](https://damianh.github.io/microstack/configuration) for all options including S3 persistence, service aliases, and Docker Compose examples.

## Multi-Tenancy

Use a 12-digit AWS access key to simulate separate accounts:

```csharp
var client1 = new AmazonSQSClient(new BasicAWSCredentials("111111111111", "test"), config);
var client2 = new AmazonSQSClient(new BasicAWSCredentials("222222222222", "test"), config);
// Each account has fully isolated resources
```

See [Multi-Tenancy](https://damianh.github.io/microstack/architecture/multi-tenancy) for details.

## Docker

```yaml
services:
  microstack:
    image: ghcr.io/damianh/microstack:latest
    ports:
      - "4566:4566"
    environment:
      - PERSIST_STATE=1
    volumes:
      - microstack-state:/tmp/ministack-state

volumes:
  microstack-state:
```

See [Docker](https://damianh.github.io/microstack/docker) for more options.

## Using with AWS CLI

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

aws --endpoint-url http://localhost:4566 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:4566 sqs create-queue --queue-name my-queue
aws --endpoint-url http://localhost:4566 dynamodb list-tables
```

See [AWS CLI](https://damianh.github.io/microstack/aws-cli) for profiles and more examples.

## License

MIT

## Acknowledgements

MicroStack is a .NET port of [MiniStack](https://github.com/damianh/ministack), a Python-based local AWS service emulator.
