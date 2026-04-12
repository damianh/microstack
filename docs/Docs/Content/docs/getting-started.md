---
title: Getting Started
description: Get MicroStack running in under a minute — via Docker or directly with .NET.
order: 1
section: Basics
---

# Getting Started

MicroStack is a lightweight local AWS service emulator for .NET. It runs 39 AWS services on a single port (4566), allowing you to build and test AWS integrations without connecting to the cloud.

## Quick Start with Docker

```bash
docker run -p 4566:4566 ghcr.io/damianh/microstack:latest
```

Verify it's running:

```bash
curl http://localhost:4566/_ministack/health
```

## Quick Start with .NET

If you have .NET 10 installed, clone and run directly:

```bash
git clone https://github.com/damianh/microstack.git
cd microstack
dotnet run --project src/MicroStack/MicroStack.csproj
```

## Using with AWS SDK for .NET

Point any AWS SDK client at `http://localhost:4566`:

```csharp
using Amazon;
using Amazon.Runtime;
using Amazon.SQS;

var config = new AmazonSQSConfig
{
    ServiceURL = "http://localhost:4566",
    RegionEndpoint = RegionEndpoint.USEast1,
};

var client = new AmazonSQSClient(
    new BasicAWSCredentials("test", "test"), config);

var response = await client.CreateQueueAsync("my-queue");
Console.WriteLine($"Queue URL: {response.QueueUrl}");
```

## Using with AWS CLI

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

aws --endpoint-url http://localhost:4566 sqs create-queue --queue-name my-queue
aws --endpoint-url http://localhost:4566 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:4566 dynamodb create-table \
  --table-name my-table \
  --key-schema AttributeName=pk,KeyType=HASH \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --billing-mode PAY_PER_REQUEST
```

## Next Steps

- [Configuration](/docs/configuration) — Environment variables and options
- [Docker](/docs/docker) — Container image details and Docker Compose
- [Testing](/docs/testing) — Integration testing with `WebApplicationFactory`
- [Services Overview](/docs/services/overview) — All 39 supported services
