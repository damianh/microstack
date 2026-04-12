---
title: Lambda
description: Lambda function emulation with Python and Node.js worker runtimes.
order: 5
section: Services
---

# Lambda

MicroStack's Lambda handler supports function management, synchronous invocation via subprocess workers, event source mappings (SQS, DynamoDB Streams, Kinesis), and API Gateway integration.

## Supported Operations

- CreateFunction, DeleteFunction, GetFunction, ListFunctions, UpdateFunctionCode, UpdateFunctionConfiguration
- Invoke (synchronous)
- CreateAlias, UpdateAlias, GetAlias, ListAliases, DeleteAlias
- PublishVersion, ListVersionsByFunction
- GetFunctionConcurrency, PutFunctionConcurrency, DeleteFunctionConcurrency
- ListTags, TagResource, UntagResource
- CreateEventSourceMapping, UpdateEventSourceMapping, GetEventSourceMapping, ListEventSourceMappings, DeleteEventSourceMapping

## Worker Runtimes

Lambda functions execute in subprocess workers:

- **Python** (`python3.x` runtimes) — Uses stdin/stdout JSON-line protocol
- **Node.js** (`nodejs18.x`, `nodejs20.x`, etc.) — Uses readline interface

## Usage

```csharp
var lambda = new AmazonLambdaClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonLambdaConfig { ServiceURL = "http://localhost:4566" });

// Create function (provide a zip with handler code)
await lambda.CreateFunctionAsync(new CreateFunctionRequest
{
    FunctionName = "my-function",
    Runtime = Runtime.Python312,
    Handler = "index.handler",
    Role = "arn:aws:iam::000000000000:role/lambda-role",
    Code = new FunctionCode { ZipFile = zipStream },
});

// Invoke
var response = await lambda.InvokeAsync(new InvokeRequest
{
    FunctionName = "my-function",
    Payload = "{\"key\": \"value\"}",
});
```

:::aside{type="note" title="Runtime Requirements"}
Python and Node.js must be available on the host (or in the container) for Lambda function execution. The Docker image includes the .NET runtime but not Python/Node.js by default.
:::
