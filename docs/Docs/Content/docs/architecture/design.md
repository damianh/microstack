---
title: Architecture Design
description: How MicroStack routes requests to 39 AWS service handlers on a single port.
order: 1
section: Architecture
---

# Architecture Design

MicroStack emulates 39 AWS services on a single HTTP port. Every request flows through the same pipeline: middleware reads the raw HTTP request, detects which AWS service is being called, dispatches to the matching handler, and writes the response.

## Request Pipeline

```
HTTP Request
    |
    v
+-------------------------------+
|   ASP.NET Core Middleware     |
|   (AwsRequestMiddleware)      |
|                               |
|  1. Read raw body             |
|  2. Decode AWS chunked enc.   |
|  3. Extract account ID        |
|  4. Route to service handler  |
+-------------------------------+
    |
    v
+-------------------------------+
|   AwsServiceRouter            |
|                               |
|  Detection chain:             |
|  1. X-Amz-Target header       |
|  2. Authorization cred scope  |
|  3. Action query parameter    |
|  4. URL path patterns         |
|  5. Host header patterns      |
|  6. Default -> S3             |
+-------------------------------+
    |
    v
+-------------------------------+
|   IServiceHandler             |
|   (e.g. SqsServiceHandler)    |
|                               |
|  - Parses request             |
|  - Executes action            |
|  - Returns ServiceResponse    |
+-------------------------------+
    |
    v
HTTP Response (with CORS headers)
```

## Key Components

### AwsRequestMiddleware

The central middleware (`src/MicroStack/Internal/AwsRequestMiddleware.cs`) handles every non-admin request:

- **Body reading**: Reads the entire request body into a `byte[]`.
- **AWS chunked decoding**: The AWS SDK sometimes uses a custom chunked transfer encoding (`STREAMING-*` content SHA-256). The middleware decodes this before passing the body to handlers.
- **Account scoping**: Extracts the AWS access key from the `Authorization` header. If it's a 12-digit number, it becomes the account ID for this request (enabling multi-tenancy).
- **Pre-router dispatch**: Some requests are routed before the main detection chain — virtual-hosted S3 buckets, execute-api data plane, ALB data plane, Cognito well-known endpoints, and others.
- **CORS**: Every response gets permissive CORS headers. OPTIONS pre-flight requests return 204 immediately.

### AwsServiceRouter

The router (`src/MicroStack/Internal/AwsServiceRouter.cs`) uses a six-step detection chain to identify the target service:

1. **X-Amz-Target header** — Most reliable for JSON-protocol services (DynamoDB, SQS, SNS, KMS, etc.). Maps prefixes like `DynamoDB_20120810` to `dynamodb`.
2. **Authorization credential scope** — Parses the `Credential=.../service/aws4_request` portion of the AWS Signature V4 header.
3. **Action query parameter** — For Query/XML services (EC2, RDS, SES, CloudFormation, etc.), the `Action` param identifies the service.
4. **URL path patterns** — Lambda (`/2015-03-31/functions`), API Gateway (`/v2/apis`), Route 53 (`/2013-04-01/`), etc.
5. **Host header patterns** — Matches subdomains like `sqs.us-east-1.localhost`.
6. **Default** — Falls back to S3 (path-style bucket operations).

### ServiceRegistry

The registry (`src/MicroStack/Internal/ServiceRegistry.cs`) maps service names to handler instances:

- Handlers register at startup in `Program.cs`.
- The `SERVICES` environment variable filters which handlers are active.
- Service name aliases are supported (e.g., `cloudwatch` -> `monitoring`, `eventbridge` -> `events`).
- Provides `ResetAll()` for the `/_microstack/reset` endpoint and `GetServiceStatus()` for health checks.

### IServiceHandler

Every service implements `IServiceHandler`:

```csharp
internal interface IServiceHandler
{
    string ServiceName { get; }
    Task<ServiceResponse> HandleAsync(ServiceRequest request);
    void Reset();
    object? GetState();
    void RestoreState(object data);
}
```

- `HandleAsync` is the main dispatch point. Each handler parses the action from headers/body/path and routes to the appropriate internal method.
- `Reset` clears all in-memory state (called by the reset endpoint).
- `GetState`/`RestoreState` enable JSON persistence.

### ServiceRequest / ServiceResponse

Simple value types that abstract the HTTP layer:

- **ServiceRequest**: method, path, headers dict, body bytes, query params.
- **ServiceResponse**: status code, headers dict, body bytes. Helpers for JSON, XML, and empty responses.

## AWS Protocol Support

MicroStack handles four AWS API protocols:

| Protocol | Services | How It Works |
|----------|----------|-------------|
| **JSON (X-Amz-Target)** | SQS, DynamoDB, SNS, KMS, SSM, Secrets Manager, Step Functions, CloudWatch Logs, ECS, Kinesis, Glue, Athena, Cognito, ECR, WAF | Action extracted from `X-Amz-Target` header, body is JSON |
| **Query/XML** | EC2, RDS, ElastiCache, SES, CloudFormation, ELB, CloudWatch, IAM, STS | Action from `Action` form parameter, response is XML |
| **REST/JSON** | Lambda, API Gateway, S3 (partially), EFS, AppSync, CloudFront, EventBridge | Action from HTTP method + path, body is JSON |
| **REST/XML** | S3, Route 53 | Action from HTTP method + path, response is XML |
| **CBOR** | CloudWatch Metrics (SDK v4) | Binary CBOR encoding for metrics operations |

## Admin Endpoints

Three admin endpoints are registered before the AWS middleware:

- `GET /_microstack/health` — Returns service status and version. Also available at `/health` and `/_localstack/health`.
- `POST /_microstack/reset` — Clears all in-memory state and deletes persisted state files.
- `POST /_microstack/config` — Applies runtime configuration (currently supports Step Functions mock config).

## Startup Flow

`Program.cs` orchestrates startup:

1. Bind environment variables into `MicroStackOptions`.
2. Configure `AccountContext` with the default account ID.
3. Register core infrastructure (router, registry, persistence) in DI.
4. Build the `WebApplication`.
5. Restore persisted state from disk (if enabled).
6. Instantiate and register all 39 service handlers.
7. Map admin endpoints.
8. Wire middleware pipeline: `UseRouting()` -> OPTIONS handler -> `UseMiddleware<AwsRequestMiddleware>()` -> `UseEndpoints()`.
9. Register shutdown hook to save state.
10. Run.

## Cross-Service Communication

Some services need to call others:

- **SNS -> SQS**: SNS subscriptions with `sqs` protocol inject messages directly via `SqsServiceHandler.InjectMessage()`.
- **Lambda -> SQS/DynamoDB**: Event source mappings poll SQS queues and DynamoDB streams.
- **API Gateway -> Lambda**: Proxy integrations invoke Lambda functions via `LambdaServiceHandler.InvokeForApiGateway()`.
- **Step Functions -> Lambda**: Task states invoke Lambda functions during execution.
- **CloudFormation -> All**: Resource provisioning dispatches to handlers via `ServiceRegistry.Resolve()`.
- **Service Discovery -> Route 53**: DNS namespace operations create hosted zones.
- **STS -> IAM**: STS reads IAM roles and access keys for assume-role operations.
- **Cognito Identity -> Cognito IdP**: Identity pools reference user pool tokens.

These dependencies are wired via constructor injection in `Program.cs`.
