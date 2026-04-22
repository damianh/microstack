# Port MiniStack to .NET 10 / C#

## TL;DR
> **Summary**: Port MiniStack (a Python AWS local emulator with 38+ services, ~38K lines) to a .NET 10 / C# ASP.NET Core application, preserving the same architecture of single-port routing, per-service handlers, in-memory state with account scoping, and optional JSON persistence. Delivered in 8 phases as vertical slices.
> **Estimated Effort**: XL

## Context
### Original Request
Port MiniStack from Python to .NET 10 / C#. MiniStack is a free, open-source local AWS service emulator running 38+ services on a single port (4566). The target is a fresh repo on branch `port-ministack`.

### Key Findings
**Source Structure** (from `D:\repos\damianh\ministack`):
- `ministack/app.py` (900 lines) — ASGI entry point, request routing, admin endpoints, lifespan management
- `ministack/core/router.py` (493 lines) — service detection via X-Amz-Target, Authorization credential scope, Action query param, host header, URL path patterns
- `ministack/core/responses.py` (243 lines) — `AccountScopedDict` for multi-tenancy, XML/JSON response builders, time/hash utilities
- `ministack/core/persistence.py` (93 lines) — JSON-based state save/load with `AccountScopedDict` serialization
- `ministack/core/lambda_runtime.py` (459 lines) — warm/cold worker subprocess pool for Python & Node.js Lambda functions
- `ministack/services/` — 39 service modules (+ cloudformation/ subpackage with 7 files)

**Service Sizes** (lines of Python, determines porting effort):
| Tier | Services | Lines |
|------|----------|-------|
| XL (>1500) | ec2 (3316), lambda_svc (2584), s3 (2442), stepfunctions (2101), rds (1940), cognito (1714), dynamodb (1610), iam_sts (1488) | 17,195 |
| L (900-1500) | eventbridge (1359), ecs (1291), apigateway_v1 (1270), cloudwatch (1218), elasticache (1122), sqs (1104) | 7,364 |
| M (500-900) | glue (938), ses (938), alb (923), sns (859), kinesis (809), appsync (795), athena (771), route53 (765), kms (736), cloudwatch_logs (726), secretsmanager (725), apigateway (603), servicediscovery (520), emr (510) | 10,618 |
| S (<500) | ecr (492), firehose (486), efs (426), ssm (409), cloudfront (387), rds_data (377), waf (310), s3files (287), acm (202), ses_v2 (178) | 3,554 |
| CloudFormation subpackage | provisioners (1658), handlers (543), engine (467), stacks (299), changesets (271), helpers (90), __init__ (67) | 3,395 |

**Total service code**: ~42,126 lines of Python to port.

**Test Sizes** (lines of Python integration tests):
| Tier | Tests | Lines |
|------|-------|-------|
| XL (>900) | test_sfn (1995), test_lambda (1300), test_s3 (1142), test_cfn (1113), test_dynamodb (1083), test_cognito (985), test_ec2 (964), test_apigwv1 (921) | 8,503 |
| L (400-900) | test_apigw (714), test_eventbridge (708), test_route53 (500), test_elbv2 (496), test_sqs (430), test_rds (429) | 3,277 |
| M-S (<400) | Everything else | ~5,500 |

**Patterns Observed**:
1. Every service module follows the same pattern: `async def handle_request(method, path, headers, body, query_params) -> (status, headers, body)`
2. State is stored in module-level `AccountScopedDict` instances (thread-safe via `threading.Lock`)
3. Two protocol families: **AWS Query/XML** (SQS, SNS, IAM, STS, EC2, RDS, ElastiCache, CloudFormation, CloudWatch, SES, ALB) and **AWS JSON** (DynamoDB, Lambda, SecretsManager, CloudWatch Logs, SSM, EventBridge, Kinesis, Step Functions, ECS, ECR, Glue, Athena, KMS, Cognito, WAF, Firehose, AppSync, ServiceDiscovery)
4. Some services use **REST APIs** with path-based routing: Lambda (`/2015-03-31/functions`), API Gateway (`/v2/apis`, `/restapis`), Route53 (`/2013-04-01/`), CloudFront (`/2020-05-31/`), EFS (`/2015-02-01/`), AppSync (`/v1/apis`)
5. Docker integration (via `docker` Python SDK) for RDS (Postgres/MySQL containers), ElastiCache (Redis containers), ECS (arbitrary containers), and Lambda (container image runtime)
6. SNS->SQS and SNS->Lambda fanout is synchronous within the process
7. Lambda event source mappings (SQS, DynamoDB Streams, Kinesis) run in background threads
8. API Gateway and ALB have separate data-plane dispatch via host-based routing (`{apiId}.execute-api.localhost`, `{lb}.alb.localhost`)

## Objectives
### Core Objective
Create a fully functional .NET 10 port of MiniStack that passes equivalent integration tests using AWS SDK for .NET, maintaining the same single-port architecture, routing logic, and service fidelity.

### Deliverables
- [ ] Solution structure with `src/MicroStack/` and `tests/MicroStack.Tests/`
- [ ] ASP.NET Core middleware-based request pipeline with AWS service routing
- [ ] Account-scoped concurrent state management
- [ ] Optional JSON persistence
- [ ] All 38+ AWS service handlers ported
- [ ] Integration tests using AWSSDK.* NuGet packages
- [ ] Dockerfile for the final container image
- [ ] Docker SDK integration (Docker.DotNet) for RDS, ElastiCache, ECS, Lambda container images

### Definition of Done
- [ ] `dotnet build` succeeds with zero warnings
- [ ] `dotnet test` passes all integration tests
- [ ] Health endpoint returns service list at `http://localhost:4566/_microstack/health`
- [ ] AWS SDK for .NET clients can connect and perform CRUD operations on all ported services

### Guardrails (Must NOT)
- Must NOT use MVC controllers — use minimal API route handlers or raw middleware
- Must NOT introduce optional/default parameters — use method overloads
- Must NOT make concrete classes non-sealed unless explicitly justified
- Must NOT expose public types from `*.Internal.*` namespaces
- Must NOT suppress warnings without explicit `#pragma` justification

## TODOs

### Phase 1 — Foundation

- [x] 1 Create Solution and Project Structure
  **What**: Create the .NET solution file, main project, and test project with proper directory structure. Set up `Directory.Build.props` for shared settings (TreatWarningsAsErrors, Nullable enable, ImplicitUsings, TargetFramework net10.0). Create `src/MicroStack/MicroStack.csproj` as a web application and `tests/MicroStack.Tests/MicroStack.Tests.csproj` as an xUnit test project.
  **Files**:
    - `MicroStack.sln`
    - `Directory.Build.props`
    - `src/MicroStack/MicroStack.csproj`
    - `src/MicroStack/Program.cs`
    - `tests/MicroStack.Tests/MicroStack.Tests.csproj`
  **Acceptance**: `dotnet build` succeeds. `dotnet run --project src/MicroStack` starts and listens on port 4566.

- [x] 2 Core Types: ServiceRequest, ServiceResponse, IServiceHandler
  **What**: Define the core abstractions. `ServiceRequest` is a record/struct holding Method, Path, Headers (dictionary), Body (byte[]), QueryParams (dictionary of string to string[]). `ServiceResponse` is a record holding StatusCode (int), Headers (dictionary), Body (byte[]). `IServiceHandler` interface with `Task<ServiceResponse> HandleAsync(ServiceRequest request)` and `string ServiceName { get; }`. Also define `ServiceHandlerBase` abstract class with shared helpers for XML and JSON response building.
  **Files**:
    - `src/MicroStack/ServiceRequest.cs`
    - `src/MicroStack/ServiceResponse.cs`
    - `src/MicroStack/IServiceHandler.cs`
  **Acceptance**: Types compile. `ServiceResponse` can be constructed with status, headers, body.

  Source reference: `ministack/core/responses.py` lines 144-208 (xml_response, json_response, error helpers)

- [x] 3 Account Scoping and Multi-Tenancy
  **What**: Port `AccountScopedDict` to C# as a generic `AccountScopedDictionary<TKey, TValue>` backed by `ConcurrentDictionary`. Use `AsyncLocal<string>` (equivalent of Python's `contextvars.ContextVar`) for per-request account ID. Port `set_request_account_id` logic (12-digit access key becomes account ID, otherwise fallback to env var or "000000000000"). Include `GetAccountId()` static method. Also port `AccountScopedList<T>` if needed for services that use lists.
  **Files**:
    - `src/MicroStack/Internal/AccountContext.cs`
    - `src/MicroStack/Internal/AccountScopedDictionary.cs`
  **Acceptance**: Unit test: two different account IDs can store/retrieve different values under the same key. `Clear()` wipes all accounts.

  Source reference: `ministack/core/responses.py` lines 18-141 (AccountScopedDict)

- [x] 4 AWS Request Router
  **What**: Port the service detection logic from `router.py`. Implement `AwsServiceRouter` as a sealed class that takes a `ServiceRequest` and returns the matched service name string. Detection order: (1) X-Amz-Target header prefix matching, (2) Authorization header credential scope extraction, (3) Action query parameter mapping, (4) URL path pattern matching, (5) Host header pattern matching, (6) default to "s3". Port all the `SERVICE_PATTERNS`, `action_service_map`, and `scope_map` dictionaries. Also port `ExtractRegion`, `ExtractAccessKeyId`, and `ExtractAccountId` helper methods.
  **Files**:
    - `src/MicroStack/Internal/AwsServiceRouter.cs`
  **Acceptance**: Unit tests covering each detection path: target header (DynamoDB), credential scope (ECR), action param (CreateQueue -> sqs), path (/2015-03-31/functions -> lambda), host (sqs.us-east-1.amazonaws.com -> sqs), default (unknown -> s3).

  Source reference: `ministack/core/router.py` (entire file, 493 lines)

- [x] 5 AWS Request Pipeline Middleware
  **What**: Create the main ASP.NET Core middleware that: (a) reads the raw HTTP request into a `ServiceRequest`, (b) decodes AWS chunked transfer encoding when `x-amz-content-sha256` starts with "STREAMING-" or `content-encoding` contains "aws-chunked", (c) sets the per-request account ID from the Authorization header, (d) resolves the target service via `AwsServiceRouter`, (e) dispatches to the matching `IServiceHandler`, (f) writes the `ServiceResponse` back. Add CORS headers on all responses. Handle OPTIONS pre-flight. Return 400 for unknown services (not 501, matching Python behavior).
  **Files**:
    - `src/MicroStack/Internal/AwsRequestMiddleware.cs`
    - `src/MicroStack/Program.cs` (update to register middleware)
  **Acceptance**: Server starts, routes a DynamoDB-style request (with X-Amz-Target header) to a stub handler, returns response.

  Source reference: `ministack/app.py` lines 193-552 (ASGI app function)

- [x] 6 Special Route Handling (Pre-Router)
  **What**: Implement the special routing paths that are handled before general service dispatch in `app.py`: (a) S3 virtual-hosted style (`{bucket}.localhost` or `{bucket}.s3.localhost`), (b) API Gateway execute-api data plane (`{apiId}.execute-api.localhost`), (c) ALB data plane (`{lb}.alb.localhost` or `/_alb/{lb}/...`), (d) S3 Control API (`/v20180820/...`), (e) RDS Data API REST paths (`/Execute`, `/BeginTransaction`, etc.), (f) SES v2 REST paths (`/v2/email/...`), (g) Cognito well-known endpoints (`/.well-known/jwks.json`, `/.well-known/openid-configuration`), (h) Lambda layer content download (`/_microstack/lambda-layers/...`). For Phase 1, stub these routes to return 501 — they'll be implemented when their respective services are ported.
  **Files**:
    - `src/MicroStack/Internal/AwsRequestMiddleware.cs` (extend)
  **Acceptance**: Virtual-hosted S3 request to `mybucket.localhost:4566/key` is recognized and routed (to stub). Execute-api host header is recognized.

  Source reference: `ministack/app.py` lines 263-513 (pre-router special cases)

- [x] 7 Admin Endpoints (Health, Reset, Config)
  **What**: Implement the three admin endpoints: (a) `GET /_microstack/health` (and aliases `/health`, `/_localstack/health`) returns JSON `{"services": {...}, "edition": "light", "version": "0.1.0"}` listing all registered service handlers, (b) `POST /_microstack/reset` calls `Reset()` on all registered service handlers and wipes persistence files, (c) `POST /_microstack/config` accepts JSON body with allowed config keys (stub for now). Register these as minimal API endpoints or handle in middleware before service dispatch.
  **Files**:
    - `src/MicroStack/Internal/AdminEndpoints.cs`
    - `src/MicroStack/Program.cs` (wire up)
  **Acceptance**: `curl http://localhost:4566/_microstack/health` returns 200 with services JSON. `curl -X POST http://localhost:4566/_microstack/reset` returns 200 with `{"reset":"ok"}`.

  Source reference: `ministack/app.py` lines 289-321, 399-408, 750-819

- [x] 8 JSON State Persistence Framework
  **What**: Port the persistence module. When `PERSIST_STATE=1` env var is set, service state is saved to `STATE_DIR` (default `/tmp/ministack-state`) as JSON files on shutdown, and reloaded on startup. Each service handler exposes `GetState()` and `RestoreState(data)` methods (define on `IServiceHandler` or as a separate `IStatefulService` interface). Use `System.Text.Json` with custom converters for `AccountScopedDictionary` serialization (the Python version uses `__scoped__` marker). Implement atomic writes (write to `.tmp` then rename).
  **Files**:
    - `src/MicroStack/Internal/StatePersistence.cs`
    - `src/MicroStack/IServiceHandler.cs` (extend with state methods or separate interface)
  **Acceptance**: Start server with `PERSIST_STATE=1`, create a resource, stop server, restart — resource is still there.

  Source reference: `ministack/core/persistence.py` (entire file, 93 lines)

- [x] 9 Response Helpers (XML and JSON builders)
  **What**: Port the response utility functions: `XmlResponse(rootTag, namespace, children, status)`, `ErrorResponseXml(code, message, status, namespace)`, `JsonResponse(data, status)`, `ErrorResponseJson(code, message, status)`. Port time utilities: `NowIso()`, `NowRfc7231()`, `IsoToRfc7231()`, `NowEpoch()`. Port hash utilities: `Md5Hash()`, `Sha256Hash()`, `NewUuid()`. Use `System.Xml.Linq` (XDocument/XElement) for XML building — cleaner than `XmlDocument`. All methods should return `ServiceResponse` directly.
  **Files**:
    - `src/MicroStack/Internal/AwsResponseHelpers.cs`
    - `src/MicroStack/Internal/TimeHelpers.cs`
    - `src/MicroStack/Internal/HashHelpers.cs`
  **Acceptance**: `XmlResponse("CreateQueueResponse", "http://queue.amazonaws.com/doc/2012-11-05/", ...)` produces valid AWS-style XML with `<?xml version="1.0"?>` declaration and `ResponseMetadata/RequestId`.

  Source reference: `ministack/core/responses.py` lines 144-243

- [x] 10 Service Name Aliases and SERVICES Environment Filter
  **What**: Port the `SERVICE_NAME_ALIASES` mapping and the `SERVICES` environment variable filter. When `SERVICES=s3,sqs,dynamodb` is set, only those services are enabled (others return 400 "Unsupported service"). Map alias names to canonical names (e.g., "cloudwatch-logs" -> "logs", "step-functions" -> "states"). Also handle `LOCALSTACK_PERSISTENCE=1` -> `S3_PERSIST=1` compatibility.
  **Files**:
    - `src/MicroStack/Internal/ServiceRegistry.cs`
  **Acceptance**: Start with `SERVICES=s3,sqs`. Request to DynamoDB returns 400. Request to S3 is routed correctly. Alias "cloudwatch-logs" resolves to "logs".

  Source reference: `ministack/app.py` lines 136-175

- [x] 11 Foundation Integration Tests
  **What**: Create the test infrastructure: a test fixture that starts the MicroStack server in-process using `WebApplicationFactory<T>` (or `TestServer`), configures AWS SDK clients (`AWSSDK.Core`, `AWSSDK.S3`, `AWSSDK.SQS`, etc.) to point at the test server endpoint. Mirror the Python `conftest.py` pattern. Write tests for health endpoint, reset endpoint, and OPTIONS CORS response.
  **Files**:
    - `tests/MicroStack.Tests/MicroStackFixture.cs`
    - `tests/MicroStack.Tests/HealthTests.cs`
  **Acceptance**: `dotnet test --filter "HealthTests"` passes — health endpoint returns services, reset returns ok.

  Source reference: `ministack/tests/conftest.py`, `ministack/tests/test_health.py`

### Phase 2 — Core Data Services

- [x] 12 SQS Service Handler
  **What**: Port `sqs.py` (1104 lines). Implement both Query/XML protocol (Action=SendMessage, etc.) and JSON protocol (X-Amz-Target: AmazonSQS.*). Actions: CreateQueue, DeleteQueue, ListQueues, GetQueueUrl, GetQueueAttributes, SetQueueAttributes, PurgeQueue, SendMessage, ReceiveMessage, DeleteMessage, ChangeMessageVisibility, ChangeMessageVisibilityBatch, SendMessageBatch, DeleteMessageBatch, ListQueueTags, TagQueue, UntagQueue. Key features: standard + FIFO queues (.fifo suffix), dead-letter queues (RedrivePolicy), long polling (WaitTimeSeconds via `Task.Delay`), message deduplication (5-min window), FIFO message-group ordering, message attributes, system attributes (ApproximateReceiveCount, SentTimestamp, etc.), queue ARN generation.
  **Files**:
    - `src/MicroStack/Services/Sqs/SqsServiceHandler.cs`
  **Acceptance**: Integration tests: create queue, send/receive/delete message, FIFO queue with dedup, DLQ redrive, queue attributes, batch operations. Match Python test coverage from `test_sqs.py` (430 lines, ~25 test cases).

  Source reference: `ministack/services/sqs.py` (1104 lines)

- [x] 13 DynamoDB Service Handler
  **What**: Port `dynamodb.py` (1610 lines). JSON protocol only (X-Amz-Target: DynamoDB_20120810.*). Actions: CreateTable, DeleteTable, DescribeTable, ListTables, UpdateTable, PutItem, GetItem, DeleteItem, UpdateItem, Query, Scan, BatchWriteItem, BatchGetItem, TransactWriteItems, TransactGetItems, DescribeTimeToLive, UpdateTimeToLive, DescribeContinuousBackups, UpdateContinuousBackups, DescribeEndpoints, TagResource, UntagResource, ListTagsOfResource. Key features: partition key + optional sort key, GSI/LSI support, condition expressions, projection expressions, update expressions, filter expressions, key condition expressions, begins_with/between/contains operators, DynamoDB Streams record emission (for Lambda ESM integration in Phase 4), consistent read, pagination (ExclusiveStartKey/LastEvaluatedKey), item size tracking, TTL auto-deletion.
  **Files**:
    - `src/MicroStack/Services/DynamoDb/DynamoDbServiceHandler.cs`
    - `src/MicroStack/Services/DynamoDb/ExpressionParser.cs`
  **Acceptance**: Integration tests matching `test_dynamodb.py` (1083 lines, ~40 test cases): CRUD operations, queries with key conditions, scans with filters, batch operations, transactions, GSI queries, update expressions.

  Source reference: `ministack/services/dynamodb.py` (1610 lines)

- [x] 14 S3 Service Handler
  **What**: Port `s3.py` (2442 lines). REST/XML protocol with path-based routing. Actions: CreateBucket, DeleteBucket, ListBuckets, HeadBucket, PutObject, GetObject, DeleteObject, HeadObject, CopyObject, ListObjectsV1 (Marker pagination), ListObjectsV2 (ContinuationToken), DeleteObjects (batch), multipart upload (Create/UploadPart/Complete/Abort/List/ListParts), object tagging, ListObjectVersions, bucket sub-resources (Policy, Versioning, Encryption, Lifecycle, CORS, ACL, Tagging, Notification, Logging, Accelerate, RequestPayment, Website), object lock (PutObjectLockConfiguration, GetObjectLockConfiguration, Retention, LegalHold), replication, range requests (206 Partial Content), Content-MD5 validation, encoding-type=url, x-amz-metadata-directive, x-amz-copy-source-if-match preconditions. Optional disk persistence via `S3_DATA_DIR` when `S3_PERSIST=1`.
  **Files**:
    - `src/MicroStack/Services/S3/S3ServiceHandler.cs`
    - `src/MicroStack/Services/S3/S3Persistence.cs`
    - `src/MicroStack/Services/S3/MultipartUploadManager.cs`
  **Acceptance**: Integration tests matching `test_s3.py` (1142 lines, ~60 test cases): bucket CRUD, object CRUD, versioning, multipart upload, copy, range requests, tagging, listing with pagination.

  Source reference: `ministack/services/s3.py` (2442 lines)

- [x] 15 SNS Service Handler
  **What**: Port `sns.py` (859 lines). Query/XML protocol. Actions: CreateTopic, DeleteTopic, ListTopics, GetTopicAttributes, SetTopicAttributes, Subscribe, Unsubscribe, ConfirmSubscription, ListSubscriptions, ListSubscriptionsByTopic, GetSubscriptionAttributes, SetSubscriptionAttributes, Publish, PublishBatch, ListTagsForResource, TagResource, UntagResource, CreatePlatformApplication, CreatePlatformEndpoint. Key feature: SNS->SQS fanout (on Publish, deliver to SQS subscriptions synchronously), SNS->Lambda fanout (invoke Lambda function), SNS->HTTP/HTTPS endpoint delivery. Filter policies on subscriptions.
  **Files**:
    - `src/MicroStack/Services/Sns/SnsServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_sns.py` (359 lines, ~20 test cases): create topic, subscribe SQS queue, publish message, verify message arrives in SQS, topic attributes, tags.

  Source reference: `ministack/services/sns.py` (859 lines)

- [x] 16 Phase 2 Integration Tests
  **What**: Full integration test suite for SQS, DynamoDB, S3, SNS using AWS SDK for .NET. Each test class mirrors the corresponding Python test file. Use `AWSSDK.SQS`, `AWSSDK.DynamoDBv2`, `AWSSDK.S3`, `AWSSDK.SimpleNotificationService` NuGet packages.
  **Files**:
    - `tests/MicroStack.Tests/SqsTests.cs`
    - `tests/MicroStack.Tests/DynamoDbTests.cs`
    - `tests/MicroStack.Tests/S3Tests.cs`
    - `tests/MicroStack.Tests/SnsTests.cs`
  **Acceptance**: All tests pass. Coverage comparable to Python test suite.

### Phase 3 — Identity & Config Services

- [x] 17 IAM/STS Service Handler
  **What**: Port `iam_sts.py` (1488 lines). Query/XML protocol for both IAM and STS (they share a module but are two separate service endpoints). IAM actions (~40): CreateUser/GetUser/ListUsers/DeleteUser, CreateRole/GetRole/ListRoles/DeleteRole, CreatePolicy/GetPolicy/ListPolicies/DeletePolicy, policy versions, AttachRolePolicy/DetachRolePolicy, inline policies (PutRolePolicy/GetRolePolicy), access keys, instance profiles, groups, OIDC providers, service-linked roles, tag operations, SimulatePrincipalPolicy/SimulateCustomPolicy. STS actions (5): GetCallerIdentity, AssumeRole, GetSessionToken, GetAccessKeyInfo, AssumeRoleWithWebIdentity.
  **Files**:
    - `src/MicroStack/Services/Iam/IamServiceHandler.cs`
    - `src/MicroStack/Services/Sts/StsServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_iam.py` (345 lines) and `test_sts.py` (69 lines): create/get/list/delete roles, users, policies, attach policies, get caller identity, assume role.

  Source reference: `ministack/services/iam_sts.py` (1488 lines)

- [x] 18 Secrets Manager Service Handler
  **What**: Port `secretsmanager.py` (725 lines). JSON protocol via X-Amz-Target. Actions: CreateSecret, GetSecretValue, ListSecrets, DeleteSecret, RestoreSecret, UpdateSecret, DescribeSecret, PutSecretValue, UpdateSecretVersionStage, TagResource, UntagResource, ListSecretVersionIds, RotateSecret, GetRandomPassword, ReplicateSecretToRegions, PutResourcePolicy, GetResourcePolicy, DeleteResourcePolicy, ValidateResourcePolicy. Key features: version staging (AWSCURRENT, AWSPENDING, AWSPREVIOUS), scheduled deletion with recovery window, binary secrets.
  **Files**:
    - `src/MicroStack/Services/SecretsManager/SecretsManagerServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_secretsmanager.py` (279 lines): create/get/update/delete secrets, version staging, rotation.

  Source reference: `ministack/services/secretsmanager.py` (725 lines)

- [x] 19 SSM Parameter Store Service Handler
  **What**: Port `ssm.py` (409 lines). JSON protocol via X-Amz-Target: AmazonSSM.*. Actions: PutParameter, GetParameter, GetParameters, GetParametersByPath, DeleteParameter, DeleteParameters, DescribeParameters, ListTagsForResource, AddTagsToResource, RemoveTagsFromResource, GetParameterHistory. Key features: String/StringList/SecureString types, hierarchical paths with recursive get, parameter versioning.
  **Files**:
    - `src/MicroStack/Services/Ssm/SsmServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_ssm.py` (166 lines): put/get/delete parameters, get by path, parameter history.

  Source reference: `ministack/services/ssm.py` (409 lines)

- [x] 20 KMS Service Handler
  **What**: Port `kms.py` (736 lines). JSON protocol via X-Amz-Target: TrentService.*. Actions: CreateKey, DescribeKey, ListKeys, EnableKey, DisableKey, ScheduleKeyDeletion, CancelKeyDeletion, CreateAlias, DeleteAlias, ListAliases, UpdateAlias, Encrypt, Decrypt, ReEncrypt, GenerateDataKey, GenerateDataKeyWithoutPlaintext, GenerateRandom, Sign, Verify, GetKeyPolicy, PutKeyPolicy, GetKeyRotationStatus, EnableKeyRotation, DisableKeyRotation, TagResource, UntagResource, ListResourceTags. Key features: key metadata with states, AES-256 key material generation, encrypt/decrypt using real AES, alias management.
  **Files**:
    - `src/MicroStack/Services/Kms/KmsServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_kms.py` (376 lines): create key, encrypt/decrypt roundtrip, aliases, key scheduling.

  Source reference: `ministack/services/kms.py` (736 lines)

- [x] 21 Phase 3 Integration Tests
  **What**: Integration tests for IAM, STS, Secrets Manager, SSM, KMS.
  **Files**:
    - `tests/MicroStack.Tests/IamTests.cs`
    - `tests/MicroStack.Tests/StsTests.cs`
    - `tests/MicroStack.Tests/SecretsManagerTests.cs`
    - `tests/MicroStack.Tests/SsmTests.cs`
    - `tests/MicroStack.Tests/KmsTests.cs`
  **Acceptance**: All tests pass.

### Phase 4 — Compute Services

- [x] 22 Lambda Service Handler (Core)
  **What**: Port `lambda_svc.py` (2584 lines). REST JSON protocol with path-based routing (`/2015-03-31/functions/...`). Actions: CreateFunction, DeleteFunction, GetFunction, GetFunctionConfiguration, ListFunctions (paginated), Invoke (RequestResponse/Event/DryRun), UpdateFunctionCode, UpdateFunctionConfiguration, PublishVersion, ListVersionsByFunction, CreateAlias/GetAlias/UpdateAlias/DeleteAlias/ListAliases, AddPermission/RemovePermission/GetPolicy, ListTags/TagResource/UntagResource, PublishLayerVersion/GetLayerVersion/ListLayerVersions/DeleteLayerVersion/ListLayers, CreateEventSourceMapping/DeleteEventSourceMapping/GetEventSourceMapping/ListEventSourceMappings/UpdateEventSourceMapping, CreateFunctionUrlConfig/GetFunctionUrlConfig/UpdateFunctionUrlConfig/DeleteFunctionUrlConfig, PutFunctionConcurrency/GetFunctionConcurrency/DeleteFunctionConcurrency. Key features: zip code storage, version/alias management, function URLs.
  **Files**:
    - `src/MicroStack/Services/Lambda/LambdaServiceHandler.cs`
    - `src/MicroStack/Services/Lambda/LambdaFunctionStore.cs`
  **Acceptance**: Integration tests: create function, list functions, get function configuration, publish version, create alias, function URLs.

  Source reference: `ministack/services/lambda_svc.py` (2584 lines)

- [x] 23 Lambda Runtime (Worker Pool)
  **What**: Port `lambda_runtime.py` (459 lines). Implement warm/cold start worker subprocess pool. Each function gets a persistent worker `Process` (Python or Node.js) that communicates via stdin/stdout JSON-line protocol. The `Worker` class: extracts zip to temp dir, spawns subprocess with init payload (code_dir, module, handler, env), reads ready response, then sends event JSON and reads result JSON for each invocation. Port `_PYTHON_WORKER_SCRIPT` and `_NODEJS_WORKER_SCRIPT` as embedded resources. Implement `GetOrCreateWorker`, `InvalidateWorker`, `Reset`. Use `System.Diagnostics.Process` for subprocess management. For Docker-based Lambda execution (LAMBDA_EXECUTOR=docker), use `Docker.DotNet` to run containers with the AWS Lambda Runtime Interface Emulator.
  **Files**:
    - `src/MicroStack/Services/Lambda/LambdaWorkerPool.cs`
    - `src/MicroStack/Services/Lambda/LambdaWorker.cs`
    - `src/MicroStack/Services/Lambda/Scripts/python_worker.py` (embedded resource)
    - `src/MicroStack/Services/Lambda/Scripts/nodejs_worker.js` (embedded resource)
  **Acceptance**: Create a Python Lambda function, invoke it, get result. Create a Node.js Lambda function, invoke it, get result. Warm invocation reuses same process (no cold start).

  Source reference: `ministack/core/lambda_runtime.py` (459 lines)

- [x] 24 Lambda Event Source Mappings
  **What**: Implement background polling for SQS, DynamoDB Streams, and Kinesis event source mappings. Port the background thread that polls SQS queues, batches messages, invokes the Lambda function, and deletes successfully processed messages. Port DynamoDB Streams polling (reads `_stream_records` from DynamoDB module). Port Kinesis shard iterator polling. Use `BackgroundService` or `IHostedService` with `Timer`/`PeriodicTimer`.
  **Files**:
    - `src/MicroStack/Services/Lambda/EventSourceMappingPoller.cs`
  **Acceptance**: Create SQS queue, create Lambda with SQS ESM, send message to queue — Lambda is invoked with the SQS event.

  Source reference: `ministack/services/lambda_svc.py` (ESM polling section, ~200 lines within the file)

- [ ] 25 API Gateway v2 (HTTP APIs) Service Handler
  **What**: Port `apigateway.py` (603 lines). REST JSON protocol for control plane (`/v2/apis/...`). Actions: CreateApi, GetApi, GetApis, DeleteApi, UpdateApi, CreateRoute, GetRoutes, DeleteRoute, CreateIntegration, GetIntegrations, DeleteIntegration, CreateStage, GetStages, DeleteStage, CreateDeployment, GetDeployments, GetDomainNames, CreateAuthorizer, GetAuthorizers, DeleteAuthorizer. Data plane: dispatch execute-api requests via host header `{apiId}.execute-api.localhost` — match route, invoke Lambda integration, return response.
  **Files**:
    - `src/MicroStack/Services/ApiGateway/ApiGatewayV2ServiceHandler.cs`
    - `src/MicroStack/Services/ApiGateway/ApiGatewayV2DataPlane.cs`
  **Acceptance**: Integration tests matching `test_apigw.py` (714 lines): create API, add route with Lambda integration, invoke via execute-api host.

  Source reference: `ministack/services/apigateway.py` (603 lines)

- [ ] 26 API Gateway v1 (REST APIs) Service Handler
  **What**: Port `apigateway_v1.py` (1270 lines). REST JSON protocol for control plane (`/restapis/...`). Actions: CreateRestApi, GetRestApis, GetRestApi, DeleteRestApi, CreateResource, GetResources, CreateMethod, PutMethod, PutIntegration, PutIntegrationResponse, PutMethodResponse, CreateDeployment, GetDeployments, GetStages, CreateStage, DeleteStage, UpdateStage, CreateApiKey, GetApiKeys, CreateUsagePlan, GetUsagePlans, GetDomainNames, CreateBasePathMapping, GetBasePathMappings, CreateModel, GetModels, GetAccount, CreateAuthorizer, GetAuthorizers. Data plane: dispatch REST API requests through method/integration/response chain with velocity template rendering.
  **Files**:
    - `src/MicroStack/Services/ApiGateway/ApiGatewayV1ServiceHandler.cs`
    - `src/MicroStack/Services/ApiGateway/ApiGatewayV1DataPlane.cs`
  **Acceptance**: Integration tests matching `test_apigwv1.py` (921 lines): create REST API, add resources/methods/integrations, deploy, invoke.

  Source reference: `ministack/services/apigateway_v1.py` (1270 lines)

- [x] 27 Step Functions Service Handler
  **What**: Port `stepfunctions.py` (2101 lines). JSON protocol via X-Amz-Target: AWSStepFunctions.*. Actions: CreateStateMachine, DescribeStateMachine, ListStateMachines, DeleteStateMachine, UpdateStateMachine, StartExecution, DescribeExecution, ListExecutions, StopExecution, GetExecutionHistory, StartSyncExecution, TagResource, UntagResource, ListTagsForResource. Key features: ASL (Amazon States Language) execution engine supporting Pass, Task, Wait, Choice, Parallel, Map, Succeed, Fail states. Task state invokes Lambda functions. Mock configuration support via `_sfn_mock_config`.
  **Files**:
    - `src/MicroStack/Services/StepFunctions/StepFunctionsServiceHandler.cs`
  **Acceptance**: 52 integration tests (51 pass, 1 skip for SNS protocol mismatch). All state types, service integrations, mock config, activities, retry/catch covered.

  Source reference: `ministack/services/stepfunctions.py` (2101 lines)

- [x] 28 Phase 4 Integration Tests
  **What**: Integration tests for Lambda, API Gateway v1/v2, Step Functions.
  **Files**:
    - `tests/MicroStack.Tests/LambdaTests.cs`
    - `tests/MicroStack.Tests/ApiGatewayV2Tests.cs`
    - `tests/MicroStack.Tests/ApiGatewayV1Tests.cs`
    - `tests/MicroStack.Tests/StepFunctionsTests.cs`
  **Acceptance**: All 478 tests pass (1 skip).

### Phase 5 — Infrastructure Services

- [ ] 29 EC2 Service Handler
  **What**: Port `ec2.py` (3316 lines — the largest service). Query/XML protocol. Actions (~50): RunInstances, DescribeInstances, TerminateInstances, StopInstances, StartInstances, RebootInstances, DescribeImages, CreateSecurityGroup/DeleteSecurityGroup/DescribeSecurityGroups, AuthorizeSecurityGroupIngress/Egress, CreateKeyPair/DeleteKeyPair/DescribeKeyPairs/ImportKeyPair, VPC operations (Create/Delete/Describe/ModifyVpcAttribute), Subnet operations, InternetGateway operations, RouteTable operations, Routes, NetworkInterface operations, VpcEndpoint operations, Address (Elastic IP) operations, Tags, AvailabilityZones, EBS Volumes (Create/Delete/Describe/Attach/Detach/Modify), EBS Snapshots. All in-memory, no real instances.
  **Files**:
    - `src/MicroStack/Services/Ec2/Ec2ServiceHandler.cs`
    - `src/MicroStack/Services/Ec2/Ec2XmlResponses.cs`
  **Acceptance**: Integration tests matching `test_ec2.py` (964 lines) and `test_ebs.py` (114 lines): VPC/subnet/SG CRUD, instance lifecycle, key pairs, EBS volumes.

  Source reference: `ministack/services/ec2.py` (3316 lines)

- [ ] 30 ALB/ELBv2 Service Handler
  **What**: Port `alb.py` (923 lines). Query/XML protocol for control plane + HTTP data plane. Control plane actions: CreateLoadBalancer, DescribeLoadBalancers, DeleteLoadBalancer, ModifyLoadBalancerAttributes, DescribeLoadBalancerAttributes, CreateTargetGroup, DescribeTargetGroups, ModifyTargetGroup, DeleteTargetGroup, RegisterTargets, DeregisterTargets, DescribeTargetHealth, CreateListener, DescribeListeners, ModifyListener, DeleteListener, CreateRule, DescribeRules, ModifyRule, DeleteRule, SetRulePriorities, AddTags, RemoveTags, DescribeTags, DescribeTargetGroupAttributes, ModifyTargetGroupAttributes. Data plane: match listener rules, forward to target group (IP targets via HTTP proxy, Lambda targets via invoke).
  **Files**:
    - `src/MicroStack/Services/Alb/AlbServiceHandler.cs`
    - `src/MicroStack/Services/Alb/AlbDataPlane.cs`
  **Acceptance**: Integration tests matching `test_elbv2.py` (496 lines): create ALB, target groups, listeners, rules, register targets.

  Source reference: `ministack/services/alb.py` (923 lines)

- [ ] 31 Route53 Service Handler
  **What**: Port `route53.py` (765 lines). REST/XML protocol with path-based routing (`/2013-04-01/hostedzone/...`). Actions: CreateHostedZone, GetHostedZone, ListHostedZones, ListHostedZonesByName, DeleteHostedZone, ChangeResourceRecordSets (UPSERT, CREATE, DELETE), ListResourceRecordSets, GetHostedZoneCount, ChangeTagsForResource, ListTagsForResource. Key features: hosted zone ID generation, SOA/NS auto-creation, record set management with pagination.
  **Files**:
    - `src/MicroStack/Services/Route53/Route53ServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_route53.py` (500 lines): create/delete hosted zones, UPSERT/CREATE/DELETE records, list records with pagination.

  Source reference: `ministack/services/route53.py` (765 lines)

- [ ] 32 ACM Service Handler
  **What**: Port `acm.py` (202 lines). JSON protocol via X-Amz-Target: CertificateManager.*. Actions: RequestCertificate, DescribeCertificate, ListCertificates, DeleteCertificate, ListTagsForCertificate, AddTagsToCertificate, RemoveTagsFromCertificate. Generates self-signed certificates using `System.Security.Cryptography.X509Certificates`.
  **Files**:
    - `src/MicroStack/Services/Acm/AcmServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_acm.py` (61 lines): request cert, describe, list, delete, tags.

  Source reference: `ministack/services/acm.py` (202 lines)

- [ ] 33 CloudWatch Logs Service Handler
  **What**: Port `cloudwatch_logs.py` (726 lines). JSON protocol via X-Amz-Target: Logs_20140328.*. Actions: CreateLogGroup, DeleteLogGroup, DescribeLogGroups, CreateLogStream, DeleteLogStream, DescribeLogStreams, PutLogEvents, GetLogEvents, FilterLogEvents, PutRetentionPolicy, DeleteRetentionPolicy, TagResource, UntagResource, ListTagsForResource, ListTagsLogGroup, TagLogGroup, UntagLogGroup, PutSubscriptionFilter, DescribeSubscriptionFilters, DeleteSubscriptionFilter, PutMetricFilter, DescribeMetricFilters, DeleteMetricFilter, PutDestination, DescribeDestinations, PutDestinationPolicy, DeleteDestination.
  **Files**:
    - `src/MicroStack/Services/CloudWatchLogs/CloudWatchLogsServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_logs.py` (254 lines): create log group/stream, put/get/filter events, retention policy.

  Source reference: `ministack/services/cloudwatch_logs.py` (726 lines)

- [ ] 34 CloudWatch Metrics Service Handler
  **What**: Port `cloudwatch.py` (1218 lines). Query/XML protocol + Smithy RPC v2 CBOR support. Actions: PutMetricData, GetMetricData, GetMetricStatistics, ListMetrics, PutMetricAlarm, DescribeAlarms, DeleteAlarms, SetAlarmState, EnableAlarmActions, DisableAlarmActions, DescribeAlarmsForMetric, DescribeAlarmHistory, PutCompositeAlarm, ListDashboards, GetDashboard, PutDashboard, DeleteDashboards, ListTagsForResource, TagResource, UntagResource. Key features: metric data point storage with timestamps, statistic calculations (SUM, AVG, MIN, MAX, COUNT), alarm evaluation, Smithy RPC v2 CBOR protocol (via `smithy-rpc-v2-cbor` content-type with CBOR encoding).
  **Files**:
    - `src/MicroStack/Services/CloudWatch/CloudWatchServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_cloudwatch.py` (361 lines): put metrics, get metric data, list metrics, create/describe/delete alarms.

  Source reference: `ministack/services/cloudwatch.py` (1218 lines)

- [ ] 35 Phase 5 Integration Tests
  **What**: Integration tests for EC2, ALB, Route53, ACM, CloudWatch Logs, CloudWatch Metrics.
  **Files**:
    - `tests/MicroStack.Tests/Ec2Tests.cs`
    - `tests/MicroStack.Tests/AlbTests.cs`
    - `tests/MicroStack.Tests/Route53Tests.cs`
    - `tests/MicroStack.Tests/AcmTests.cs`
    - `tests/MicroStack.Tests/CloudWatchLogsTests.cs`
    - `tests/MicroStack.Tests/CloudWatchTests.cs`
  **Acceptance**: All tests pass.

### Phase 6 — Container & Database Services

- [ ] 36 ECS Service Handler
  **What**: Port `ecs.py` (1291 lines). JSON protocol via X-Amz-Target or REST paths. 47 operations covering clusters, task definitions, services, tasks, capacity providers, tags, account settings, attributes, deployments, agent operations. Docker integration: when Docker socket is available, `RunTask` actually starts containers. Use `Docker.DotNet` for container lifecycle management.
  **Files**:
    - `src/MicroStack/Services/Ecs/EcsServiceHandler.cs`
    - `src/MicroStack/Services/Ecs/EcsDockerIntegration.cs`
  **Acceptance**: Integration tests matching `test_ecs.py` (265 lines): create cluster, register task definition, create service, run task, stop task.

  Source reference: `ministack/services/ecs.py` (1291 lines)

- [ ] 37 RDS Service Handler
  **What**: Port `rds.py` (1940 lines). Query/XML protocol. Actions: CreateDBInstance, DeleteDBInstance, DescribeDBInstances, ModifyDBInstance, Start/Stop/RebootDBInstance, CreateDBCluster/DeleteDBCluster/DescribeDBClusters/ModifyDBCluster, subnet groups, parameter groups, snapshots, cluster snapshots, option groups, read replicas, global clusters, tags, engine versions, orderable options. Docker integration: `CreateDBInstance` spins up real Postgres/MySQL container with `Docker.DotNet`, returns actual host:port as endpoint.
  **Files**:
    - `src/MicroStack/Services/Rds/RdsServiceHandler.cs`
    - `src/MicroStack/Services/Rds/RdsDockerIntegration.cs`
  **Acceptance**: Integration tests matching `test_rds.py` (429 lines): create/describe/delete instances, clusters, subnet groups, parameter groups, snapshots, tags.

  Source reference: `ministack/services/rds.py` (1940 lines)

- [ ] 38 ElastiCache Service Handler
  **What**: Port `elasticache.py` (1122 lines). Query/XML protocol. Actions: Create/Delete/Describe/Modify/Reboot CacheCluster, replication groups, subnet groups, parameter groups, users, user groups, cache engine versions, snapshots, tags, events. Docker integration: `CreateCacheCluster` spins up Redis container when Docker available, otherwise returns configured Redis sidecar address.
  **Files**:
    - `src/MicroStack/Services/ElastiCache/ElastiCacheServiceHandler.cs`
    - `src/MicroStack/Services/ElastiCache/ElastiCacheDockerIntegration.cs`
  **Acceptance**: Integration tests matching `test_elasticache.py` (242 lines): create/describe/delete clusters, replication groups, subnet groups, parameter groups.

  Source reference: `ministack/services/elasticache.py` (1122 lines)

- [ ] 39 ECR Service Handler
  **What**: Port `ecr.py` (492 lines). JSON protocol via X-Amz-Target: AmazonEC2ContainerRegistry_V20150921.*. Actions: CreateRepository, DeleteRepository, DescribeRepositories, ListImages, GetAuthorizationToken, BatchGetImage, PutImage, BatchDeleteImage, PutLifecyclePolicy, GetLifecyclePolicy, DeleteLifecyclePolicy, TagResource, UntagResource, ListTagsForResource, GetRepositoryPolicy, SetRepositoryPolicy, DeleteRepositoryPolicy.
  **Files**:
    - `src/MicroStack/Services/Ecr/EcrServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_ecr.py` (158 lines): create/describe/delete repos, authorization token, lifecycle policies.

  Source reference: `ministack/services/ecr.py` (492 lines)

- [ ] 40 RDS Data API Service Handler
  **What**: Port `rds_data.py` (377 lines). REST JSON protocol with path-based routing (`/Execute`, `/BeginTransaction`, etc.). Actions: ExecuteStatement, BatchExecuteStatement, BeginTransaction, CommitTransaction, RollbackTransaction. Routes SQL to actual Postgres/MySQL instances created by RDS service.
  **Files**:
    - `src/MicroStack/Services/RdsData/RdsDataServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_rds_data.py` (189 lines): execute statement, batch execute, transactions.

  Source reference: `ministack/services/rds_data.py` (377 lines)

- [ ] 41 Phase 6 Integration Tests
  **What**: Integration tests for ECS, RDS, ElastiCache, ECR, RDS Data API.
  **Files**:
    - `tests/MicroStack.Tests/EcsTests.cs`
    - `tests/MicroStack.Tests/RdsTests.cs`
    - `tests/MicroStack.Tests/ElastiCacheTests.cs`
    - `tests/MicroStack.Tests/EcrTests.cs`
    - `tests/MicroStack.Tests/RdsDataTests.cs`
  **Acceptance**: All tests pass.

### Phase 7 — Advanced Services

- [ ] 42 CloudFormation Service Handler
  **What**: Port `cloudformation/` subpackage (3395 lines across 7 files). Query/XML protocol. Actions: CreateStack, DescribeStacks, UpdateStack, DeleteStack, ListStacks, DescribeStackEvents, DescribeStackResource, DescribeStackResources, ListStackResources, GetTemplateSummary, ValidateTemplate, CreateChangeSet, DescribeChangeSet, ExecuteChangeSet, DeleteChangeSet, ListChangeSets, ListExports, ListImports, UpdateTerminationProtection, SetStackPolicy, GetStackPolicy. Key features: template parsing (JSON/YAML), resource provisioning engine that calls other MicroStack services to create resources (S3 buckets, SQS queues, Lambda functions, etc.), dependency resolution via DependsOn and Ref/GetAtt, stack outputs and exports, change set diffing.
  **Files**:
    - `src/MicroStack/Services/CloudFormation/CloudFormationServiceHandler.cs`
    - `src/MicroStack/Services/CloudFormation/StackEngine.cs`
    - `src/MicroStack/Services/CloudFormation/ResourceProvisioners.cs`
    - `src/MicroStack/Services/CloudFormation/TemplateHelpers.cs`
    - `src/MicroStack/Services/CloudFormation/ChangeSetManager.cs`
  **Acceptance**: Integration tests matching `test_cfn.py` (1113 lines): create stack with S3 bucket + SQS queue, describe stack, stack events, outputs, update stack, delete stack, change sets.

  Source reference: `ministack/services/cloudformation/` (7 files, 3395 lines total)

- [ ] 43 Cognito Service Handler
  **What**: Port `cognito.py` (1714 lines). JSON protocol via X-Amz-Target for both Cognito-IdP (AWSCognitoIdentityProviderService) and Cognito-Identity (AWSCognitoIdentityService). Cognito-IdP actions: CreateUserPool, DescribeUserPool, ListUserPools, DeleteUserPool, CreateUserPoolClient, DescribeUserPoolClient, ListUserPoolClients, DeleteUserPoolClient, AdminCreateUser, AdminGetUser, AdminDeleteUser, AdminSetUserPassword, AdminInitiateAuth, AdminRespondToAuthChallenge, AdminConfirmSignUp, SignUp, ConfirmSignUp, InitiateAuth, RespondToAuthChallenge, ForgotPassword, ConfirmForgotPassword, ChangePassword, GlobalSignOut, AdminUpdateUserAttributes, AdminListGroupsForUser, CreateGroup, GetGroup, ListGroups, DeleteGroup, AdminAddUserToGroup, AdminRemoveUserFromGroup, UpdateUserPoolClient, ListUsers, SetUserPoolMfaConfig. Cognito-Identity: CreateIdentityPool, DescribeIdentityPool, ListIdentityPools, DeleteIdentityPool, GetId, GetCredentialsForIdentity, GetOpenIdTokenForDeveloperIdentity. OAuth2 endpoints: `/oauth2/token`. Well-known: JWKS, OpenID configuration. JWT token generation with RSA keys.
  **Files**:
    - `src/MicroStack/Services/Cognito/CognitoServiceHandler.cs`
    - `src/MicroStack/Services/Cognito/JwtTokenGenerator.cs`
  **Acceptance**: Integration tests matching `test_cognito.py` (985 lines): user pool CRUD, user management, auth flows, user pool clients, groups, identity pools.

  Source reference: `ministack/services/cognito.py` (1714 lines)

- [ ] 44 EventBridge Service Handler
  **What**: Port `eventbridge.py` (1359 lines). JSON protocol via X-Amz-Target: AWSEvents.*. Actions: PutEvents, PutRule, DescribeRule, ListRules, DeleteRule, EnableRule, DisableRule, PutTargets, RemoveTargets, ListTargetsByRule, PutPartnerEventsSource, CreateEventBus, DescribeEventBus, ListEventBuses, DeleteEventBus, DescribePartnerEventSource, ListPartnerEventSources, ListPartnerEventSourceAccounts, CreateConnection, DescribeConnection, UpdateConnection, DeleteConnection, ListConnections, CreateApiDestination, DescribeApiDestination, UpdateApiDestination, DeleteApiDestination, ListApiDestinations, ListTagsForResource, TagResource, UntagResource, CreateArchive, DescribeArchive, ListArchives, DeleteArchive, DescribeReplay, StartReplay, ListReplays, PutPermission, RemovePermission, DescribeEndpoint. Key features: event pattern matching for rules, target fanout (SQS, Lambda, etc.).
  **Files**:
    - `src/MicroStack/Services/EventBridge/EventBridgeServiceHandler.cs`
    - `src/MicroStack/Services/EventBridge/EventPatternMatcher.cs`
  **Acceptance**: Integration tests matching `test_eventbridge.py` (708 lines): put events, create rules with patterns, targets to SQS/Lambda, event buses, connections, API destinations.

  Source reference: `ministack/services/eventbridge.py` (1359 lines)

- [ ] 45 Kinesis Service Handler
  **What**: Port `kinesis.py` (809 lines). JSON protocol via X-Amz-Target: Kinesis_20131202.*. Actions: CreateStream, DeleteStream, DescribeStream, DescribeStreamSummary, ListStreams, ListShards, GetShardIterator, GetRecords, PutRecord, PutRecords, MergeShards, SplitShard, IncreaseStreamRetentionPeriod, DecreaseStreamRetentionPeriod, ListStreamConsumers, RegisterStreamConsumer, DeregisterStreamConsumer, DescribeStreamConsumer, ListTagsForStream, AddTagsToStream, RemoveTagsFromStream, UpdateShardCount.
  **Files**:
    - `src/MicroStack/Services/Kinesis/KinesisServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_kinesis.py` (318 lines): create stream, put/get records, shard iterators, stream tagging.

  Source reference: `ministack/services/kinesis.py` (809 lines)

- [ ] 46 Firehose Service Handler
  **What**: Port `firehose.py` (486 lines). JSON protocol via X-Amz-Target: Firehose_20150804.*. Actions: CreateDeliveryStream, DeleteDeliveryStream, DescribeDeliveryStream, ListDeliveryStreams, UpdateDestination, PutRecord, PutRecordBatch, TagDeliveryStream, UntagDeliveryStream, ListTagsForDeliveryStream. S3 destination writes to MicroStack S3 buckets.
  **Files**:
    - `src/MicroStack/Services/Firehose/FirehoseServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_firehose.py` (292 lines): create/describe delivery stream, put records, S3 destination.

  Source reference: `ministack/services/firehose.py` (486 lines)

- [ ] 47 Glue Service Handler
  **What**: Port `glue.py` (938 lines). JSON protocol via X-Amz-Target: AWSGlue.*. Actions: CreateDatabase, GetDatabase, GetDatabases, DeleteDatabase, UpdateDatabase, CreateTable, GetTable, GetTables, DeleteTable, UpdateTable, BatchGetTable, CreatePartition, GetPartition, GetPartitions, BatchCreatePartition, BatchDeletePartition, DeletePartition, UpdatePartition, CreateCrawler, GetCrawler, GetCrawlers, DeleteCrawler, UpdateCrawler, StartCrawler, StopCrawler, GetCrawlerMetrics, CreateJob, GetJob, GetJobs, DeleteJob, UpdateJob, StartJobRun, GetJobRun, GetJobRuns, BatchStopJobRun, CreateRegistry, GetRegistry, ListRegistries, DeleteRegistry, CreateSchema, GetSchema, ListSchemas, DeleteSchema, RegisterSchemaVersion, GetSchemaVersion, ListSchemaVersions, TagResource, UntagResource, GetTags.
  **Files**:
    - `src/MicroStack/Services/Glue/GlueServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_glue.py` (358 lines): database/table/partition CRUD, crawler operations, job operations.

  Source reference: `ministack/services/glue.py` (938 lines)

- [ ] 48 Athena Service Handler
  **What**: Port `athena.py` (771 lines). JSON protocol via X-Amz-Target: AmazonAthena.*. Actions: CreateWorkGroup, GetWorkGroup, ListWorkGroups, DeleteWorkGroup, UpdateWorkGroup, StartQueryExecution, GetQueryExecution, ListQueryExecutions, StopQueryExecution, GetQueryResults, GetNamedQuery, CreateNamedQuery, DeleteNamedQuery, ListNamedQueries, CreateDataCatalog, GetDataCatalog, ListDataCatalogs, DeleteDataCatalog, ListTableMetadata, GetTableMetadata, ListDatabases, GetDatabase, TagResource, UntagResource, ListTagsForResource. Optional: real query execution against configured Athena engine.
  **Files**:
    - `src/MicroStack/Services/Athena/AthenaServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_athena.py` (267 lines): workgroups, start/get query execution, named queries, data catalogs.

  Source reference: `ministack/services/athena.py` (771 lines)

- [ ] 49 Phase 7 Integration Tests
  **What**: Integration tests for CloudFormation, Cognito, EventBridge, Kinesis, Firehose, Glue, Athena.
  **Files**:
    - `tests/MicroStack.Tests/CloudFormationTests.cs`
    - `tests/MicroStack.Tests/CognitoTests.cs`
    - `tests/MicroStack.Tests/EventBridgeTests.cs`
    - `tests/MicroStack.Tests/KinesisTests.cs`
    - `tests/MicroStack.Tests/FirehoseTests.cs`
    - `tests/MicroStack.Tests/GlueTests.cs`
    - `tests/MicroStack.Tests/AthenaTests.cs`
  **Acceptance**: All tests pass.

### Phase 8 — Remaining Services

- [ ] 50 SES Service Handler
  **What**: Port `ses.py` (938 lines) + `ses_v2.py` (178 lines). SES v1: Query/XML protocol. Actions: SendEmail, SendRawEmail, VerifyEmailIdentity, VerifyEmailAddress, VerifyDomainIdentity, VerifyDomainDkim, ListIdentities, DeleteIdentity, GetSendQuota, GetSendStatistics, ListVerifiedEmailAddresses, configuration sets, templates. SES v2: REST JSON protocol (`/v2/email/...`). Actions: SendEmail, GetAccount, CreateEmailIdentity, GetEmailIdentity, ListEmailIdentities.
  **Files**:
    - `src/MicroStack/Services/Ses/SesServiceHandler.cs`
    - `src/MicroStack/Services/Ses/SesV2ServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_ses.py` (207 lines): send email, verify identity, configuration sets, templates.

  Source reference: `ministack/services/ses.py` (938 lines), `ministack/services/ses_v2.py` (178 lines)

- [ ] 51 WAF v2 Service Handler
  **What**: Port `waf.py` (310 lines). JSON protocol via X-Amz-Target: AWSWAF_20190729.*. Actions: CreateWebACL, GetWebACL, ListWebACLs, DeleteWebACL, UpdateWebACL, CreateIPSet, GetIPSet, ListIPSets, DeleteIPSet, UpdateIPSet, CreateRuleGroup, ListRuleGroups, ListTagsForResource, TagResource, UntagResource, GetLoggingConfiguration, PutLoggingConfiguration, DeleteLoggingConfiguration.
  **Files**:
    - `src/MicroStack/Services/Waf/WafServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_waf.py` (179 lines): WebACL CRUD, IP sets, rule groups, tags.

  Source reference: `ministack/services/waf.py` (310 lines)

- [ ] 52 EFS Service Handler
  **What**: Port `efs.py` (426 lines). REST JSON protocol with path-based routing (`/2015-02-01/file-systems/...`). Actions: CreateFileSystem, DescribeFileSystems, DeleteFileSystem, CreateMountTarget, DescribeMountTargets, DeleteMountTarget, DescribeAccessPoints, CreateAccessPoint, DeleteAccessPoint, TagResource, UntagResource, ListTagsForResource.
  **Files**:
    - `src/MicroStack/Services/Efs/EfsServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_efs.py` (102 lines): create/describe/delete file systems, mount targets, access points.

  Source reference: `ministack/services/efs.py` (426 lines)

- [ ] 53 EMR Service Handler
  **What**: Port `emr.py` (510 lines). JSON protocol via X-Amz-Target: ElasticMapReduce.*. Actions: RunJobFlow, DescribeCluster, ListClusters, TerminateJobFlows, AddJobFlowSteps, ListSteps, DescribeStep, AddTags, RemoveTags, SetTerminationProtection, SetVisibleToAllUsers, ListInstanceGroups, PutAutoScalingPolicy, RemoveAutoScalingPolicy, ListBootstrapActions.
  **Files**:
    - `src/MicroStack/Services/Emr/EmrServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_emr.py` (301 lines): run job flow, describe cluster, add steps, list clusters.

  Source reference: `ministack/services/emr.py` (510 lines)

- [ ] 54 AppSync Service Handler
  **What**: Port `appsync.py` (795 lines). REST JSON protocol with path-based routing (`/v1/apis/...`). Actions: CreateGraphqlApi, GetGraphqlApi, ListGraphqlApis, DeleteGraphqlApi, UpdateGraphqlApi, CreateDataSource, GetDataSource, ListDataSources, DeleteDataSource, UpdateDataSource, StartSchemaCreation, GetSchemaCreationStatus, GetIntrospectionSchema, CreateResolver, GetResolver, ListResolvers, DeleteResolver, UpdateResolver, CreateFunction, GetFunction, ListFunctions, DeleteFunction, UpdateFunction, CreateApiKey, ListApiKeys, DeleteApiKey, UpdateApiKey, TagResource, UntagResource, ListTagsForResource.
  **Files**:
    - `src/MicroStack/Services/AppSync/AppSyncServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_appsync.py` (257 lines): GraphQL API CRUD, data sources, resolvers, API keys.

  Source reference: `ministack/services/appsync.py` (795 lines)

- [ ] 55 CloudFront Service Handler
  **What**: Port `cloudfront.py` (387 lines). REST/XML protocol with path-based routing (`/2020-05-31/distribution/...`). Actions: CreateDistribution, GetDistribution, ListDistributions, DeleteDistribution, UpdateDistribution, TagResource, UntagResource, ListTagsForResource, CreateInvalidation, ListInvalidations, GetInvalidation, CreateOriginAccessControl, ListOriginAccessControls, GetOriginAccessControl, DeleteOriginAccessControl.
  **Files**:
    - `src/MicroStack/Services/CloudFront/CloudFrontServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_cloudfront.py` (194 lines): distribution CRUD, invalidations, origin access controls.

  Source reference: `ministack/services/cloudfront.py` (387 lines)

- [ ] 56 ServiceDiscovery (Cloud Map) Service Handler
  **What**: Port `servicediscovery.py` (520 lines). JSON protocol via X-Amz-Target: Route53AutoNaming_v20170314.*. Actions: CreatePrivateDnsNamespace, CreatePublicDnsNamespace, CreateHttpNamespace, GetNamespace, ListNamespaces, DeleteNamespace, CreateService, GetService, ListServices, DeleteService, UpdateService, RegisterInstance, DeregisterInstance, GetInstance, ListInstances, DiscoverInstances, GetOperation, ListOperations, TagResource, UntagResource, ListTagsForResource.
  **Files**:
    - `src/MicroStack/Services/ServiceDiscovery/ServiceDiscoveryServiceHandler.cs`
  **Acceptance**: Integration tests matching `test_servicediscovery.py` (169 lines): namespace/service/instance CRUD, discover instances.

  Source reference: `ministack/services/servicediscovery.py` (520 lines)

- [ ] 57 S3Files Service Handler
  **What**: Port `s3files.py` (287 lines). Credential-scope based routing. Provides file-system like operations on S3 buckets.
  **Files**:
    - `src/MicroStack/Services/S3Files/S3FilesServiceHandler.cs`
  **Acceptance**: Basic smoke test: service accepts requests and returns expected response structure.

  Source reference: `ministack/services/s3files.py` (287 lines)

- [ ] 58 Phase 8 Integration Tests
  **What**: Integration tests for SES, WAF, EFS, EMR, AppSync, CloudFront, ServiceDiscovery, S3Files.
  **Files**:
    - `tests/MicroStack.Tests/SesTests.cs`
    - `tests/MicroStack.Tests/WafTests.cs`
    - `tests/MicroStack.Tests/EfsTests.cs`
    - `tests/MicroStack.Tests/EmrTests.cs`
    - `tests/MicroStack.Tests/AppSyncTests.cs`
    - `tests/MicroStack.Tests/CloudFrontTests.cs`
    - `tests/MicroStack.Tests/ServiceDiscoveryTests.cs`
  **Acceptance**: All tests pass.

### Phase 9 — Packaging & Polish

- [x] 59 Docker / OCI Image
  **What**: Replaced the multi-stage Dockerfile with `dotnet publish /t:PublishContainer` OCI image support. Container configuration is in `MicroStack.csproj` via `ContainerRepository`, `ContainerImageTag`, `ContainerPort`, and `ContainerEnvironmentVariable` MSBuild items. Removed `Dockerfile` and `.dockerignore`.
  **Files**:
    - `src/MicroStack/MicroStack.csproj` (container properties added)
    - `Dockerfile` (deleted)
    - `.dockerignore` (deleted)
  **Acceptance**: `dotnet publish /t:PublishContainer -c Release` pushes `microstack:latest` to local Docker. `docker run -p 4566:4566 microstack:latest` starts and health endpoint responds. ✅ Verified.

- [x] 60 Configuration System
  **What**: Consolidate all environment variable handling into a strongly-typed configuration class. Environment variables: `GATEWAY_PORT`, `LOG_LEVEL`, `MINISTACK_HOST`, `MINISTACK_REGION`, `MINISTACK_ACCOUNT_ID`, `PERSIST_STATE`, `STATE_DIR`, `S3_PERSIST`, `S3_DATA_DIR`, `REDIS_HOST`, `REDIS_PORT`, `RDS_BASE_PORT`, `ELASTICACHE_BASE_PORT`, `LAMBDA_EXECUTOR`, `LAMBDA_DOCKER_NETWORK`, `SERVICES`, `LOCALSTACK_PERSISTENCE`. Use `IConfiguration` binding.
  **Files**:
    - `src/MicroStack/MicroStackOptions.cs`
    - `src/MicroStack/Program.cs` (update configuration binding)
  **Acceptance**: All environment variables are read from configuration. Changing `GATEWAY_PORT=5000` starts server on port 5000.

- [ ] 61 Multi-Tenancy Integration Test
  **What**: Port `test_multitenancy.py` (113 lines). Verify that two different AWS access keys (12-digit account IDs) see isolated state for all services.
  **Files**:
    - `tests/MicroStack.Tests/MultiTenancyTests.cs`
  **Acceptance**: Account A creates a resource, Account B cannot see it. Reset clears both.

  Source reference: `ministack/tests/test_multitenancy.py`

- [x] 62 Persistence Integration Test
  **What**: Port `test_microstack_persist.py` (272 lines). Verify that with `PERSIST_STATE=1`, service state survives server restart.
  **Files**:
    - `tests/MicroStack.Tests/PersistenceTests.cs`
  **Acceptance**: Create resources, restart server, resources still exist.

  Source reference: `ministack/tests/test_microstack_persist.py`

- [ ] 63 Unicode Handling Test
  **What**: Port `test_unicode.py` (64 lines). Verify Unicode characters in S3 keys, SQS messages, DynamoDB items, SNS messages are handled correctly.
  **Files**:
    - `tests/MicroStack.Tests/UnicodeTests.cs`
  **Acceptance**: Unicode roundtrip tests pass.

  Source reference: `ministack/tests/test_unicode.py`

## Verification

- [ ] `dotnet build src/MicroStack/MicroStack.csproj` succeeds with zero warnings
- [ ] `dotnet build tests/MicroStack.Tests/MicroStack.Tests.csproj` succeeds with zero warnings
- [ ] `dotnet test tests/MicroStack.Tests/` — all integration tests pass
- [ ] Health endpoint at `http://localhost:4566/_microstack/health` returns all services
- [ ] Reset endpoint at `http://localhost:4566/_microstack/reset` clears all state
- [ ] AWS SDK for .NET clients with `ServiceURL=http://localhost:4566` can perform operations on all services
- [ ] Docker build succeeds and container runs correctly
- [ ] No public types in `*.Internal.*` namespaces
- [ ] All concrete classes are sealed (unless explicitly excepted)
- [ ] No optional/default parameters — only method overloads
- [ ] No suppressed warnings without `#pragma` justification

## Appendix: NuGet Package Dependencies

### Main Project (`src/MicroStack/MicroStack.csproj`)
- `Microsoft.AspNetCore.App` (framework reference — built-in)
- `Docker.DotNet` — Docker SDK for RDS, ECS, ElastiCache, Lambda container images
- `System.Formats.Cbor` — for Smithy RPC v2 CBOR protocol (CloudWatch)
- `YamlDotNet` — for CloudFormation YAML template parsing

### Test Project (`tests/MicroStack.Tests/MicroStack.Tests.csproj`)
- `Microsoft.AspNetCore.Mvc.Testing` — for `WebApplicationFactory<T>`
- `xunit` + `xunit.runner.visualstudio`
- `AWSSDK.S3`, `AWSSDK.SQS`, `AWSSDK.SimpleNotificationService`, `AWSSDK.DynamoDBv2`
- `AWSSDK.Lambda`, `AWSSDK.IdentityManagement`, `AWSSDK.SecurityToken`
- `AWSSDK.SecretsManager`, `AWSSDK.SimpleSystemsManagement`, `AWSSDK.KeyManagementService`
- `AWSSDK.CloudWatch`, `AWSSDK.CloudWatchLogs`
- `AWSSDK.EC2`, `AWSSDK.ElasticLoadBalancingV2`, `AWSSDK.Route53`, `AWSSDK.CertificateManager`
- `AWSSDK.ECS`, `AWSSDK.RDS`, `AWSSDK.ElastiCache`, `AWSSDK.ECR`
- `AWSSDK.CloudFormation`, `AWSSDK.CognitoIdentityProvider`, `AWSSDK.CognitoIdentity`
- `AWSSDK.EventBridge`, `AWSSDK.Kinesis`, `AWSSDK.KinesisFirehose`
- `AWSSDK.Glue`, `AWSSDK.Athena`, `AWSSDK.StepFunctions`
- `AWSSDK.SimpleEmail`, `AWSSDK.SimpleEmailV2`
- `AWSSDK.WAFv2`, `AWSSDK.ElasticFileSystem`, `AWSSDK.ElasticMapReduce`
- `AWSSDK.AppSync`, `AWSSDK.CloudFront`, `AWSSDK.ServiceDiscovery`
- `AWSSDK.APIGateway`, `AWSSDK.ApiGatewayV2`
- `AWSSDK.RDSDataService`

## Appendix: Protocol Mapping Reference

| Protocol | Services | C# Pattern |
|----------|----------|------------|
| AWS Query/XML (Action param) | SQS, SNS, IAM, STS, EC2, RDS, ElastiCache, CloudFormation, CloudWatch Metrics, SES v1, ALB/ELBv2 | Parse `Action` from query string or form body, switch-dispatch, return XML |
| AWS JSON (X-Amz-Target) | DynamoDB, Lambda (partial), SecretsManager, CloudWatch Logs, SSM, EventBridge, Kinesis, Firehose, Step Functions, ECS, ECR, Glue, Athena, Cognito, KMS, WAF, EMR, AppSync, ServiceDiscovery, ACM | Parse target header, deserialize JSON body, switch-dispatch, return JSON |
| REST/JSON (path-based) | Lambda, API Gateway v1/v2, Route53, CloudFront, EFS, AppSync, SES v2, RDS Data API | Match URL path patterns, deserialize JSON body, return JSON |
| REST/XML (path-based) | S3, Route53, CloudFront | Match URL path + HTTP method, parse XML body when present, return XML |
| Smithy RPC v2 CBOR | CloudWatch Metrics (alternate) | Parse CBOR body via `System.Formats.Cbor`, return CBOR |
