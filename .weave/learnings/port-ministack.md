# Learnings: Port MiniStack

## Task 11: Foundation Integration Tests

- **Discrepancy**: Plan said DI couldn't construct `ServiceRegistry`/`StatePersistence` due to internal constructors. That was correct and fixed with factory lambdas in `AddSingleton`. But two additional issues were not anticipated:
  1. `AwsRequestMiddleware` also needs a **public constructor** and **public `InvokeAsync`** method — ASP.NET Core's `UseMiddleware<T>` convention requires both to be public (not just `internal`).
  2. `UseMiddleware<AwsRequestMiddleware>()` was registered before `UseRouting()`, so the AWS middleware intercepted all requests including the admin health/reset endpoints, routing them to the service dispatcher which returned 400.
  3. OPTIONS pre-flight requests were matched by the routing engine as "405 Method Not Allowed" before our CORS handler could run.
- **Resolution**:
  - Made `AwsRequestMiddleware` constructor and `InvokeAsync` method `public` with comments explaining the ASP.NET Core middleware convention requirement.
  - Added `app.UseRouting()` before `UseMiddleware<AwsRequestMiddleware>()`.
  - Added `app.UseEndpoints(_ => {})` after the AWS middleware to activate mapped endpoints.
  - Added a short-circuit `app.Use(...)` middleware between `UseRouting()` and `UseMiddleware<AwsRequestMiddleware>()` to handle OPTIONS pre-flight before the routing engine emits 405.
  - Added `context.GetEndpoint() is not null → _next(context)` check in `AwsRequestMiddleware.InvokeAsync` to skip AWS dispatch for matched admin endpoints.
  - Fixed the CORS OPTIONS test assertion to only check `response.Headers` (not `response.Content.Headers`).
- **Suggestion**: The plan should note that ASP.NET Core middleware convention requires **public** constructor and **public** `Invoke`/`InvokeAsync`. Also note the middleware ordering: `UseRouting()` → OPTIONS short-circuit → `UseMiddleware<T>` → `UseEndpoints()`.

## Task 12: SQS Service Handler

- **Discrepancy**: The `CreateSqsClient` method had broken code from a failed prior edit (doubly-nested `AmazonSQSClient` with a nonsensical ternary). The `CanonicalizeUriHandler` class was missing entirely.
- **Resolution**: Rewrote `CreateSqsClient` to: (1) get `Server.CreateHandler()`, (2) wrap in a new `CanonicalizeUriHandler` (`DelegatingHandler` that does `new Uri(uri.AbsoluteUri)` to strip the dangerous flag), (3) create `HttpClient` with that handler, (4) pass via `AmazonSQSConfig.HttpClientFactory = new FixedHttpClientFactory(httpClient)`. Added `CanonicalizeUriHandler` class to the test file.
- **Discrepancy**: Three tests failed after the client fix: (1) `ReceiveMessageWithSystemAttributes` — NullReferenceException; (2) `FifoQueueDeduplication` — MD5 hash mismatch; (3) `DeadLetterQueueRedrive` — DLQ count was 0.
- **Resolution**:
  1. **System attributes**: AWS SDK v4 JSON protocol sends `MessageSystemAttributeNames` (not `AttributeNames`). Fixed by checking `data["AttributeNames"] ?? data["MessageSystemAttributeNames"]` in `ActReceiveMessageAsync`.
  2. **FIFO dedup MD5**: For duplicate sends, the SDK validates `MD5OfMessageBody` against the **current** request body. We were returning the cached MD5 (original body). Fixed by computing `MD5` of the current `bodyText` for dedup responses.
  3. **DLQ sweep**: `ActReceiveMessageAsync` was calling the empty stub `DlqSweep(q)` instead of `DlqSweepWithQueues(q)`. Removed the stub, renamed call site to use the real method.
- **Suggestion**: The plan should note that AWS SDK v4 JSON protocol uses `MessageSystemAttributeNames` (not `AttributeNames`) for system attributes. Also note that for FIFO dedup responses the SDK still validates MD5 against the sent body, so the server must return MD5 of the current request body, not the cached original.

## Task 13: DynamoDB Service Handler

- **Discrepancy**: `DynamoDbServiceHandler` was already implemented but not registered in `Program.cs` and not tested. Handler compiled but had several correctness issues discovered during testing.
- **Resolution**:
  1. **Program.cs registration**: Added `using MicroStack.Services.DynamoDb;` and `registry.Register(new DynamoDbServiceHandler());`.
  2. **AWSSDK.DynamoDBv2**: Added to test project as `Version="4.*"`.
  3. **SDK v4 API differences**: `ScanAsync`, `DescribeContinuousBackupsAsync` — no convenience string overloads in v4; must use explicit request objects. `GetItemResponse.Item` is **null** (not empty dict) when item not found — use `Assert.Null` not `Assert.Empty`. `ScanResponse.LastEvaluatedKey` is **null** (not empty dict) when no more pages — use `?.Count > 0` not `.Count > 0`.
  4. **TransactionCanceledException**: `TransactCancelResponse` was returning a `JsonObject` that got wrapped in 200 OK — SDK never saw the error. Fixed: replaced with `ThrowTransactCanceled` that throws `DdbException` with a `RawBody` property; `HandleAsync` detects `RawBody` and sends it verbatim as the HTTP error body at status 400.
  5. **`ResolveKeysStrict` validation**: Was not validating missing sort key, wrong key type, or extra key attributes. Added validation using `GetKeyAttrType`+`NodeMatchesType` helpers that throw `ValidationException` when key schema mismatches.
  6. **BOOL filter comparison**: `CompareDdb` switch matched on `(string, string)` and `(decimal, decimal)` but not `(bool, bool)`. Items with `BOOL` attributes returned 0 matches on `= :t` filter. Fixed by adding `("=", bool lb, bool rb)` and `("<>", bool lb, bool rb)` cases.
- **Suggestion**: Plan should note that `TransactionCanceledException` requires returning HTTP 400 with `__type=TransactionCanceledException` plus `CancellationReasons` array — it's not a simple `DdbException`. The `ResolveKeysStrict` needs key type validation (not just extraction) to match real DynamoDB behavior. BOOL comparison must be handled explicitly in the comparator.

## Task 14: S3 Service Handler

- **Architecture**: S3 uses REST/XML protocol (not JSON like DynamoDB or Query/XML like SQS). Path-based routing: `/{bucket}` and `/{bucket}/{key}` with query param sub-resource dispatch (e.g. `?tagging`, `?versioning`, `?uploads`).
- **SDK v4 differences**:
  1. `AmazonS3Config.ForcePathStyle = true` is required — otherwise SDK uses virtual-hosted bucket style which complicates routing.
  2. `NoSuchKeyException` is a separate exception type (not `AmazonS3Exception`) — tests must catch the right type.
  3. `DeleteObjectsException` is thrown by the SDK when batch delete has errors — not the same as `AmazonS3Exception`.
  4. SDK v4 swallows 404 errors on some operations (encryption, lifecycle, website, replication delete-then-get) — tests need try/catch patterns instead of simple `Assert.ThrowsAsync`.
  5. `TaggingDirective` header is NOT sent by SDK v4 on `CopyObject` — must use separate `PutObjectTagging` after copy to replace tags.
  6. `list.Buckets` and `response.S3Objects` can be null (not empty list) — always use `?? []` or null-conditional.
- **Skipped tests** (to revisit after SNS/EventBridge are ported):
  - `test_s3_event_notification_to_sqs` — S3→SQS notification
  - `test_s3_event_notification_filter` — S3→SQS with key filter
  - `test_s3_event_notification_delete` — S3→SQS on delete
  - `test_s3_eventbridge_notification` — S3→EventBridge→SQS
  - `test_s3_eventbridge_notification_on_delete` — S3→EventBridge on delete
  - `test_s3_event_to_sqs` — S3→SQS create+delete events
  - `test_s3_put_get_json_chunked` — raw HTTP chunked encoding (covered by middleware)
  - `test_s3_control_*` — S3 Control API tests (separate handler)
- **Suggestion**: Event notification tests should be added once SNS and EventBridge handlers exist. The S3 handler has stub points for `_fire_s3_event_async` but no implementation — will need cross-service wiring.

## Task 15: SNS Service Handler

- **SNS→SQS fanout architecture**: The Python code directly accesses `_sqs._queues` and `_sqs._queue_url` to inject messages. In the C# port, we added an `InjectMessage(queueName, body, groupId, dedupId)` internal method on `SqsServiceHandler` — cleaner encapsulation while still allowing direct in-process delivery. The `SnsServiceHandler` constructor takes a `SqsServiceHandler` reference for this purpose.
- **AWSSDK.SimpleNotificationService v4**: 
  1. `ListTopicsResponse.Topics` can be **null** (not empty list) when no topics exist — use `?? []`.
  2. `ListSubscriptionsByTopicResponse.Subscriptions` can be **null** — same pattern.
  3. `PublishBatchResponse.Successful` and `.Failed` can be **null** — use `?.Count ?? 0` and `?? []`.
  4. `ReceiveMessageResponse.Messages` can be **null** from SQS SDK — same SDK v4 null-collection pattern.
  5. `NotFoundException` is NOT thrown directly — the SDK throws `AmazonSimpleNotificationServiceException` with `ErrorCode = "NotFoundException"`. Use the broader exception type in `Assert.ThrowsAsync`.
  6. `BatchEntryIdsNotDistinctException` exists in both `Amazon.SQS.Model` and `Amazon.SimpleNotificationService.Model` — need fully-qualified type name to disambiguate. Same for `MessageAttributeValue`.
- **SNS→Lambda fanout**: Stubbed (no-op) in the SNS handler. Will be wired up when the Lambda handler is ported (Task 17+). Two Python tests skipped: `test_sns_to_lambda_fanout`, `test_sns_to_lambda_event_subscription_arn`.

## Task 17: IAM/STS Service Handler

- **IAM/STS shared state architecture**: IAM and STS are separate service handlers (separate `ServiceName` = "iam" / "sts"), but STS needs access to IAM's state (roles, access keys). Solved by passing `IamServiceHandler` reference to `StsServiceHandler` constructor (same pattern as SNS→SQS). IAM exposes `internal` accessors for `Roles` and `AccessKeys` properties.
- **User inline policies composite key**: Python uses tuple key `(user_name, policy_name)` for `_user_inline_policies`. Since `AccountScopedDictionary` requires `TKey : notnull`, used string composite key `$"{userName}\0{policyName}"` with null character as separator. Iteration splits on `\0` to extract user/policy names.
- **Unsigned STS requests routing**: AWS SDK v4 sends `AssumeRoleWithWebIdentity` without an `Authorization` header (anonymous auth). The credential scope detection in `AwsServiceRouter` failed, causing the request to fall through to the S3 default handler (returning 405). Fix: added form body `Action` parsing in `AwsRequestMiddleware` for unsigned form-encoded requests — merges the `Action` into routing params so the router can detect the service correctly.
- **AWSSDK.IdentityManagement v4**: Uses `NoSuchEntityException` (specific exception type) for IAM "not found" errors. `GetPolicyVersionResponse.PolicyVersion.Document` is URL-encoded — use `Uri.UnescapeDataString` before asserting content. `GetRolePolicyResponse.PolicyDocument` and `GetUserPolicyResponse.PolicyDocument` are also URL-encoded. All operations require explicit Request objects (no convenience overloads with plain strings).
- **AWSSDK.SecurityToken v4**: JSON protocol used via `X-Amz-Target: AWSSecurityTokenServiceV20110615.ActionName`. JSON Expiration is a Unix timestamp (epoch seconds), XML Expiration is ISO 8601. `AssumeRoleWithWebIdentityRequest.WebIdentityToken` is required.

## Task 18: Secrets Manager Service Handler

- **JSON protocol pattern**: Secrets Manager uses `X-Amz-Target: secretsmanager.ActionName`. Request body is JSON, responses are JSON. Used `AwsResponseHelpers.JsonResponse()` and `AwsResponseHelpers.ErrorResponseJson()` — straightforward compared to XML-based services.
- **Version staging logic**: The `ApplyCurrentPromotion` helper is the most complex piece — promotes a version to AWSCURRENT, demotes old CURRENT→PREVIOUS, and prunes stageless versions. `UpdateSecretVersionStage` has elaborate validation: must specify `RemoveFromVersionId` when moving an already-attached label.
- **BatchGetSecretValue**: Calls internal `GetSecretValueInternal` (not `HandleAsync`) to avoid re-parsing. Response includes both `SecretValues` (successes) and `Errors` (failures with ErrorCode/Message).
- **GetRandomPassword**: Uses `System.Security.Cryptography.RandomNumberGenerator` for secure random. Builds character pools and optionally requires one character from each included type.
- **AWSSDK.SecretsManager v4**: `CreateSecretResponse.VersionId` returned directly. `DescribeSecretResponse.VersionIdsToStages` is a `Dictionary<string, List<string>>`. `ListSecretVersionIdsResponse.Versions` can be null — use `?? []`. `GetSecretValueResponse.SecretBinary` is a `MemoryStream` (not `byte[]`) — use `.ToArray()` for comparison.

## Task 27-28: Step Functions Service Handler + Tests

- **Internal value wrappers**: Python's dynamic typing allows `currentInput` in the execution loop to be any type (dict, list, string, etc.). C#'s `Dictionary<string, object?>` constraint requires wrapper conventions:
  - `__scalar__` — wraps non-dict, non-list scalar Results from Pass state (e.g., `{"__scalar__": "enriched"}`)
  - `__list__` — wraps List results from Parallel/Map states (e.g., `{"__list__": [item1, item2]}`)
  - `UnwrapForSerialization(object?)` recursively strips these wrappers before JSON serialization
  - `ApplyResultPath` unwraps both `__scalar__` and `__list__` before applying to paths (so `ResultPath: "$.status"` with scalar `"enriched"` sets `output["status"] = "enriched"` directly)
  - `ApplyResultPathRaw("$", ...)` re-wraps scalars as `__scalar__` and lists as `__list__` to maintain the `Dictionary<string, object?>` return type
- **PostAsJsonAsync camelCase**: `HttpClient.PostAsJsonAsync` uses `JsonSerializerDefaults.Web` in .NET 10 which camelCases property names. Mock config endpoint expects PascalCase keys (StateMachines, TestCases, MockedResponses). Solution: send raw JSON strings with explicit property names instead of using anonymous types.
- **Config endpoint nested properties**: The `/_ministack/config` endpoint receives `{"stepfunctions": {"_sfn_mock_config": {...}}}` as a nested object, NOT a dotted property name. Must navigate `doc.RootElement.TryGetProperty("stepfunctions", out var sfnEl)` then `sfnEl.TryGetProperty("_sfn_mock_config", out var mockEl)`.
- **SNS protocol mismatch**: SFN's `DispatchToService` sends JSON body with `x-amz-target: SNS.Publish`, but the SNS handler expects query/XML (form-encoded) protocol. Integration test skipped.
- **AWS SDK v4 type ambiguity**: `Tag`, `TagResourceRequest`, `UntagResourceRequest` are ambiguous between StepFunctions and DynamoDBv2 namespaces — use type aliases (`SfnTag`, `SfnTagResourceRequest`, `SfnUntagResourceRequest`).
- **xUnit2012**: Don't use `Assert.True(collection.Any(...))` — use `Assert.Contains(collection, predicate)` instead.
