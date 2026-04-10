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
