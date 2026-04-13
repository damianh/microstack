# Native AOT Migration for MicroStack

## TL;DR
> **Summary**: Make MicroStack fully Native AOT compatible by eliminating all `object?`-typed serialization paths — changing `IServiceHandler.GetState()/RestoreState()` to `JsonElement`, replacing `Dictionary<string, object?>` serialization in CloudFormation/StepFunctions/Lambda with `JsonObject`/`JsonNode`, fixing anonymous types in Program.cs, and enabling `<PublishAot>true</PublishAot>`.
> **Estimated Effort**: XL

## Context
### Original Request
Make MicroStack (.NET 10 ASP.NET Core app emulating 39 AWS services) compatible with Native AOT publishing, eliminating all trim/AOT warnings and verifying all 1,178 integration tests pass.

### Key Findings

**Handlers with real `GetState()`/`RestoreState()` implementations (5 of 39):**
- `SecretsManagerServiceHandler` — persists `_secrets` + `_resourcePolicies` via `AccountScopedDictionary.ToRaw()`
- `SsmServiceHandler` — persists `_parameters` + `_parameterHistory` + `_tags` via `ToRaw()`
- `SnsServiceHandler` — persists `_topics` + `_subArnToTopic` + `_platformApps` + `_platformEndpoints` via `ToRaw()`
- `SqsServiceHandler` — persists `_queues` + `_queueNameToUrl` via `ToRaw()`
- `S3ServiceHandler` — returns `null` (stub)

**The remaining 34 handlers** return `null` from `GetState()` and have empty `RestoreState()` bodies.

**AOT blockers identified:**

1. **`IServiceHandler.GetState()` returns `object?`** — serialized via `JsonSerializer.Serialize(state)` in `StatePersistence.SaveState()`. The 5 active handlers build `Dictionary<string, object?>` containing `AccountScopedDictionary.ToRaw()` results (which are `IReadOnlyDictionary<(string, TKey), TValue>`). This is the primary AOT blocker because the serializer cannot know the concrete types at compile time.

2. **`StatePersistence.LoadState()` deserializes to `object`** — `JsonSerializer.Deserialize<object>(json)` returns `JsonElement` at runtime but the cast chain in `RestoreState()` expects `Dictionary<string, object?>` with typed values inside. The current persistence system relies on the runtime's ability to round-trip object graphs via `System.Text.Json`, which actually breaks even today (deserializing returns `JsonElement`, not `Dictionary<string, object?>`). **This means persistence is effectively broken for the 5 handlers that implement it** — the `RestoreState` `is not Dictionary<string, object?>` check will fail on deserialized data.

3. **`AwsResponseHelpers.JsonResponse(object data)` accepts `object`** — called ~438 times across all handlers with `Dictionary<string, object?>`. Since all callers pass dictionaries with string/number/bool/null/list/dict values, this serializes fine at runtime, but AOT needs type info. However, `Dictionary<string, object?>` is actually handled by STJ's default converters without source generators. The real issue is when values contain non-primitive nested types.

4. **CloudFormation provisioners** — 30+ methods return `(string, Dictionary<string, object?>)` where the dict values are strings. The serialization calls (`JsonSerializer.Serialize(createReq)` on line 181, 251, etc.) serialize `Dictionary<string, object?>` where values are strings, ints, bools, or nested `Dictionary<string, object?>`/`List<object?>`. These are AOT-unsafe because STJ doesn't know the concrete value types.

5. **StepFunctions handler** — ~200 uses of `Dictionary<string, object?>` as a dynamic JSON object model, including recursive `UnwrapForSerialization()`, `ParseJsonToDict()`, `JsonElementToDict()`, and `SerializeOutput()`. The entire ASL execution engine operates on `Dictionary<string, object?>` graphs. This is the largest and most complex AOT blocker.

6. **Lambda worker** — `InjectRequestId()` and init payload use `Dictionary<string, object?>` serialization. `DeserializeJsonElement()` converts `JsonElement` → `object?` graph.

7. **Program.cs** — 3 anonymous types in `Results.Ok(new { ... })`.

8. **Program.cs `/_ministack/config`** — `JsonElementToDict()` / `JsonElementToObject()` helper converts `JsonElement` → `Dictionary<string, object?>` → passed to `sfnHandler.SetMockConfig()`.

**Critical insight about the `Dictionary<string, object?>` pattern:**

The codebase has two distinct uses of `Dictionary<string, object?>`:

- **As in-memory state** (StepFunctions state machines, executions, Lambda ESMs, CloudFormation stacks, ApiGateway resources) — these are used as dynamic property bags, read/written by key, values compared as strings/ints/bools. Converting these to `JsonNode` would change hundreds of accessor patterns.

- **As serialization payloads** — building JSON bodies to send to other handlers via `HandleAsync()`, or building JSON response bodies. These only need to produce a JSON string/byte[], so they can use `JsonObject` or `Utf8JsonWriter`.

**The pragmatic approach:** The `Dictionary<string, object?>` in-memory model throughout StepFunctions, Lambda ESMs, ApiGateway V1, and CloudFormation is deeply embedded (~800+ lines in StepFunctions alone). Converting all of these to `JsonNode` is a massive refactor that would touch virtually every line. Instead, we should:

1. Keep `Dictionary<string, object?>` as the in-memory model where values are known primitives (string, long, double, bool, null, nested dict/list).
2. Register `Dictionary<string, object?>` in a `JsonSerializerContext` with a custom converter that handles the known primitive value types.
3. Replace the specific `JsonSerializer.Serialize(object)` calls with typed overloads that use the source-generated context.

### AOT Strategy

**Use `JsonSerializerContext` with a custom `DictionaryObjectConverter`** that handles `Dictionary<string, object?>` serialization by walking the known value types (string, long, double, bool, null, `Dictionary<string, object?>`, `List<object?>`). This avoids rewriting the entire StepFunctions/CloudFormation/Lambda/ApiGateway codebase while making all serialization AOT-safe.

For `IServiceHandler.GetState()/RestoreState()`, change to `JsonElement?`/`JsonElement` — this fixes the persistence interface cleanly and makes `StatePersistence` trivially AOT-safe (it just reads/writes `JsonElement` which is already a known type).

## Objectives
### Core Objective
Enable Native AOT publishing for MicroStack with zero trim warnings and all 1,178 tests passing.

### Deliverables
- [ ] `<PublishAot>true</PublishAot>` in csproj with AOT container base image
- [ ] `IServiceHandler` interface changed to `JsonElement?` / `JsonElement` for state
- [ ] `StatePersistence` updated for `JsonElement` round-trip
- [ ] `MicroStackJsonContext` source-generated serializer context
- [ ] Custom `DictionaryObjectJsonConverter` for `Dictionary<string, object?>` AOT serialization
- [ ] All 39 handlers updated for new interface signature
- [ ] 5 handlers with real state persistence updated to produce/consume `JsonElement`
- [ ] `AwsResponseHelpers.JsonResponse` updated to use context
- [ ] All `JsonSerializer.Serialize(dict)` calls updated to use context
- [ ] Program.cs anonymous types replaced with named records
- [ ] All 1,178 tests pass
- [ ] `dotnet publish` with AOT produces zero trim warnings

### Definition of Done
- [ ] `dotnet publish -c Release` succeeds with no warnings (run from `src/MicroStack/`)
- [ ] `dotnet test` passes all 1,178 tests (run from `tests/MicroStack.Tests/`)
- [ ] `dotnet publish -c Release -r win-x64` (or appropriate RID) produces a native binary

### Guardrails (Must NOT)
- Must NOT use MVC controllers
- Must NOT suppress warnings without `#pragma` justification
- Must NOT break any existing tests
- Must NOT change the wire protocol (JSON/XML responses must be byte-identical)
- Must NOT introduce reflection-based serialization paths
- Must NOT use `dynamic` or expression trees

## TODOs

### Phase 1: AOT Infrastructure + Interface Change

- [x] 1. Enable AOT in csproj and update container base image
  **What**: Add `<PublishAot>true</PublishAot>`, `<TrimMode>full</TrimMode>`, `<InvariantGlobalization>false</InvariantGlobalization>`, and change container base image from `aspnet:10.0-alpine` to `runtime-deps:10.0-alpine` (AOT is self-contained). Keep existing `ContainerRuntimeIdentifiers`.
  **Files**: `src/MicroStack/MicroStack.csproj`
  **Acceptance**: csproj has AOT properties; `dotnet build` still succeeds (AOT warnings expected at this stage)

- [x] 2. Change `IServiceHandler.GetState()` and `RestoreState()` signatures
  **What**: Change `object? GetState()` → `JsonElement? GetState()` and `void RestoreState(object state)` → `void RestoreState(JsonElement state)`. This is the interface-level change.
  **Files**: `src/MicroStack/IServiceHandler.cs`
  **Acceptance**: Interface compiles; all implementors will need updating (next tasks)

- [x] 3. Update `StatePersistence` for `JsonElement` round-trip
  **What**: `SaveState` receives `JsonElement`, writes its raw text directly to file. `LoadState` reads JSON text and parses to `JsonElement` via `JsonDocument.Parse()`. Remove the `JsonSerializerOptions` field (no longer needed for state persistence). The `SaveState` signature becomes `SaveState(string serviceName, JsonElement state)` and `LoadState` returns `JsonElement?`.
  **Files**: `src/MicroStack/Internal/StatePersistence.cs`
  **Acceptance**: `StatePersistence` compiles with no `JsonSerializer.Serialize(object)` or `Deserialize<object>` calls

- [x] 4. Update 34 null-returning handlers for new interface signature
  **What**: For the 34 handlers that return `null` from `GetState()` and have empty `RestoreState()`, change signatures to match new interface: `public JsonElement? GetState() => null;` and `public void RestoreState(JsonElement state) { }`. This is a mechanical find-and-replace.
  **Files**: All 34 handler files listed below:
    - `src/MicroStack/Services/Ec2/Ec2ServiceHandler.cs`
    - `src/MicroStack/Services/Alb/AlbServiceHandler.cs`
    - `src/MicroStack/Services/CloudWatch/CloudWatchServiceHandler.cs`
    - `src/MicroStack/Services/Rds/RdsServiceHandler.cs`
    - `src/MicroStack/Services/Ecr/EcrServiceHandler.cs`
    - `src/MicroStack/Services/ElastiCache/ElastiCacheServiceHandler.cs`
    - `src/MicroStack/Services/CloudFormation/CloudFormationServiceHandler.cs`
    - `src/MicroStack/Services/ApiGateway/ApiGatewayV2ServiceHandler.cs`
    - `src/MicroStack/Services/DynamoDb/DynamoDbServiceHandler.cs`
    - `src/MicroStack/Services/Iam/IamServiceHandler.cs`
    - `src/MicroStack/Services/Kms/KmsServiceHandler.cs`
    - `src/MicroStack/Services/Lambda/LambdaServiceHandler.cs`
    - `src/MicroStack/Services/StepFunctions/StepFunctionsServiceHandler.cs`
    - `src/MicroStack/Services/Acm/AcmServiceHandler.cs`
    - `src/MicroStack/Services/CloudWatchLogs/CloudWatchLogsServiceHandler.cs`
    - `src/MicroStack/Services/Ecs/EcsServiceHandler.cs`
    - `src/MicroStack/Services/EventBridge/EventBridgeServiceHandler.cs`
    - `src/MicroStack/Services/Kinesis/KinesisServiceHandler.cs`
    - `src/MicroStack/Services/Firehose/FirehoseServiceHandler.cs`
    - `src/MicroStack/Services/Glue/GlueServiceHandler.cs`
    - `src/MicroStack/Services/Athena/AthenaServiceHandler.cs`
    - `src/MicroStack/Services/Ses/SesServiceHandler.cs`
    - `src/MicroStack/Services/Waf/WafServiceHandler.cs`
    - `src/MicroStack/Services/Efs/EfsServiceHandler.cs`
    - `src/MicroStack/Services/Emr/EmrServiceHandler.cs`
    - `src/MicroStack/Services/AppSync/AppSyncServiceHandler.cs`
    - `src/MicroStack/Services/ServiceDiscovery/ServiceDiscoveryServiceHandler.cs`
    - `src/MicroStack/Services/Cognito/CognitoIdpServiceHandler.cs`
    - `src/MicroStack/Services/Cognito/CognitoIdentityServiceHandler.cs`
    - `src/MicroStack/Services/S3Files/S3FilesServiceHandler.cs`
    - `src/MicroStack/Services/S3/S3ServiceHandler.cs`
    - `src/MicroStack/Services/CloudFront/CloudFrontServiceHandler.cs`
    - `src/MicroStack/Services/RdsData/RdsDataServiceHandler.cs`
    - `src/MicroStack/Services/Route53/Route53ServiceHandler.cs`
    - `src/MicroStack/Services/Sts/StsServiceHandler.cs`
  **Acceptance**: All 34 handlers compile with new signature; `using System.Text.Json;` added where needed

- [x] 5. Update 5 stateful handlers to produce/consume `JsonElement`
  **What**: For the 5 handlers with real `GetState()`/`RestoreState()` implementations, each handler's `GetState()` must serialize its `AccountScopedDictionary` data into a `JsonElement` internally, and `RestoreState(JsonElement)` must parse the `JsonElement` back. The approach for each:

  **Pattern**: In `GetState()`, use `JsonSerializer.SerializeToElement()` (or serialize to bytes then `JsonDocument.Parse`) to convert the state dictionary to `JsonElement`. In `RestoreState()`, parse the `JsonElement` properties back into the `AccountScopedDictionary` using `FromRaw()`.

  **Note on `AccountScopedDictionary.ToRaw()`**: `ToRaw()` returns `IReadOnlyDictionary<(string AccountId, TKey Key), TValue>`. The tuple key `(string, TKey)` needs a custom serialization strategy since STJ cannot serialize tuple keys by default. Each handler should serialize using a flat structure like: `{ "entries": [ { "accountId": "123", "key": "name", "value": {...} }, ... ] }`. This requires adding `ToJsonElement()` and `FromJsonElement()` helper methods to `AccountScopedDictionary<TKey, TValue>`, or having each handler implement its own serialization.

  **Recommended approach**: Add generic `SerializeToJsonElement()` / `RestoreFromJsonElement()` methods to `AccountScopedDictionary` that use a `JsonSerializerContext` or explicit `JsonSerializerOptions` with a type info resolver. Alternatively, each handler serializes its own dictionaries with explicit type knowledge.

  **Handlers to update**:
  - `SecretsManagerServiceHandler` — keys are `string`, values are `SmSecret`, `string`
  - `SsmServiceHandler` — keys are `string`, values are `SsmParameter`, `List<SsmHistoryEntry>`, `Dictionary<string, string>`
  - `SnsServiceHandler` — keys are `string`, values are `SnsTopic`, `string`, `SnsPlatformApp`, `SnsPlatformEndpoint`
  - `SqsServiceHandler` — keys are `string`, values are `SqsQueue`, `string`
  - `S3ServiceHandler` — already returns `null`, just update signature

  **Files**:
    - `src/MicroStack/Services/SecretsManager/SecretsManagerServiceHandler.cs`
    - `src/MicroStack/Services/Ssm/SsmServiceHandler.cs`
    - `src/MicroStack/Services/Sns/SnsServiceHandler.cs`
    - `src/MicroStack/Services/Sqs/SqsServiceHandler.cs`
    - `src/MicroStack/Internal/AccountScopedDictionary.cs` (if adding serialization helpers)
  **Acceptance**: 5 handlers compile; state round-trip tested via existing persistence tests

- [x] 6. Fix Program.cs anonymous types and `Dictionary<string, object>` usage
  **What**: Replace 3 anonymous types with named record types:
  - Health endpoint: `Results.Ok(new { services, edition, version })` → named record `HealthResponse(Dictionary<string, string> Services, string Edition, string Version)`
  - Reset endpoint: `Results.Ok(new { reset = "ok" })` → named record `ResetResponse(string Reset)`
  - Config endpoint: `Results.Ok(new { applied })` → named record `ConfigResponse(Dictionary<string, object> Applied)` (or `Dictionary<string, string>` since values are always strings like `"applied"`)

  Also fix line 147: `var applied = new Dictionary<string, object>();` → use `Dictionary<string, string>` since only string values are stored.

  Also fix `AwsResponseHelpers.ErrorResponseJson` which uses `new { __type = code, message }` — replace with named type.

  The `JsonElementToDict()` / `JsonElementToObject()` static local functions at the bottom of Program.cs (lines 171-199) produce `Dictionary<string, object?>` which is passed to `sfnHandler.SetMockConfig()`. Since StepFunctions keeps `Dictionary<string, object?>` internally, this stays as-is but needs the custom converter (Phase 2).

  **Files**:
    - `src/MicroStack/Program.cs`
    - `src/MicroStack/Internal/AwsResponseHelpers.cs`
  **Acceptance**: No anonymous types remain in Program.cs or AwsResponseHelpers; all replaced with `sealed record` types; project compiles

### Phase 2: Create AOT-Safe Serialization Infrastructure

- [x] 7. Create `MicroStackJsonContext` source-generated serializer context
  **What**: Create a `JsonSerializerContext` that registers all types used in `JsonSerializer.Serialize()` / `Deserialize()` calls across the codebase. Key types to register:
  - `Dictionary<string, object?>` (with custom converter — see task 8)
  - `Dictionary<string, string>`
  - `Dictionary<string, string[]>`
  - `List<object?>`
  - `List<Dictionary<string, object?>>`
  - `List<Dictionary<string, string>>`
  - Response record types from task 6 (HealthResponse, ResetResponse, ConfigResponse, ErrorResponse)
  - Any other concrete types passed to `JsonSerializer.Serialize()`

  Mark with `[JsonSerializable]` attributes. Set `GenerationMode = JsonSourceGenerationMode.Default` to support both serialization and deserialization.

  **Files**: `src/MicroStack/Internal/MicroStackJsonContext.cs` (new file)
  **Acceptance**: Context class compiles; `dotnet build` generates source for all registered types

- [x] 8. Create `DictionaryObjectJsonConverter` for AOT-safe `Dictionary<string, object?>` serialization
  **What**: Create a custom `JsonConverter<Dictionary<string, object?>>` that handles serialization/deserialization of `Dictionary<string, object?>` where values are constrained to the known primitive types used throughout the codebase: `string`, `long`, `int`, `double`, `bool`, `null`, `Dictionary<string, object?>`, `List<object?>`, `List<Dictionary<string, object?>>`, `List<Dictionary<string, string>>`.

  Serialization: Walk each value, switch on type, call the appropriate `Utf8JsonWriter` method.
  Deserialization: Read JSON tokens, map to C# types (object → dict, array → list, string → string, number → long/double, bool → bool, null → null).

  Also create a matching `ListObjectJsonConverter` for `List<object?>`.

  This converter replaces the reflection-based serialization that STJ uses at runtime.

  **Files**: `src/MicroStack/Internal/DictionaryObjectJsonConverter.cs` (new file)
  **Acceptance**: Converter handles all value types; registered in `MicroStackJsonContext`; round-trip tests pass for representative payloads

- [x] 9. Update `AwsResponseHelpers.JsonResponse` to use source-generated context
  **What**: Change `JsonResponse(object data, int statusCode = 200)` to accept `Dictionary<string, object?>` (the actual type all callers pass) and serialize using `MicroStackJsonContext.Default`. Or add an overload that takes the context.

  Also update `ErrorResponseJson` to use the named record type from task 6 instead of anonymous type.

  **Files**: `src/MicroStack/Internal/AwsResponseHelpers.cs`
  **Acceptance**: No `JsonSerializer.Serialize(object)` calls remain; all use typed context

- [x] 10. Update all `JsonSerializer.Serialize(dict)` calls to use context
  **What**: Across the codebase, find all calls to `JsonSerializer.Serialize()` or `JsonSerializer.SerializeToUtf8Bytes()` that pass `Dictionary<string, object?>` or other untyped data, and update them to use `MicroStackJsonContext.Default` options (or the context directly). Key locations:

  **CloudFormation provisioners** (`CloudFormationProvisioners.cs`): ~15 `JsonSerializer.Serialize(createReq)` calls where `createReq` is `Dictionary<string, object?>`. Update to `JsonSerializer.Serialize(createReq, MicroStackJsonContext.Default.DictionaryStringObject)` or pass `MicroStackJsonContext.Default.Options`.

  **StepFunctions handler** (`StepFunctionsServiceHandler.cs`):
  - `SerializeOutput()` line 168: `JsonSerializer.Serialize(UnwrapForSerialization(data))` — the `UnwrapForSerialization` returns `object?`, update to pass context options.
  - `FailExecution()` line 1298: `JsonSerializer.Serialize(new Dictionary<string, object?> {...})` — pass context.
  - `CallLambda()` line 2576: `JsonSerializer.SerializeToUtf8Bytes(eventData)` — pass context.
  - `DispatchToService()` line 2769: `JsonSerializer.SerializeToUtf8Bytes(inputData)` — pass context.
  - `InvokeNestedStartExecution()` line 2800: `JsonSerializer.Serialize(nestedInput)` — pass context.
  - `States.JsonToString` intrinsic line 2425: `JsonSerializer.Serialize(args[0])` — pass context.
  - `InvokeActivity()` line 2637: `JsonSerializer.Serialize(inputData)` — pass context.

  **Lambda worker** (`LambdaWorker.cs`):
  - `InjectRequestId()` lines 418, 435, 439, 449: `JsonSerializer.Serialize(dict)` — pass context.
  - `Spawn()` line 247: `JsonSerializer.Serialize(initPayload)` — pass context.

  **Lambda handler** (`LambdaServiceHandler.cs`):
  - Various `JsonSerializer.Serialize()` calls for policy JSON, error payloads, ESM configs.
  - `JsonSerializer.SerializeToUtf8Bytes()` calls for invocation payloads and error bodies.

  **ApiGateway V1** (`ApiGatewayV1ServiceHandler.cs`):
  - `JsonSerializer.Deserialize<Dictionary<string, object?>>()` in `HandleControlPlane` line ~116 — pass context.
  - Lambda event building line ~645 — serialization of `Dictionary<string, object?>`.

  **ApiGateway V2** (`ApiGatewayV2ServiceHandler.cs`):
  - `JsonSerializer.Deserialize<Dictionary<string, object?>>()` in control plane handler line ~145 — pass context.
  - `JsonSerializer.SerializeToUtf8Bytes(data, s_jsonOpts)` in `ApigwResponse` line ~110.

  **CloudFormation provisioners** (`CloudFormationProvisioners.cs`):
  - `SecretsManager` provisioner line 606: `JsonSerializer.Deserialize<Dictionary<string, object?>>(template)` — pass context.
  - `SecretsManager` provisioner line 608: `JsonSerializer.Serialize(obj)` — pass context.

  **Files**:
    - `src/MicroStack/Services/CloudFormation/CloudFormationProvisioners.cs`
    - `src/MicroStack/Services/StepFunctions/StepFunctionsServiceHandler.cs`
    - `src/MicroStack/Services/Lambda/LambdaWorker.cs`
    - `src/MicroStack/Services/Lambda/LambdaServiceHandler.cs`
    - `src/MicroStack/Services/Lambda/EventSourceMappingPoller.cs`
    - `src/MicroStack/Services/ApiGateway/ApiGatewayV1ServiceHandler.cs`
    - `src/MicroStack/Services/ApiGateway/ApiGatewayV2ServiceHandler.cs`
    - `src/MicroStack/Services/Kms/KmsServiceHandler.cs`
    - `src/MicroStack/Services/Ses/SesServiceHandler.cs`
    - `src/MicroStack/Services/CloudWatch/CloudWatchServiceHandler.cs`
    - `src/MicroStack/Program.cs`
  **Acceptance**: `grep -r "JsonSerializer.Serialize\b" src/MicroStack/` shows no calls without context; `grep -r "JsonSerializer.Deserialize<object>" src/MicroStack/` returns zero matches

### Phase 3: Remaining Serialization Fixes

- [x] 11. Fix `LambdaServiceHandler` response helper and typed serialization
  **What**: The handler has its own `JsonResponse(object data, int statusCode)` method (line 2649) that calls `JsonSerializer.SerializeToUtf8Bytes(data)`. Change to accept the typed `Dictionary<string, object?>` (which all callers pass) and use context.

  Also fix `InvokeForApiGateway` (line ~2421) which returns `JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object?> {...})`.

  The `LambdaWorkerPool.GetOrCreate()` takes `Dictionary<string, object?> config` — this is fine as in-memory state, but ensure any serialization of `config` uses context.

  **Files**:
    - `src/MicroStack/Services/Lambda/LambdaServiceHandler.cs`
    - `src/MicroStack/Services/Lambda/LambdaWorkerPool.cs`
  **Acceptance**: No untyped `JsonSerializer` calls in Lambda handler; all tests pass

- [x] 12. Fix `SesServiceHandler` response helper
  **What**: Has its own `JsonResponse(object data)` method (line 1070) that calls `JsonSerializer.SerializeToUtf8Bytes(data)`. Change parameter type to `Dictionary<string, object?>` and use context.
  **Files**: `src/MicroStack/Services/Ses/SesServiceHandler.cs`
  **Acceptance**: No untyped `JsonSerializer` calls; SES tests pass

- [x] 13. Audit and fix all remaining `JsonSerializer.Serialize`/`Deserialize` calls
  **What**: Do a final sweep of all `JsonSerializer` calls in `src/MicroStack/` to ensure every one uses the source-generated context. Check for:
  - `JsonSerializer.Serialize(` without context parameter
  - `JsonSerializer.SerializeToUtf8Bytes(` without context parameter
  - `JsonSerializer.Deserialize<` without context parameter
  - `JsonSerializer.Deserialize(` without context parameter

  Some calls that use concrete types like `JsonSerializer.Deserialize<JsonElement>()` or `JsonDocument.Parse()` are already AOT-safe and need no changes.

  Calls serializing `JsonElement` values (like writing a `JsonElement` to response) are AOT-safe.

  The `CloudWatchServiceHandler` has CBOR codec methods (`CborOk(object)` — lines 269-270) that may need attention. If they use `new { }` anonymous types, replace with named types.
  **Files**: Any remaining files with uncontexted serialization calls
  **Acceptance**: `dotnet publish -c Release` with AOT shows zero IL trim warnings related to serialization

### Phase 4: Final Cleanup and Verification

- [x] 14. Fix `AwsResponseHelpers.XmlResponse` and XML-related serialization
  **What**: Review `DictToXml` in `AwsResponseHelpers.cs` which uses `IReadOnlyDictionary<string, object?>` and `IEnumerable<object?>`. These don't go through `JsonSerializer` — they manually build XML via LINQ to XML. Verify they're AOT-safe (they are, since they use `ToString()` on values, not serialization). No changes needed unless trim analysis finds issues.
  **Acceptance**: No trim warnings from XML helpers

- [x] 15. Run AOT publish and fix any remaining trim warnings
  **What**: Run `dotnet publish -c Release -r win-x64` (or `linux-musl-x64`) and check for trim/AOT warnings. Fix any remaining issues. Common things to check:
  - Ensure no `Type.GetType()`, `Activator.CreateInstance()`, or reflection calls
  - Ensure `UseMiddleware<AwsRequestMiddleware>()` is AOT-safe (it is — ASP.NET Core supports this pattern in AOT when the middleware has a public constructor and `InvokeAsync` method)
  - Check that `WebApplicationFactory<Program>` in tests doesn't need AOT metadata (tests run in JIT mode, so this is fine)
  **Acceptance**: `dotnet publish -c Release -r win-x64` completes with zero trim/AOT warnings

- [x] 16. Run full test suite
  **What**: Execute `dotnet test` from `tests/MicroStack.Tests/` and verify all 1,178 tests pass. The tests use AWS SDK v4 and exercise the full request/response pipeline, so they validate that serialization changes haven't altered the wire format.
  **Acceptance**: All 1,178 tests pass; zero failures, zero skipped

- [x] 17. Verify AOT binary runs correctly
  **What**: Run the AOT-compiled binary and execute a smoke test:
  1. Start the binary
  2. Hit `/_ministack/health` endpoint
  3. Create an SQS queue via AWS SDK
  4. Send/receive a message
  5. Hit `/_ministack/reset`
  6. Verify health returns clean state
  **Acceptance**: AOT binary starts, serves requests, returns correct responses

## Verification
- [ ] `dotnet build -c Release` — zero warnings
- [ ] `dotnet publish -c Release -r win-x64` — zero trim/AOT warnings, produces native binary
- [ ] `dotnet test` from `tests/MicroStack.Tests/` — all 1,178 tests pass
- [ ] No `JsonSerializer.Serialize(object)` or `Deserialize<object>` calls remain in src
- [ ] No anonymous types remain in src
- [ ] Container image builds with `runtime-deps` base (self-contained AOT)
- [ ] Wire format unchanged (responses byte-identical for same inputs)

## Appendix: File Impact Summary

| Category | Files | Effort |
|----------|-------|--------|
| Interface + infrastructure | 3 files (IServiceHandler, StatePersistence, csproj) | Low |
| New serialization infra | 2 new files (JsonContext, DictionaryConverter) | Medium |
| Program.cs + AwsResponseHelpers | 2 files | Low |
| 34 null-state handlers | 34 files (mechanical signature change) | Low |
| 5 stateful handlers | 5 files | Medium |
| AccountScopedDictionary | 1 file (optional serialization helpers) | Low |
| StepFunctions (serialization calls) | 1 file | Medium |
| CloudFormation provisioners | 1 file | Medium |
| Lambda handler + worker + pool + poller | 4 files | Medium |
| ApiGateway V1 + V2 | 2 files | Low |
| KMS, SES, CloudWatch | 3 files | Low |
| **Total** | **~57 files** | **XL** |

## Appendix: Risk Assessment

**Highest risk**: The `DictionaryObjectJsonConverter` must perfectly replicate STJ's default behavior for `Dictionary<string, object?>` with nested dicts/lists. Any difference in number formatting (e.g., `1` vs `1.0`), null handling, or key ordering could cause test failures. Mitigate by writing converter unit tests first.

**Medium risk**: The 5 stateful handlers' `GetState()`/`RestoreState()` changes must correctly round-trip `AccountScopedDictionary` data through `JsonElement`. The tuple key `(string AccountId, TKey Key)` needs explicit serialization. If any existing persistence tests exist, they'll validate this.

**Low risk**: The 34 null-state handler signature changes are mechanical and cannot break behavior. The anonymous type replacements are straightforward.

**Dependency on .NET 10**: Ensure the target SDK supports `PublishAot` for web apps. .NET 10 has full AOT support for ASP.NET Core minimal APIs and middleware, so this should work. `UseMiddleware<T>()` is supported in AOT when the middleware follows conventions.
