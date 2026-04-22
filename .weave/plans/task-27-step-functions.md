# Task 27: Step Functions Service Handler

## TL;DR
Port `stepfunctions.py` (2,492 lines) to C# as `StepFunctionsServiceHandler`. Includes control plane (state machine + execution + activity CRUD), full ASL execution engine (Pass, Task, Choice, Wait, Parallel, Map, Succeed, Fail), service integrations (Lambda, SQS, SNS, DynamoDB, nested executions), intrinsic functions, mock config, callback/activity patterns, and timestamp normalization.

Test file: `test_sfn.py` (2,213 lines, 68 tests). We port ~55 tests initially (skipping ECS/RDS-dependent tests that require handlers not yet ported).

## Source Reference
- Python handler: `D:\repos\damianh\ministack\ministack\services\stepfunctions.py` (2,492 lines)
- Python tests: `D:\repos\damianh\ministack\tests\test_sfn.py` (2,213 lines, 68 tests)

## Key Design Decisions

### Service Registration
- `ServiceName` = `"states"` (router already maps `AWSStepFunctions` target prefix → `"states"`)
- Handler constructor takes `ServiceRegistry` reference for aws-sdk service dispatch
- Handler also takes `LambdaServiceHandler` reference for direct Lambda invocation
- Register in `Program.cs` after all other handlers

### Protocol
- JSON protocol via `X-Amz-Target: AWSStepFunctions.<Action>`
- Actions dispatched via switch on the action suffix

### Timestamp Handling
- Step Functions stores dates as ISO strings internally
- Response serialization converts timestamp fields (`creationDate`, `startDate`, `stopDate`, `updateDate`, `timestamp`, `redriveDate`) to Unix epoch seconds (double) so AWS SDKs deserialize them as `DateTime`
- Use `_normalizeTimestampResponse` equivalent in C#

### ASL Execution Engine
- Executions run on background threads (`Thread` with `IsBackground = true`)
- `_run_execution` drives the state machine to completion
- State handlers: `_execute_pass`, `_execute_task`, `_execute_choice`, `_execute_wait`, `_execute_parallel`, `_execute_map`
- `_run_sub_machine` for Parallel branches and Map iterations
- `_ExecutionError` exception class for error propagation

### Cross-Service Invocation (aws-sdk dispatch)
- JSON protocol: Build fake request with `X-Amz-Target` header, route through `ServiceRegistry.Resolve()`, call `HandleAsync()`
- Query protocol: Not needed initially (RDS/ECS not yet ported) — stub with error
- For direct service integrations (SQS, SNS, DynamoDB), call handlers directly

### Mock Config
- Python uses `_microstack_config` endpoint to set `stepfunctions._sfn_mock_config` at runtime
- C# approach: Expose internal `SetMockConfig(Dictionary<string, object?> config)` method
- Wire up `/_microstack/config` endpoint to route SFN mock config to the handler
- Tests that use mock config: `test_sfn_mock_config_return`, `test_sfn_mock_config_throw` — these need the config endpoint enhanced

### Tests to Skip (dependencies not yet ported)
These tests depend on ECS or RDS handlers which are Phase 5/6:
- `test_sfn_integration_ecs_run_task` (ecs)
- `test_sfn_integration_ecs_run_task_sync_success` (ecs)
- `test_sfn_integration_ecs_run_task_output_contains_status` (ecs)
- `test_sfn_aws_sdk_rds_create_and_describe_cluster` (rds)
- `test_sfn_aws_sdk_rds_create_and_describe_instance` (rds)
- `test_sfn_aws_sdk_rds_modify_cluster` (rds)
- `test_sfn_aws_sdk_rds_not_found_error` (rds)
- `test_sfn_aws_sdk_query_acronym_param_mapping` (rds)

Also these use `_microstack_config` which needs special handling:
- `test_sfn_mock_config_return`
- `test_sfn_mock_config_throw`

These are pure unit tests of helper functions — port as C# tests:
- `test_sfn_key_to_api_name_must_convert`
- `test_sfn_key_to_api_name_must_not_convert`
- `test_sfn_key_to_api_name_idempotent`
- `test_sfn_key_to_api_name_round_trip`
- `test_convert_params_to_api_names_nested`

### AWS SDK Client Notes
- NuGet: `AWSSDK.StepFunctions`
- Client: `AmazonStepFunctionsClient`
- Namespace: `Amazon.StepFunctions` / `Amazon.StepFunctions.Model`
- For `StartSyncExecution`, the SDK normally adds a `sync-` host prefix — we need `inject_host_prefix=false` equivalent. In .NET, override `ServiceURL` or use `CanonicalizeUriHandler` like v1 tests.

## TODOs (Execution Steps)

### Step 1: Handler Skeleton + Control Plane
**What**: Create `StepFunctionsServiceHandler.cs` with handler skeleton, state machine CRUD (Create, Describe, Update, Delete, List), and tagging (TagResource, UntagResource, ListTagsForResource). Wire up in `Program.cs`.

**Files**:
- `src/MicroStack/Services/StepFunctions/StepFunctionsServiceHandler.cs` — NEW
- `src/MicroStack/Program.cs` — add registration

**Details**:
- State storage: `AccountScopedDictionary<string, Dictionary<string, object?>>` for `_stateMachines`, `_executions`, `_tags`, `_activities`, `_activityTasks`, `_taskTokens`
- `HandleAsync` switch on action: `CreateStateMachine`, `DeleteStateMachine`, `DescribeStateMachine`, `UpdateStateMachine`, `ListStateMachines`, `TagResource`, `UntagResource`, `ListTagsForResource`
- Timestamp response normalization: `NormalizeTimestampResponse` + `FinalizeResponse` methods
- `Reset()` clears all dictionaries
- Constructor: `internal StepFunctionsServiceHandler(LambdaServiceHandler lambda, ServiceRegistry registry)`
- Build and verify 0 warnings

### Step 2: Execution Lifecycle + Pass/Fail/Succeed States
**What**: Implement `StartExecution`, `StopExecution`, `DescribeExecution`, `ListExecutions`, `GetExecutionHistory`, `StartSyncExecution`, `DescribeStateMachineForExecution`. Implement the ASL execution engine skeleton with `_run_execution` on a background thread. Implement Pass, Succeed, Fail state handlers.

**Files**:
- `src/MicroStack/Services/StepFunctions/StepFunctionsServiceHandler.cs` — extend

**Details**:
- `StartExecution`: Parse `#TestCase` suffix from ARN, create execution record, start background thread
- `StopExecution`: Set status to ABORTED
- `DescribeExecution`: Return execution state
- `ListExecutions`: Filter by SM ARN and status
- `GetExecutionHistory`: Return events, support `reverseOrder`
- `StartSyncExecution`: Run `_run_execution` synchronously (on calling thread)
- `DescribeStateMachineForExecution`: Look up SM from execution
- `_run_execution`: Parse definition, walk states, handle Succeed/Fail/Pass
- Path processing: `_apply_input_path`, `_apply_output_path`, `_apply_result_path`, `_apply_parameters`, `_apply_result_selector`
- `_resolve_path` with `$.field` and `$.field[0]` support
- `_resolve_ctx_path` for `$$.` context paths
- `_resolve_params_obj` for `"key.$"` parameter substitution
- `_next_or_end` helper
- `_add_event` helper
- Build and verify 0 warnings

### Step 3: Choice + Wait States
**What**: Implement Choice state with full rule evaluation and Wait state.

**Files**:
- `src/MicroStack/Services/StepFunctions/StepFunctionsServiceHandler.cs` — extend

**Details**:
- `_execute_choice`: Evaluate rules, route to matching branch or Default
- `_evaluate_rule`: And/Or/Not combinators, Variable resolution
- Type checks: IsPresent, IsNull, IsNumeric, IsString, IsBoolean, IsTimestamp
- String comparisons: StringEquals, StringEqualsPath, StringLessThan, StringGreaterThan, StringLessThanEquals, StringGreaterThanEquals, StringMatches (wildcard to regex)
- Numeric comparisons: NumericEquals, NumericEqualsPath, NumericLessThan, NumericGreaterThan, etc.
- Boolean: BooleanEquals, BooleanEqualsPath
- Timestamp comparisons: TimestampEquals, TimestampLessThan, etc.
- `_execute_wait`: Seconds, Timestamp, SecondsPath, TimestampPath
- Build and verify 0 warnings

### Step 4: Task State + Lambda Integration + Retry/Catch
**What**: Implement Task state with retry/catch logic, Lambda invocation (both direct ARN and `states:::lambda:invoke`), and the execution error framework.

**Files**:
- `src/MicroStack/Services/StepFunctions/StepFunctionsServiceHandler.cs` — extend

**Details**:
- `ExecutionError` internal exception class
- `_execute_task`: Input/output path processing, retry loop, catch handling
- `_invoke_resource`: Dispatch based on resource ARN pattern
- `_extract_lambda_name`: Parse function name from ARN
- `_call_lambda`: Use `LambdaServiceHandler.InvokeForApiGateway()` or similar internal method
- Need new `InvokeForStepFunctions(string funcName, object eventPayload)` method on `LambdaServiceHandler` that returns `(bool success, object? result, string? errorType, string? errorMessage)`
- `_find_matching_retrier` / `_find_matching_catcher`: Error matching with States.ALL, States.TaskFailed
- Retry with backoff: IntervalSeconds * BackoffRate^count, capped at 60s
- Build and verify 0 warnings

### Step 5: Intrinsic Functions
**What**: Implement States.* intrinsic function parsing and evaluation.

**Files**:
- `src/MicroStack/Services/StepFunctions/StepFunctionsServiceHandler.cs` — extend

**Details**:
- `_parse_intrinsic_args`: Recursive descent parser for argument lists
- `_parse_intrinsic_call`: Parse `States.Xxx(...)` calls
- `_eval_intrinsic_arg`: Evaluate string/number/bool/null/path/call arguments
- `_exec_intrinsic`: Execute intrinsic functions:
  - `States.StringToJson` — JSON parse
  - `States.JsonToString` — JSON serialize (compact)
  - `States.JsonMerge` — shallow merge two objects
  - `States.Format` — string template with `{}` placeholders
  - `States.ArrayGetItem` — array indexing
  - `States.Array` — construct array from args
  - `States.ArrayLength` — array length
- `_evaluate_intrinsic`: Parse + execute entry point
- Integrate into `_resolve_params_obj` for `"key.$": "States.Xxx(...)"`
- Build and verify 0 warnings

### Step 6: Parallel + Map States
**What**: Implement Parallel and Map state handlers with concurrent branch/item execution.

**Files**:
- `src/MicroStack/Services/StepFunctions/StepFunctionsServiceHandler.cs` — extend

**Details**:
- `_execute_parallel`: Run branches concurrently using `Thread`s, collect results, apply ResultSelector/ResultPath/OutputPath
- `_execute_map`: Get items from ItemsPath, run Iterator/ItemProcessor for each item, support MaxConcurrency, ItemSelector/Parameters
- `_run_sub_machine`: Shared sub-machine runner for both Parallel branches and Map iterations
- Build and verify 0 warnings

### Step 7: Service Integrations (SQS, SNS, DynamoDB, Nested Executions)
**What**: Implement direct service integrations for Task state resource ARNs.

**Files**:
- `src/MicroStack/Services/StepFunctions/StepFunctionsServiceHandler.cs` — extend

**Details**:
- `_invoke_sqs_send_message`: Call SQS handler directly via ServiceRegistry
- `_invoke_sns_publish`: Call SNS handler, parse MessageId from XML response
- `_invoke_dynamodb`: Call DynamoDB handler for putItem/getItem/deleteItem/updateItem
- `_invoke_nested_start_execution`: Run nested SM via `_start_sync_execution` internally
- Nested sync:2 variant parses Output as JSON
- Build and verify 0 warnings

### Step 8: Activity + Callback Pattern
**What**: Implement Activity CRUD and worker pattern (GetActivityTask, SendTaskSuccess/Failure/Heartbeat).

**Files**:
- `src/MicroStack/Services/StepFunctions/StepFunctionsServiceHandler.cs` — extend

**Details**:
- `CreateActivity`, `DeleteActivity`, `DescribeActivity`, `ListActivities`
- `GetActivityTask`: Long-poll (60s) using async wait + `ManualResetEventSlim` or `SemaphoreSlim`
- `SendTaskSuccess`: Set result on task token, signal event
- `SendTaskFailure`: Set error on task token, signal event
- `SendTaskHeartbeat`: Update heartbeat timestamp
- `_invoke_activity`: Enqueue task, block until worker calls SendTask*
- `_invoke_with_callback`: waitForTaskToken pattern — invoke Lambda then block
- Build and verify 0 warnings

### Step 9: TestState API + aws-sdk Dispatch
**What**: Implement TestState API and generic aws-sdk service integration dispatch.

**Files**:
- `src/MicroStack/Services/StepFunctions/StepFunctionsServiceHandler.cs` — extend

**Details**:
- `_test_state`: Single-state execution in isolation with mock support, inspectionLevel (INFO/DEBUG/TRACE)
- `_dispatch_aws_sdk_json`: Build `ServiceRequest` with X-Amz-Target, route through ServiceRegistry, parse JSON response, apply `_api_name_to_sfn_key` key conversion
- `_api_name_to_sfn_key`: Java SDK V2 naming convention (DBClusters → DbClusters)
- `_sfn_key_to_api_name`: Reverse conversion for param names
- `_convert_params_to_api_names`: Recursive key conversion
- `_AWS_ACRONYMS` set for acronym expansion
- `_invoke_aws_sdk_integration`: Parse service+action from ARN, dispatch to JSON or query protocol
- Build and verify 0 warnings

### Step 10: Mock Config Support
**What**: Wire up the `/_microstack/config` endpoint to set SFN mock config at runtime.

**Files**:
- `src/MicroStack/Services/StepFunctions/StepFunctionsServiceHandler.cs` — add `SetMockConfig` method
- `src/MicroStack/Program.cs` — enhance config endpoint to route SFN config

**Details**:
- `_sfnMockConfig`: Internal dictionary for mock configuration
- `_get_mock_response`: Look up mock response by SM name, test case, state name, attempt index
- Support attempt range keys ("1-3")
- Config endpoint: parse `stepfunctions._sfn_mock_config` key from JSON body, call `SetMockConfig`
- Build and verify 0 warnings

### Step 11: NuGet + Test Infrastructure
**What**: Add AWSSDK.StepFunctions NuGet package, create test class with fixture setup.

**Files**:
- `tests/MicroStack.Tests/MicroStack.Tests.csproj` — add NuGet ref
- `tests/MicroStack.Tests/StepFunctionsTests.cs` — NEW, test class skeleton

**Details**:
- Add `<PackageReference Include="AWSSDK.StepFunctions" />` to test project
- Test class: `public sealed class StepFunctionsTests : IClassFixture<MicroStackFixture>, IAsyncLifetime`
- Two clients: `_sfn` (normal) and `_sfnSync` (with `inject_host_prefix=false` equivalent)
  - For sync client, the SDK adds `sync-` prefix to hostname — need to disable. Check if `AmazonStepFunctionsConfig.DisableHostPrefixInjection` is available, or use custom `HttpClientFactory`
- `_sqs`, `_ddb`, `_sns`, `_lambda`, `_sm` (SecretsManager), `_ssm` clients for integration tests
- `WaitForExecution` polling helper
- `InitializeAsync` calls `/_microstack/reset`
- Build and verify 0 warnings

### Step 12: Core Tests (State Machine CRUD + Execution)
**What**: Port tests for state machine CRUD, basic execution, and lifecycle.

**Files**:
- `tests/MicroStack.Tests/StepFunctionsTests.cs` — extend

**Details**: Port these tests:
- `test_sfn_create_state_machine_v2` → `CreateStateMachine`
- `test_sfn_list_state_machines_v2` → `ListStateMachines`
- `test_sfn_describe_state_machine_v2` → `DescribeStateMachine`
- `test_sfn_update_state_machine` → `UpdateStateMachine`
- `test_sfn_create_duplicate_name` → `CreateDuplicateNameFails`
- `test_sfn_describe_not_found` → `DescribeNotFoundFails`
- `test_sfn_start_execution_not_found` → `StartExecutionNotFoundFails`
- `test_sfn_start_execution_pass_v2` → `StartExecutionPass`
- `test_sfn_execution_choice_v2` → `ExecutionChoice`
- `test_sfn_stop_execution_v2` → `StopExecution`
- `test_sfn_stop_execution` (duplicate coverage)
- `test_sfn_get_execution_history_v2` → `GetExecutionHistory`
- `test_sfn_start_sync_execution` → `StartSyncExecution`
- `test_sfn_describe_state_machine_for_execution` → `DescribeStateMachineForExecution`
- `test_sfn_pass_state_result` → `PassStateResult`
- `test_sfn_fail_state` → `FailState`
- `test_sfn_choice_state` → `ChoiceState`
- `test_sfn_list_executions_filter` → `ListExecutionsFilter`
- `test_sfn_tags_v2` → `Tags`
- Build and run tests

### Step 13: Intrinsic + Timestamp Tests
**What**: Port intrinsic function tests and timestamp SDK compatibility tests.

**Files**:
- `tests/MicroStack.Tests/StepFunctionsTests.cs` — extend

**Details**: Port these tests:
- `test_sfn_intrinsic_string_to_json` → `IntrinsicStringToJson`
- `test_sfn_intrinsic_json_to_string` → `IntrinsicJsonToString`
- `test_sfn_intrinsic_json_merge` → `IntrinsicJsonMerge`
- `test_sfn_intrinsic_format` → `IntrinsicFormat`
- `test_sfn_intrinsic_nested` → `IntrinsicNested`
- `test_sfn_timestamp_fields_are_sdk_compatible` → `TimestampFieldsAreSdkCompatible`
- `test_sfn_activity_timestamp_fields_are_sdk_compatible` → `ActivityTimestampFieldsAreSdkCompatible`
- Build and run tests

### Step 14: Service Integration Tests
**What**: Port tests for SQS, SNS, DynamoDB, Lambda, and nested execution integrations.

**Files**:
- `tests/MicroStack.Tests/StepFunctionsTests.cs` — extend

**Details**: Port these tests:
- `test_sfn_integration_sqs_send_message` → `IntegrationSqsSendMessage`
- `test_sfn_integration_sns_publish` → `IntegrationSnsPublish`
- `test_sfn_integration_dynamodb_put_get` → `IntegrationDynamoDbPutGet`
- `test_sfn_integration_dynamodb_error_catch` → `IntegrationDynamoDbErrorCatch`
- `test_sfn_integration_lambda_invoke` → `IntegrationLambdaInvoke`
- `test_sfn_integration_multi_service_pipeline` → `IntegrationMultiServicePipeline`
- `test_sfn_integration_nested_start_execution_sync_returns_string_output` → `IntegrationNestedSyncStringOutput`
- `test_sfn_integration_nested_start_execution_sync2_returns_json_output` → `IntegrationNestedSync2JsonOutput`
- Build and run tests

### Step 15: Activity + TestState Tests
**What**: Port activity CRUD/worker tests and TestState API tests.

**Files**:
- `tests/MicroStack.Tests/StepFunctionsTests.cs` — extend

**Details**: Port these tests:
- `test_sfn_activity_create_describe_delete` → `ActivityCreateDescribeDelete`
- `test_sfn_activity_list` → `ActivityList`
- `test_sfn_activity_create_already_exists` → `ActivityCreateAlreadyExists`
- `test_sfn_activity_worker_flow` → `ActivityWorkerFlow`
- `test_sfn_activity_worker_failure` → `ActivityWorkerFailure`
- `test_sfn_test_state_pass` → `TestStatePass`
- `test_sfn_test_state_choice` → `TestStateChoice`
- `test_sfn_test_state_fail` → `TestStateFail`
- `test_sfn_test_state_task_with_mock_return` → `TestStateTaskWithMockReturn`
- `test_sfn_test_state_task_with_mock_error` → `TestStateTaskWithMockError`
- `test_sfn_test_state_debug_inspection` → `TestStateDebugInspection`
- `test_sfn_test_state_from_full_definition` → `TestStateFromFullDefinition`
- Build and run tests

### Step 16: aws-sdk Integration Tests + Helper Unit Tests
**What**: Port aws-sdk integration tests (SecretsManager, DynamoDB, SSM) and helper function unit tests.

**Files**:
- `tests/MicroStack.Tests/StepFunctionsTests.cs` — extend

**Details**: Port these tests:
- `test_sfn_aws_sdk_secretsmanager_create_and_get` → `AwsSdkSecretsManagerCreateAndGet`
- `test_sfn_aws_sdk_dynamodb_put_and_get` → `AwsSdkDynamoDbPutAndGet`
- `test_sfn_aws_sdk_unknown_service_fails` → `AwsSdkUnknownServiceFails`
- `test_sfn_aws_sdk_query_pascal_case` → `AwsSdkQueryPascalCase` (SSM)
- `test_sfn_aws_sdk_json_pascal_case` → `AwsSdkJsonPascalCase` (SecretsManager)
- `test_sfn_key_to_api_name_must_convert` → `ApiNameToSfnKeyMustConvert` (helper unit test)
- `test_sfn_key_to_api_name_must_not_convert` → `ApiNameToSfnKeyMustNotConvert`
- `test_sfn_key_to_api_name_idempotent` → `ApiNameToSfnKeyIdempotent`
- `test_sfn_key_to_api_name_round_trip` → `ApiNameToSfnKeyRoundTrip`
- `test_convert_params_to_api_names_nested` → `ConvertParamsToApiNamesNested`
- Build and run tests

### Step 17: Mock Config Tests + Final Polish
**What**: Port mock config tests, any remaining tests, build verification.

**Files**:
- `src/MicroStack/Services/StepFunctions/StepFunctionsServiceHandler.cs` — finalize
- `tests/MicroStack.Tests/StepFunctionsTests.cs` — finalize

**Details**:
- `test_sfn_mock_config_return` → `MockConfigReturn`
- `test_sfn_mock_config_throw` → `MockConfigThrow`
- From test_sfn_create_execute / test_sfn_list (the very first two Python tests that share state) — port as appropriate
- Run full test suite: `dotnet test`
- Run build: `dotnet build MicroStack.slnx -c Release` — verify 0 warnings
- Count final passing tests

## Target File Structure
```
src/MicroStack/Services/StepFunctions/
  StepFunctionsServiceHandler.cs   (~2,000-2,500 lines)

tests/MicroStack.Tests/
  StepFunctionsTests.cs            (~1,200-1,600 lines)
```

## Cross-Handler Dependencies
- `LambdaServiceHandler` — for Lambda function invocation from Task states
- `ServiceRegistry` — for aws-sdk service integration dispatch
- `SqsServiceHandler` — direct SQS integration (via registry or direct ref)
- `SnsServiceHandler` — direct SNS integration (via registry or direct ref)
- `DynamoDbServiceHandler` — direct DynamoDB integration (via registry or direct ref)
- `SecretsManagerServiceHandler` — aws-sdk JSON dispatch
- `SsmServiceHandler` — aws-sdk JSON dispatch

## Acceptance Criteria
- `dotnet build MicroStack.slnx -c Release` — 0 warnings, 0 errors
- `dotnet test` — all existing 427 tests still pass + ~55 new Step Functions tests pass
- Total test count: ~480+
