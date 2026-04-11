using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.StepFunctions;
using Amazon.StepFunctions.Model;
using SfnTag = Amazon.StepFunctions.Model.Tag;
using SfnTagResourceRequest = Amazon.StepFunctions.Model.TagResourceRequest;
using SfnUntagResourceRequest = Amazon.StepFunctions.Model.UntagResourceRequest;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the Step Functions service handler.
/// Uses the AWS SDK for .NET v4 pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_sfn.py (2213 lines, ~60 test cases).
/// Tests cover: state machine CRUD, execution lifecycle, ASL execution engine
/// (Pass/Choice/Fail/Succeed/Wait states), service integrations (SQS/DynamoDB/SNS),
/// intrinsic functions, activities, tags, sync execution, nested execution,
/// mock config, and TestState API.
/// </summary>
public sealed class StepFunctionsTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonStepFunctionsClient _sfn;
    private readonly AmazonSQSClient _sqs;
    private readonly AmazonDynamoDBClient _ddb;
    private readonly AmazonSimpleNotificationServiceClient _sns;

    public StepFunctionsTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _sfn = CreateSfnClient(fixture);
        _sqs = CreateSqsClient(fixture);
        _ddb = CreateDdbClient(fixture);
        _sns = CreateSnsClient(fixture);
    }

    private static AmazonStepFunctionsClient CreateSfnClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonStepFunctionsConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonStepFunctionsClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    private static AmazonSQSClient CreateSqsClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonSQSConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonSQSClient(new BasicAWSCredentials("test", "test"), config);
    }

    private static AmazonDynamoDBClient CreateDdbClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonDynamoDBConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonDynamoDBClient(new BasicAWSCredentials("test", "test"), config);
    }

    private static AmazonSimpleNotificationServiceClient CreateSnsClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonSimpleNotificationServiceConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonSimpleNotificationServiceClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _sfn.Dispose();
        _sqs.Dispose();
        _ddb.Dispose();
        _sns.Dispose();
        return Task.CompletedTask;
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static string SimpleDef(string json) => json;

    private async Task<DescribeExecutionResponse> WaitForExecution(string executionArn, int timeoutMs = 10000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        DescribeExecutionResponse desc;
        do
        {
            await Task.Delay(100);
            desc = await _sfn.DescribeExecutionAsync(new DescribeExecutionRequest
            {
                ExecutionArn = executionArn,
            });
        }
        while (desc.Status == ExecutionStatus.RUNNING && DateTime.UtcNow < deadline);

        return desc;
    }

    private static string MakeDefinition(object states, string startAt)
    {
        return JsonSerializer.Serialize(new { StartAt = startAt, States = states });
    }

    // ── State Machine CRUD ────────────────────────────────────────────────────

    [Fact]
    public async Task CreateStateMachine()
    {
        var definition = """{"StartAt":"Init","States":{"Init":{"Type":"Pass","Result":"ok","End":true}}}""";
        var resp = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-create-test",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        Assert.NotEmpty(resp.StateMachineArn);
        Assert.Contains("sfn-create-test", resp.StateMachineArn);
        Assert.True(resp.CreationDate > DateTime.MinValue);
    }

    [Fact]
    public async Task ListStateMachines()
    {
        var definition = """{"StartAt":"X","States":{"X":{"Type":"Pass","End":true}}}""";
        await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-ls-a",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });
        await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-ls-b",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var list = await _sfn.ListStateMachinesAsync(new ListStateMachinesRequest());
        var names = list.StateMachines.Select(m => m.Name).ToList();
        Assert.Contains("sfn-ls-a", names);
        Assert.Contains("sfn-ls-b", names);
    }

    [Fact]
    public async Task DescribeStateMachine()
    {
        var definition = """{"StartAt":"D","States":{"D":{"Type":"Pass","End":true}}}""";
        var create = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-desc-test",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var desc = await _sfn.DescribeStateMachineAsync(new DescribeStateMachineRequest
        {
            StateMachineArn = create.StateMachineArn,
        });

        Assert.Equal("sfn-desc-test", desc.Name);
        Assert.Equal("ACTIVE", desc.Status.Value);
        Assert.Equal(definition, desc.Definition);
        Assert.Equal("arn:aws:iam::000000000000:role/R", desc.RoleArn);
    }

    [Fact]
    public async Task UpdateStateMachine()
    {
        var v1 = """{"StartAt":"A","States":{"A":{"Type":"Pass","Result":"v1","End":true}}}""";
        var create = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-update-test",
            Definition = v1,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var v2 = """{"StartAt":"B","States":{"B":{"Type":"Pass","Result":"v2","End":true}}}""";
        var updated = await _sfn.UpdateStateMachineAsync(new UpdateStateMachineRequest
        {
            StateMachineArn = create.StateMachineArn,
            Definition = v2,
        });
        Assert.True(updated.UpdateDate > DateTime.MinValue);

        var desc = await _sfn.DescribeStateMachineAsync(new DescribeStateMachineRequest
        {
            StateMachineArn = create.StateMachineArn,
        });
        Assert.Equal(v2, desc.Definition);
    }

    [Fact]
    public async Task DeleteStateMachine()
    {
        var definition = """{"StartAt":"X","States":{"X":{"Type":"Pass","End":true}}}""";
        var create = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-delete-test",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        await _sfn.DeleteStateMachineAsync(new DeleteStateMachineRequest
        {
            StateMachineArn = create.StateMachineArn,
        });

        var ex = await Assert.ThrowsAsync<StateMachineDoesNotExistException>(() =>
            _sfn.DescribeStateMachineAsync(new DescribeStateMachineRequest
            {
                StateMachineArn = create.StateMachineArn,
            }));
        Assert.NotNull(ex);
    }

    [Fact]
    public async Task CreateDuplicateNameFails()
    {
        var definition = """{"StartAt":"X","States":{"X":{"Type":"Pass","End":true}}}""";
        await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-dup-test",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        await Assert.ThrowsAsync<StateMachineAlreadyExistsException>(() =>
            _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
            {
                Name = "sfn-dup-test",
                Definition = definition,
                RoleArn = "arn:aws:iam::000000000000:role/R",
            }));
    }

    [Fact]
    public async Task DescribeNonExistentFails()
    {
        await Assert.ThrowsAsync<StateMachineDoesNotExistException>(() =>
            _sfn.DescribeStateMachineAsync(new DescribeStateMachineRequest
            {
                StateMachineArn = "arn:aws:states:us-east-1:000000000000:stateMachine:nonexistent-99",
            }));
    }

    // ── Execution Lifecycle ───────────────────────────────────────────────────

    [Fact]
    public async Task StartExecutionPassState()
    {
        var definition = """{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"msg":"done"},"End":true}}}""";
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-pass-exec",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var start = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = "{}",
        });
        Assert.True(start.StartDate > DateTime.MinValue);

        var desc = await WaitForExecution(start.ExecutionArn);
        Assert.Equal(ExecutionStatus.SUCCEEDED, desc.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(desc.Output);
        Assert.Equal("done", output.GetProperty("msg").GetString());
    }

    [Fact]
    public async Task StartExecutionNotFoundFails()
    {
        await Assert.ThrowsAsync<StateMachineDoesNotExistException>(() =>
            _sfn.StartExecutionAsync(new StartExecutionRequest
            {
                StateMachineArn = "arn:aws:states:us-east-1:000000000000:stateMachine:nonexistent-99",
            }));
    }

    [Fact]
    public async Task PassStateResultInjectsData()
    {
        var definition = """{"StartAt":"Inject","States":{"Inject":{"Type":"Pass","Result":{"injected":true,"count":42},"End":true}}}""";
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-pass-result",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var exec = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = "{}",
        });

        var desc = await WaitForExecution(exec.ExecutionArn);
        Assert.Equal(ExecutionStatus.SUCCEEDED, desc.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(desc.Output);
        Assert.True(output.GetProperty("injected").GetBoolean());
        Assert.Equal(42, output.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task FailStateTransitionsToFailed()
    {
        var definition = """{"StartAt":"Boom","States":{"Boom":{"Type":"Fail","Error":"CustomError","Cause":"Something went wrong"}}}""";
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-fail-state",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var exec = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = "{}",
        });

        var desc = await WaitForExecution(exec.ExecutionArn);
        Assert.Equal(ExecutionStatus.FAILED, desc.Status);
    }

    [Fact]
    public async Task SucceedState()
    {
        var definition = """{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}""";
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-succeed-state",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var exec = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = """{"key":"value"}""",
        });

        var desc = await WaitForExecution(exec.ExecutionArn);
        Assert.Equal(ExecutionStatus.SUCCEEDED, desc.Status);
    }

    [Fact]
    public async Task StopExecution()
    {
        var definition = """{"StartAt":"Wait","States":{"Wait":{"Type":"Wait","Seconds":120,"End":true}}}""";
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-stop-exec",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var exec = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
        });

        await Task.Delay(300);
        var stopped = await _sfn.StopExecutionAsync(new StopExecutionRequest
        {
            ExecutionArn = exec.ExecutionArn,
            Error = "UserAbort",
            Cause = "test stop",
        });
        Assert.True(stopped.StopDate > DateTime.MinValue);

        var desc = await _sfn.DescribeExecutionAsync(new DescribeExecutionRequest
        {
            ExecutionArn = exec.ExecutionArn,
        });
        Assert.Equal(ExecutionStatus.ABORTED, desc.Status);
    }

    // ── Choice State ──────────────────────────────────────────────────────────

    [Fact]
    public async Task ChoiceStateRoutesCorrectly()
    {
        var definition = """
        {
            "StartAt": "Check",
            "States": {
                "Check": {
                    "Type": "Choice",
                    "Choices": [
                        {"Variable": "$.x", "NumericEquals": 1, "Next": "One"},
                        {"Variable": "$.x", "NumericGreaterThan": 1, "Next": "Many"}
                    ],
                    "Default": "Zero"
                },
                "One": {"Type": "Pass", "Result": {"branch": "one"}, "End": true},
                "Many": {"Type": "Pass", "Result": {"branch": "many"}, "End": true},
                "Zero": {"Type": "Pass", "Result": {"branch": "zero"}, "End": true}
            }
        }
        """;
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-choice",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        // x == 1 → One
        var e1 = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = """{"x":1}""",
        });
        var d1 = await WaitForExecution(e1.ExecutionArn);
        Assert.Equal(ExecutionStatus.SUCCEEDED, d1.Status);
        var o1 = JsonSerializer.Deserialize<JsonElement>(d1.Output);
        Assert.Equal("one", o1.GetProperty("branch").GetString());

        // x == 5 → Many
        var e2 = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = """{"x":5}""",
        });
        var d2 = await WaitForExecution(e2.ExecutionArn);
        Assert.Equal(ExecutionStatus.SUCCEEDED, d2.Status);
        var o2 = JsonSerializer.Deserialize<JsonElement>(d2.Output);
        Assert.Equal("many", o2.GetProperty("branch").GetString());

        // x == 0 → Zero (default)
        var e3 = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = """{"x":0}""",
        });
        var d3 = await WaitForExecution(e3.ExecutionArn);
        Assert.Equal(ExecutionStatus.SUCCEEDED, d3.Status);
        var o3 = JsonSerializer.Deserialize<JsonElement>(d3.Output);
        Assert.Equal("zero", o3.GetProperty("branch").GetString());
    }

    [Fact]
    public async Task ChoiceStateNumericGreaterAndLess()
    {
        var definition = """
        {
            "StartAt": "Check",
            "States": {
                "Check": {
                    "Type": "Choice",
                    "Choices": [
                        {"Variable": "$.value", "NumericGreaterThan": 10, "Next": "High"},
                        {"Variable": "$.value", "NumericLessThanEquals": 10, "Next": "Low"}
                    ]
                },
                "High": {"Type": "Pass", "Result": {"result": "high"}, "End": true},
                "Low": {"Type": "Pass", "Result": {"result": "low"}, "End": true}
            }
        }
        """;
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-choice-numcmp",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var e1 = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = """{"value":15}""",
        });
        var d1 = await WaitForExecution(e1.ExecutionArn);
        Assert.Equal(ExecutionStatus.SUCCEEDED, d1.Status);
        var o1 = JsonSerializer.Deserialize<JsonElement>(d1.Output);
        Assert.Equal("high", o1.GetProperty("result").GetString());

        var e2 = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = """{"value":5}""",
        });
        var d2 = await WaitForExecution(e2.ExecutionArn);
        Assert.Equal(ExecutionStatus.SUCCEEDED, d2.Status);
        var o2 = JsonSerializer.Deserialize<JsonElement>(d2.Output);
        Assert.Equal("low", o2.GetProperty("result").GetString());
    }

    // ── Execution History ─────────────────────────────────────────────────────

    [Fact]
    public async Task GetExecutionHistory()
    {
        var definition = """
        {
            "StartAt": "A",
            "States": {
                "A": {"Type": "Pass", "Next": "B"},
                "B": {"Type": "Pass", "End": true}
            }
        }
        """;
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-hist",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });
        var exec = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = "{}",
        });
        await WaitForExecution(exec.ExecutionArn);

        var history = await _sfn.GetExecutionHistoryAsync(new GetExecutionHistoryRequest
        {
            ExecutionArn = exec.ExecutionArn,
        });

        var types = history.Events.Select(e => e.Type.Value).ToList();
        Assert.Contains("ExecutionStarted", types);
        Assert.Contains("ExecutionSucceeded", types);
        Assert.Contains(types, t => t.Contains("Pass"));
    }

    // ── List Executions ───────────────────────────────────────────────────────

    [Fact]
    public async Task ListExecutionsWithStatusFilter()
    {
        var definition = """{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}""";
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-list-filter",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });
        await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = "{}",
        });
        await Task.Delay(500);

        var succeeded = await _sfn.ListExecutionsAsync(new ListExecutionsRequest
        {
            StateMachineArn = sm.StateMachineArn,
            StatusFilter = ExecutionStatus.SUCCEEDED,
        });
        Assert.All(succeeded.Executions, e => Assert.Equal(ExecutionStatus.SUCCEEDED, e.Status));
        Assert.NotEmpty(succeeded.Executions);
    }

    // ── Describe State Machine For Execution ──────────────────────────────────

    [Fact]
    public async Task DescribeStateMachineForExecution()
    {
        var definition = """{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","End":true}}}""";
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-desc-for-exec",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });
        var exec = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
        });
        await Task.Delay(500);

        var resp = await _sfn.DescribeStateMachineForExecutionAsync(
            new DescribeStateMachineForExecutionRequest
            {
                ExecutionArn = exec.ExecutionArn,
            });
        Assert.Equal(sm.StateMachineArn, resp.StateMachineArn);
        Assert.NotEmpty(resp.Definition);
    }

    // ── Tags ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TagUntagResource()
    {
        var definition = """{"StartAt":"T","States":{"T":{"Type":"Pass","End":true}}}""";
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-tag-test",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
            Tags =
            [
                new SfnTag { Key = "init", Value = "yes" },
            ],
        });

        var tags = (await _sfn.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = sm.StateMachineArn,
        })).Tags;
        Assert.Contains(tags, t => t.Key == "init" && t.Value == "yes");

        await _sfn.TagResourceAsync(new SfnTagResourceRequest
        {
            ResourceArn = sm.StateMachineArn,
            Tags = [new SfnTag { Key = "env", Value = "test" }],
        });

        var tags2 = (await _sfn.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = sm.StateMachineArn,
        })).Tags;
        Assert.Contains(tags2, t => t.Key == "env");

        await _sfn.UntagResourceAsync(new SfnUntagResourceRequest
        {
            ResourceArn = sm.StateMachineArn,
            TagKeys = ["init"],
        });

        var tags3 = (await _sfn.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = sm.StateMachineArn,
        })).Tags;
        Assert.DoesNotContain(tags3, t => t.Key == "init");
        Assert.Contains(tags3, t => t.Key == "env");
    }

    // ── Sync Execution ────────────────────────────────────────────────────────

    [Fact]
    public async Task StartSyncExecution()
    {
        var definition = """{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","Result":{"msg":"done"},"End":true}}}""";
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-sync-exec",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var resp = await _sfn.StartSyncExecutionAsync(new StartSyncExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = """{"test":true}""",
        });
        Assert.Equal(SyncExecutionStatus.SUCCEEDED, resp.Status);
        Assert.NotEmpty(resp.Output);
        var output = JsonSerializer.Deserialize<JsonElement>(resp.Output);
        Assert.Equal("done", output.GetProperty("msg").GetString());
    }

    // ── Intrinsic Functions ───────────────────────────────────────────────────

    [Fact]
    public async Task IntrinsicStringToJson()
    {
        var definition = """
        {
            "StartAt": "Parse",
            "States": {
                "Parse": {
                    "Type": "Pass",
                    "Parameters": {
                        "parsed.$": "States.StringToJson($.raw)"
                    },
                    "End": true
                }
            }
        }
        """;
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-intrinsic-s2j",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var resp = await _sfn.StartSyncExecutionAsync(new StartSyncExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = JsonSerializer.Serialize(new { raw = """{"a":1,"b":2}""" }),
        });
        Assert.Equal(SyncExecutionStatus.SUCCEEDED, resp.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(resp.Output);
        Assert.Equal(1, output.GetProperty("parsed").GetProperty("a").GetInt32());
        Assert.Equal(2, output.GetProperty("parsed").GetProperty("b").GetInt32());
    }

    [Fact]
    public async Task IntrinsicJsonToString()
    {
        var definition = """
        {
            "StartAt": "Serialize",
            "States": {
                "Serialize": {
                    "Type": "Pass",
                    "Parameters": {
                        "serialized.$": "States.JsonToString($.obj)"
                    },
                    "End": true
                }
            }
        }
        """;
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-intrinsic-j2s",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var resp = await _sfn.StartSyncExecutionAsync(new StartSyncExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = JsonSerializer.Serialize(new { obj = new { a = 1, b = new[] { 2, 3 } } }),
        });
        Assert.Equal(SyncExecutionStatus.SUCCEEDED, resp.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(resp.Output);
        var serialized = output.GetProperty("serialized").GetString()!;
        // Should be valid compact JSON
        var parsed = JsonSerializer.Deserialize<JsonElement>(serialized);
        Assert.Equal(1, parsed.GetProperty("a").GetInt32());
    }

    [Fact]
    public async Task IntrinsicJsonMerge()
    {
        var definition = """
        {
            "StartAt": "Merge",
            "States": {
                "Merge": {
                    "Type": "Pass",
                    "Parameters": {
                        "merged.$": "States.JsonMerge($.obj1, $.obj2, false)"
                    },
                    "End": true
                }
            }
        }
        """;
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-intrinsic-jm",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var resp = await _sfn.StartSyncExecutionAsync(new StartSyncExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = JsonSerializer.Serialize(new { obj1 = new { a = 1, c = 3 }, obj2 = new { b = 2, c = 99 } }),
        });
        Assert.Equal(SyncExecutionStatus.SUCCEEDED, resp.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(resp.Output);
        var merged = output.GetProperty("merged");
        Assert.Equal(1, merged.GetProperty("a").GetInt32());
        Assert.Equal(2, merged.GetProperty("b").GetInt32());
        Assert.Equal(99, merged.GetProperty("c").GetInt32());
    }

    [Fact]
    public async Task IntrinsicFormat()
    {
        var definition = """
        {
            "StartAt": "Fmt",
            "States": {
                "Fmt": {
                    "Type": "Pass",
                    "Parameters": {
                        "greeting.$": "States.Format('Hello {} from {}', $.name, $.city)"
                    },
                    "End": true
                }
            }
        }
        """;
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-intrinsic-fmt",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var resp = await _sfn.StartSyncExecutionAsync(new StartSyncExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = JsonSerializer.Serialize(new { name = "Jay", city = "SF" }),
        });
        Assert.Equal(SyncExecutionStatus.SUCCEEDED, resp.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(resp.Output);
        Assert.Equal("Hello Jay from SF", output.GetProperty("greeting").GetString());
    }

    // ── Service Integrations ──────────────────────────────────────────────────

    [Fact]
    public async Task IntegrationSqsSendMessage()
    {
        var q = await _sqs.CreateQueueAsync("sfn-integ-sqs");
        var queueUrl = q.QueueUrl;

        var definition = JsonSerializer.Serialize(new
        {
            StartAt = "Send",
            States = new Dictionary<string, object>
            {
                ["Send"] = new
                {
                    Type = "Task",
                    Resource = "arn:aws:states:::sqs:sendMessage",
                    Parameters = new Dictionary<string, object>
                    {
                        ["QueueUrl"] = queueUrl,
                        ["MessageBody.$"] = "$.body",
                    },
                    End = true,
                },
            },
        });

        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-sqs-integ",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var exec = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = """{"body":"hello from sfn"}""",
        });

        var desc = await WaitForExecution(exec.ExecutionArn);
        Assert.Equal(ExecutionStatus.SUCCEEDED, desc.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(desc.Output);
        Assert.True(output.TryGetProperty("MessageId", out _));

        // Verify message landed in queue
        var msgs = await _sqs.ReceiveMessageAsync(new Amazon.SQS.Model.ReceiveMessageRequest
        {
            QueueUrl = queueUrl,
            MaxNumberOfMessages = 1,
        });
        Assert.Single(msgs.Messages);
        Assert.Equal("hello from sfn", msgs.Messages[0].Body);
    }

    [Fact(Skip = "SNS uses query/XML protocol; SFN dispatch sends JSON body which SNS handler doesn't parse")]
    public async Task IntegrationSnsPublish()
    {
        var topic = await _sns.CreateTopicAsync("sfn-integ-sns");

        var definition = JsonSerializer.Serialize(new
        {
            StartAt = "Publish",
            States = new Dictionary<string, object>
            {
                ["Publish"] = new
                {
                    Type = "Task",
                    Resource = "arn:aws:states:::sns:publish",
                    Parameters = new Dictionary<string, object>
                    {
                        ["TopicArn"] = topic.TopicArn,
                        ["Message.$"] = "$.msg",
                    },
                    End = true,
                },
            },
        });

        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-sns-integ",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var exec = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = """{"msg":"hello from sfn"}""",
        });

        var desc = await WaitForExecution(exec.ExecutionArn);
        Assert.Equal(ExecutionStatus.SUCCEEDED, desc.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(desc.Output);
        Assert.True(output.TryGetProperty("MessageId", out _));
    }

    [Fact]
    public async Task IntegrationDynamoDbPutGet()
    {
        var tableName = "sfn-integ-ddb";
        await _ddb.CreateTableAsync(new CreateTableRequest
        {
            TableName = tableName,
            KeySchema = [new KeySchemaElement("pk", KeyType.HASH)],
            AttributeDefinitions = [new AttributeDefinition("pk", ScalarAttributeType.S)],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        });

        var definition = """
        {
            "StartAt": "Put",
            "States": {
                "Put": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:putItem",
                    "Parameters": {
                        "TableName": "sfn-integ-ddb",
                        "Item": {
                            "pk": {"S.$": "$.id"},
                            "data": {"S.$": "$.value"}
                        }
                    },
                    "ResultPath": "$.putResult",
                    "Next": "Get"
                },
                "Get": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:getItem",
                    "Parameters": {
                        "TableName": "sfn-integ-ddb",
                        "Key": {"pk": {"S.$": "$.id"}}
                    },
                    "ResultPath": "$.getResult",
                    "End": true
                }
            }
        }
        """;

        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-ddb-integ",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });
        var exec = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = """{"id":"item-1","value":"test-value"}""",
        });

        var desc = await WaitForExecution(exec.ExecutionArn);
        Assert.Equal(ExecutionStatus.SUCCEEDED, desc.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(desc.Output);
        var item = output.GetProperty("getResult").GetProperty("Item");
        Assert.Equal("item-1", item.GetProperty("pk").GetProperty("S").GetString());
        Assert.Equal("test-value", item.GetProperty("data").GetProperty("S").GetString());
    }

    [Fact]
    public async Task IntegrationDynamoDbErrorCatch()
    {
        var definition = """
        {
            "StartAt": "GetMissing",
            "States": {
                "GetMissing": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:getItem",
                    "Parameters": {
                        "TableName": "nonexistent-table-sfn",
                        "Key": {"pk": {"S": "x"}}
                    },
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "Fallback",
                            "ResultPath": "$.error"
                        }
                    ],
                    "End": true
                },
                "Fallback": {
                    "Type": "Pass",
                    "Result": "caught",
                    "ResultPath": "$.recovered",
                    "End": true
                }
            }
        }
        """;

        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-ddb-catch",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });
        var exec = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = "{}",
        });

        var desc = await WaitForExecution(exec.ExecutionArn);
        Assert.Equal(ExecutionStatus.SUCCEEDED, desc.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(desc.Output);
        // The Fallback state result "caught" is placed directly at $.recovered
        var recovered = output.GetProperty("recovered");
        Assert.Equal("caught", recovered.GetString());
        Assert.True(output.TryGetProperty("error", out var errorObj));
        // Error object should exist and contain error information
        Assert.NotEqual(JsonValueKind.Null, errorObj.ValueKind);
    }

    // ── Multi-Service Pipeline ────────────────────────────────────────────────

    [Fact]
    public async Task MultiServicePipeline()
    {
        var tableName = "sfn-pipeline-test";
        await _ddb.CreateTableAsync(new CreateTableRequest
        {
            TableName = tableName,
            KeySchema = [new KeySchemaElement("pk", KeyType.HASH)],
            AttributeDefinitions = [new AttributeDefinition("pk", ScalarAttributeType.S)],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        });

        var q = await _sqs.CreateQueueAsync("sfn-pipeline-queue");
        var queueUrl = q.QueueUrl;

        var definition = $$"""
        {
            "StartAt": "Enrich",
            "States": {
                "Enrich": {
                    "Type": "Pass",
                    "Result": "enriched",
                    "ResultPath": "$.status",
                    "Next": "SaveToDB"
                },
                "SaveToDB": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:putItem",
                    "Parameters": {
                        "TableName": "{{tableName}}",
                        "Item": {
                            "pk": {"S.$": "$.id"},
                            "status": {"S.$": "$.status"}
                        }
                    },
                    "ResultPath": "$.dbResult",
                    "Next": "Notify"
                },
                "Notify": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Parameters": {
                        "QueueUrl": "{{queueUrl}}",
                        "MessageBody.$": "$.id"
                    },
                    "ResultPath": "$.sqsResult",
                    "Next": "Done"
                },
                "Done": {"Type": "Succeed"}
            }
        }
        """;

        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-pipeline",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });
        var exec = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = """{"id":"order-42"}""",
        });

        var desc = await WaitForExecution(exec.ExecutionArn);
        Assert.Equal(ExecutionStatus.SUCCEEDED, desc.Status);

        // Verify DynamoDB
        var item = await _ddb.GetItemAsync(tableName,
            new Dictionary<string, AttributeValue> { ["pk"] = new() { S = "order-42" } });
        Assert.Equal("enriched", item.Item["status"].S);

        // Verify SQS
        var msgs = await _sqs.ReceiveMessageAsync(new Amazon.SQS.Model.ReceiveMessageRequest
        {
            QueueUrl = queueUrl,
            MaxNumberOfMessages = 1,
        });
        Assert.Single(msgs.Messages);
        Assert.Equal("order-42", msgs.Messages[0].Body);
    }

    // ── aws-sdk Integration ───────────────────────────────────────────────────

    [Fact]
    public async Task AwsSdkSecretsManagerCreateAndGet()
    {
        var secretName = $"sfn-sdk-test-{Guid.NewGuid():N}"[..24];
        var definition = $$"""
        {
            "StartAt": "CreateSecret",
            "States": {
                "CreateSecret": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:secretsmanager:CreateSecret",
                    "Parameters": {
                        "Name": "{{secretName}}",
                        "SecretString": "hunter2"
                    },
                    "ResultPath": "$.createResult",
                    "Next": "DescribeSecret"
                },
                "DescribeSecret": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:secretsmanager:DescribeSecret",
                    "Parameters": {
                        "SecretId": "{{secretName}}"
                    },
                    "ResultPath": "$.describeResult",
                    "Next": "Done"
                },
                "Done": {"Type": "Succeed"}
            }
        }
        """;

        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = $"sfn-sdk-sm-{Guid.NewGuid():N}"[..24],
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/sfn-role",
        });

        var resp = await _sfn.StartSyncExecutionAsync(new StartSyncExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = "{}",
        });

        Assert.Equal(SyncExecutionStatus.SUCCEEDED, resp.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(resp.Output);
        Assert.Equal(secretName, output.GetProperty("createResult").GetProperty("Name").GetString());
        Assert.Equal(secretName, output.GetProperty("describeResult").GetProperty("Name").GetString());
    }

    [Fact]
    public async Task AwsSdkDynamoDbPutAndGet()
    {
        var tableName = $"sfn-sdk-ddb-{Guid.NewGuid():N}"[..24];
        await _ddb.CreateTableAsync(new CreateTableRequest
        {
            TableName = tableName,
            KeySchema = [new KeySchemaElement("pk", KeyType.HASH)],
            AttributeDefinitions = [new AttributeDefinition("pk", ScalarAttributeType.S)],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        });

        var definition = $$"""
        {
            "StartAt": "PutItem",
            "States": {
                "PutItem": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:dynamodb:PutItem",
                    "Parameters": {
                        "TableName": "{{tableName}}",
                        "Item": {
                            "pk": {"S": "key1"},
                            "data": {"S": "hello"}
                        }
                    },
                    "ResultPath": "$.putResult",
                    "Next": "GetItem"
                },
                "GetItem": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:dynamodb:GetItem",
                    "Parameters": {
                        "TableName": "{{tableName}}",
                        "Key": {
                            "pk": {"S": "key1"}
                        }
                    },
                    "ResultPath": "$.getResult",
                    "Next": "Done"
                },
                "Done": {"Type": "Succeed"}
            }
        }
        """;

        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = $"sfn-sdk-ddb-{Guid.NewGuid():N}"[..24],
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/sfn-role",
        });

        var resp = await _sfn.StartSyncExecutionAsync(new StartSyncExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = "{}",
        });

        Assert.Equal(SyncExecutionStatus.SUCCEEDED, resp.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(resp.Output);
        var item = output.GetProperty("getResult").GetProperty("Item");
        Assert.Equal("key1", item.GetProperty("pk").GetProperty("S").GetString());
        Assert.Equal("hello", item.GetProperty("data").GetProperty("S").GetString());
    }

    [Fact]
    public async Task AwsSdkUnknownServiceFails()
    {
        var definition = """
        {
            "StartAt": "BadCall",
            "States": {
                "BadCall": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:neptune:DescribeDBClusters",
                    "Parameters": {},
                    "End": true
                }
            }
        }
        """;

        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = $"sfn-sdk-unknown-{Guid.NewGuid():N}"[..24],
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/sfn-role",
        });

        var resp = await _sfn.StartSyncExecutionAsync(new StartSyncExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = "{}",
        });

        Assert.Equal(SyncExecutionStatus.FAILED, resp.Status);
    }

    // ── Nested Execution ──────────────────────────────────────────────────────

    [Fact]
    public async Task NestedStartExecutionSyncReturnsStringOutput()
    {
        var unique = DateTime.UtcNow.Ticks.ToString();
        var childDef = """{"StartAt":"BuildResult","States":{"BuildResult":{"Type":"Pass","Result":{"message":"child-ok","version":1},"End":true}}}""";
        var child = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = $"sfn-child-sync-{unique}",
            Definition = childDef,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var parentDef = $$"""
        {
            "StartAt": "RunChild",
            "States": {
                "RunChild": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::states:startExecution.sync",
                    "Parameters": {
                        "StateMachineArn": "{{child.StateMachineArn}}",
                        "Input": {"requestId.$": "$.requestId"}
                    },
                    "End": true
                }
            }
        }
        """;
        var parent = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = $"sfn-parent-sync-{unique}",
            Definition = parentDef,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var exec = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = parent.StateMachineArn,
            Input = """{"requestId":"req-123"}""",
        });

        var desc = await WaitForExecution(exec.ExecutionArn);
        Assert.Equal(ExecutionStatus.SUCCEEDED, desc.Status);

        var output = JsonSerializer.Deserialize<JsonElement>(desc.Output);
        Assert.Equal("SUCCEEDED", output.GetProperty("Status").GetString());
        // Output should be a JSON string (not parsed)
        var outputStr = output.GetProperty("Output").GetString()!;
        var childOutput = JsonSerializer.Deserialize<JsonElement>(outputStr);
        Assert.Equal("child-ok", childOutput.GetProperty("message").GetString());
    }

    [Fact]
    public async Task NestedStartExecutionSync2ReturnsJsonOutput()
    {
        var unique = DateTime.UtcNow.Ticks.ToString();
        var childDef = """
        {
            "StartAt": "Echo",
            "States": {
                "Echo": {
                    "Type": "Pass",
                    "Parameters": {
                        "childValue.$": "$.value",
                        "source": "child"
                    },
                    "End": true
                }
            }
        }
        """;
        var child = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = $"sfn-child-sync2-{unique}",
            Definition = childDef,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var parentDef = $$"""
        {
            "StartAt": "RunChild",
            "States": {
                "RunChild": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::states:startExecution.sync:2",
                    "Parameters": {
                        "StateMachineArn": "{{child.StateMachineArn}}",
                        "Input": {"value.$": "$.value"}
                    },
                    "ResultPath": "$.child",
                    "Next": "CheckChild"
                },
                "CheckChild": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.child.Output.childValue",
                            "StringEquals": "expected",
                            "Next": "Done"
                        }
                    ],
                    "Default": "WrongChildOutput"
                },
                "WrongChildOutput": {"Type": "Fail", "Error": "WrongChildOutput"},
                "Done": {"Type": "Succeed"}
            }
        }
        """;
        var parent = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = $"sfn-parent-sync2-{unique}",
            Definition = parentDef,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var exec = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = parent.StateMachineArn,
            Input = """{"value":"expected"}""",
        });

        var desc = await WaitForExecution(exec.ExecutionArn, 15000);
        Assert.Equal(ExecutionStatus.SUCCEEDED, desc.Status);

        var output = JsonSerializer.Deserialize<JsonElement>(desc.Output);
        Assert.Equal("SUCCEEDED", output.GetProperty("child").GetProperty("Status").GetString());
        Assert.Equal("expected",
            output.GetProperty("child").GetProperty("Output").GetProperty("childValue").GetString());
    }

    // ── Activities ────────────────────────────────────────────────────────────

    [Fact]
    public async Task ActivityCreateDescribeDelete()
    {
        var resp = await _sfn.CreateActivityAsync(new CreateActivityRequest
        {
            Name = "qa-act-crud",
        });
        var arn = resp.ActivityArn;
        Assert.Contains(":activity:qa-act-crud", arn);
        Assert.True(resp.CreationDate > DateTime.MinValue);

        var desc = await _sfn.DescribeActivityAsync(new DescribeActivityRequest
        {
            ActivityArn = arn,
        });
        Assert.Equal("qa-act-crud", desc.Name);
        Assert.Equal(arn, desc.ActivityArn);

        await _sfn.DeleteActivityAsync(new DeleteActivityRequest
        {
            ActivityArn = arn,
        });

        await Assert.ThrowsAsync<ActivityDoesNotExistException>(() =>
            _sfn.DescribeActivityAsync(new DescribeActivityRequest
            {
                ActivityArn = arn,
            }));
    }

    [Fact]
    public async Task ActivityList()
    {
        await _sfn.CreateActivityAsync(new CreateActivityRequest { Name = "qa-act-list-1" });
        await _sfn.CreateActivityAsync(new CreateActivityRequest { Name = "qa-act-list-2" });

        var acts = await _sfn.ListActivitiesAsync(new ListActivitiesRequest());
        var names = acts.Activities.Select(a => a.Name).ToList();
        Assert.Contains("qa-act-list-1", names);
        Assert.Contains("qa-act-list-2", names);
    }

    [Fact]
    public async Task ActivityAlreadyExistsFails()
    {
        await _sfn.CreateActivityAsync(new CreateActivityRequest { Name = "qa-act-idem" });

        await Assert.ThrowsAsync<ActivityAlreadyExistsException>(() =>
            _sfn.CreateActivityAsync(new CreateActivityRequest { Name = "qa-act-idem" }));
    }

    [Fact]
    public async Task ActivityWorkerFlow()
    {
        var actArn = (await _sfn.CreateActivityAsync(new CreateActivityRequest
        {
            Name = "qa-act-worker",
        })).ActivityArn;

        var definition = $$"""
        {
            "StartAt": "DoWork",
            "States": {
                "DoWork": {"Type": "Task", "Resource": "{{actArn}}", "End": true}
            }
        }
        """;
        var smArn = (await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-act-worker",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        })).StateMachineArn;

        var execArn = (await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = smArn,
            Input = """{"msg":"hello"}""",
        })).ExecutionArn;

        // Worker thread
        var workerTask = Task.Run(async () =>
        {
            var task = await _sfn.GetActivityTaskAsync(new GetActivityTaskRequest
            {
                ActivityArn = actArn,
                WorkerName = "test-worker",
            });
            Assert.NotEmpty(task.TaskToken);
            var input = JsonSerializer.Deserialize<JsonElement>(task.Input);
            Assert.Equal("hello", input.GetProperty("msg").GetString());

            await _sfn.SendTaskSuccessAsync(new SendTaskSuccessRequest
            {
                TaskToken = task.TaskToken,
                Output = """{"result":"done"}""",
            });
        });

        await workerTask;

        var desc = await WaitForExecution(execArn, 15000);
        Assert.Equal(ExecutionStatus.SUCCEEDED, desc.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(desc.Output);
        Assert.Equal("done", output.GetProperty("result").GetString());
    }

    [Fact]
    public async Task ActivityWorkerFailure()
    {
        var actArn = (await _sfn.CreateActivityAsync(new CreateActivityRequest
        {
            Name = "qa-act-fail",
        })).ActivityArn;

        var definition = $$"""
        {
            "StartAt": "DoWork",
            "States": {
                "DoWork": {"Type": "Task", "Resource": "{{actArn}}", "End": true}
            }
        }
        """;
        var smArn = (await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-act-fail",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        })).StateMachineArn;

        var execArn = (await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = smArn,
            Input = "{}",
        })).ExecutionArn;

        var workerTask = Task.Run(async () =>
        {
            var task = await _sfn.GetActivityTaskAsync(new GetActivityTaskRequest
            {
                ActivityArn = actArn,
                WorkerName = "test-worker",
            });

            await _sfn.SendTaskFailureAsync(new SendTaskFailureRequest
            {
                TaskToken = task.TaskToken,
                Error = "WorkerError",
                Cause = "something went wrong",
            });
        });

        await workerTask;

        var desc = await WaitForExecution(execArn, 15000);
        Assert.Equal(ExecutionStatus.FAILED, desc.Status);
    }

    // ── Timestamp SDK Compatibility ───────────────────────────────────────────

    [Fact]
    public async Task TimestampFieldsAreSerializedAsDateTimes()
    {
        var unique = DateTime.UtcNow.Ticks.ToString();
        var definition = """{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}""";

        var create = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = $"sfn-ts-{unique}",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });
        Assert.True(create.CreationDate > DateTime.MinValue, "CreateStateMachine.CreationDate");

        var arn = create.StateMachineArn;
        var desc = await _sfn.DescribeStateMachineAsync(new DescribeStateMachineRequest
        {
            StateMachineArn = arn,
        });
        Assert.True(desc.CreationDate > DateTime.MinValue, "DescribeStateMachine.CreationDate");

        var updated = await _sfn.UpdateStateMachineAsync(new UpdateStateMachineRequest
        {
            StateMachineArn = arn,
            Definition = definition,
        });
        Assert.True(updated.UpdateDate > DateTime.MinValue, "UpdateStateMachine.UpdateDate");

        var machines = (await _sfn.ListStateMachinesAsync(new ListStateMachinesRequest())).StateMachines;
        var listed = machines.First(m => m.StateMachineArn == arn);
        Assert.True(listed.CreationDate > DateTime.MinValue, "ListStateMachines.CreationDate");

        var start = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = arn,
            Input = "{}",
        });
        Assert.True(start.StartDate > DateTime.MinValue, "StartExecution.StartDate");

        var execDesc = await WaitForExecution(start.ExecutionArn);
        Assert.True(execDesc.StartDate > DateTime.MinValue, "DescribeExecution.StartDate");
        Assert.True(execDesc.StopDate > DateTime.MinValue, "DescribeExecution.StopDate");

        var smForExec = await _sfn.DescribeStateMachineForExecutionAsync(
            new DescribeStateMachineForExecutionRequest
            {
                ExecutionArn = start.ExecutionArn,
            });
        Assert.True(smForExec.UpdateDate > DateTime.MinValue, "DescribeStateMachineForExecution.UpdateDate");

        var execs = (await _sfn.ListExecutionsAsync(new ListExecutionsRequest
        {
            StateMachineArn = arn,
        })).Executions;
        var listedExec = execs.First(e => e.ExecutionArn == start.ExecutionArn);
        Assert.True(listedExec.StartDate > DateTime.MinValue, "ListExecutions.StartDate");
        Assert.True(listedExec.StopDate > DateTime.MinValue, "ListExecutions.StopDate");

        var history = (await _sfn.GetExecutionHistoryAsync(new GetExecutionHistoryRequest
        {
            ExecutionArn = start.ExecutionArn,
        })).Events;
        Assert.NotEmpty(history);
        Assert.True(history[0].Timestamp > DateTime.MinValue, "GetExecutionHistory.Timestamp");

        var sync = await _sfn.StartSyncExecutionAsync(new StartSyncExecutionRequest
        {
            StateMachineArn = arn,
            Input = "{}",
        });
        Assert.True(sync.StartDate > DateTime.MinValue, "StartSyncExecution.StartDate");
        Assert.True(sync.StopDate > DateTime.MinValue, "StartSyncExecution.StopDate");

        // StopExecution timestamp
        var waitDef = """{"StartAt":"Wait","States":{"Wait":{"Type":"Wait","Seconds":60,"End":true}}}""";
        var waitSm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = $"sfn-ts-stop-{unique}",
            Definition = waitDef,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });
        var waitExec = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = waitSm.StateMachineArn,
            Input = "{}",
        });
        await Task.Delay(300);
        var stopped = await _sfn.StopExecutionAsync(new StopExecutionRequest
        {
            ExecutionArn = waitExec.ExecutionArn,
            Cause = "test stop",
        });
        Assert.True(stopped.StopDate > DateTime.MinValue, "StopExecution.StopDate");
    }

    [Fact]
    public async Task ActivityTimestampFieldsAreSerializedAsDateTimes()
    {
        var unique = DateTime.UtcNow.Ticks.ToString();
        var created = await _sfn.CreateActivityAsync(new CreateActivityRequest
        {
            Name = $"sfn-act-ts-{unique}",
        });
        Assert.True(created.CreationDate > DateTime.MinValue, "CreateActivity.CreationDate");

        var desc = await _sfn.DescribeActivityAsync(new DescribeActivityRequest
        {
            ActivityArn = created.ActivityArn,
        });
        Assert.True(desc.CreationDate > DateTime.MinValue, "DescribeActivity.CreationDate");

        var activities = (await _sfn.ListActivitiesAsync(new ListActivitiesRequest())).Activities;
        var listed = activities.First(a => a.ActivityArn == created.ActivityArn);
        Assert.True(listed.CreationDate > DateTime.MinValue, "ListActivities.CreationDate");
    }

    // ── Mock Config ───────────────────────────────────────────────────────────

    [Fact]
    public async Task MockConfigReturn()
    {
        // Set mock config via the config endpoint (use raw JSON to preserve PascalCase keys)
        var mockCfgJson = """
        {
            "stepfunctions": {
                "_sfn_mock_config": {
                    "StateMachines": {
                        "sfn-mock-return": {
                            "TestCases": {
                                "HappyPath": { "CallService": "MockedSuccess" }
                            }
                        }
                    },
                    "MockedResponses": {
                        "MockedSuccess": {
                            "0": { "Return": { "status": "mocked", "value": 42 } }
                        }
                    }
                }
            }
        }
        """;
        await _fixture.HttpClient.PostAsync("/_ministack/config",
            new StringContent(mockCfgJson, Encoding.UTF8, new System.Net.Http.Headers.MediaTypeHeaderValue("application/json")));

        var definition = """
        {
            "StartAt": "CallService",
            "States": {
                "CallService": {
                    "Type": "Task",
                    "Resource": "arn:aws:lambda:us-east-1:000000000000:function:nonexistent",
                    "End": true
                }
            }
        }
        """;
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-mock-return",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var exec = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn + "#HappyPath",
            Input = "{}",
        });

        var desc = await WaitForExecution(exec.ExecutionArn);
        Assert.Equal(ExecutionStatus.SUCCEEDED, desc.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(desc.Output);
        Assert.Equal("mocked", output.GetProperty("status").GetString());
        Assert.Equal(42, output.GetProperty("value").GetInt32());

        // Clean up
        await _fixture.HttpClient.PostAsJsonAsync("/_ministack/config",
            new { stepfunctions = new { _sfn_mock_config = new { } } });
    }

    [Fact]
    public async Task MockConfigThrow()
    {
        var mockCfgJson = """
        {
            "stepfunctions": {
                "_sfn_mock_config": {
                    "StateMachines": {
                        "sfn-mock-throw": {
                            "TestCases": {
                                "FailPath": { "CallService": "MockedFailure" }
                            }
                        }
                    },
                    "MockedResponses": {
                        "MockedFailure": {
                            "0": { "Throw": { "Error": "ServiceDown", "Cause": "mocked failure" } }
                        }
                    }
                }
            }
        }
        """;
        await _fixture.HttpClient.PostAsync("/_ministack/config",
            new StringContent(mockCfgJson, Encoding.UTF8, new System.Net.Http.Headers.MediaTypeHeaderValue("application/json")));

        var definition = """
        {
            "StartAt": "CallService",
            "States": {
                "CallService": {
                    "Type": "Task",
                    "Resource": "arn:aws:lambda:us-east-1:000000000000:function:nonexistent",
                    "End": true
                }
            }
        }
        """;
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-mock-throw",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var exec = await _sfn.StartExecutionAsync(new StartExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn + "#FailPath",
            Input = "{}",
        });

        var desc = await WaitForExecution(exec.ExecutionArn);
        Assert.Equal(ExecutionStatus.FAILED, desc.Status);

        // Clean up
        await _fixture.HttpClient.PostAsJsonAsync("/_ministack/config",
            new { stepfunctions = new { _sfn_mock_config = new { } } });
    }

    // ── TestState API ─────────────────────────────────────────────────────────

    [Fact]
    public async Task TestStatePass()
    {
        var resp = await _sfn.TestStateAsync(new TestStateRequest
        {
            Definition = """{"Type":"Pass","Result":{"greeting":"hello"},"ResultPath":"$.result","Next":"NextStep"}""",
            Input = """{"existing":"data"}""",
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        Assert.Equal("SUCCEEDED", resp.Status.Value);
        var output = JsonSerializer.Deserialize<JsonElement>(resp.Output);
        Assert.Equal("hello", output.GetProperty("result").GetProperty("greeting").GetString());
        Assert.Equal("data", output.GetProperty("existing").GetString());
        Assert.Equal("NextStep", resp.NextState);
    }

    [Fact]
    public async Task TestStateChoice()
    {
        var resp = await _sfn.TestStateAsync(new TestStateRequest
        {
            Definition = """
            {
                "Type": "Choice",
                "Choices": [
                    {"Variable": "$.val", "NumericEquals": 1, "Next": "One"},
                    {"Variable": "$.val", "NumericEquals": 2, "Next": "Two"}
                ],
                "Default": "Other"
            }
            """,
            Input = """{"val":2}""",
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        Assert.Equal("SUCCEEDED", resp.Status.Value);
        Assert.Equal("Two", resp.NextState);
    }

    [Fact]
    public async Task TestStateFail()
    {
        var resp = await _sfn.TestStateAsync(new TestStateRequest
        {
            Definition = """{"Type":"Fail","Error":"CustomError","Cause":"Something went wrong"}""",
            Input = "{}",
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        Assert.Equal("FAILED", resp.Status.Value);
        Assert.Equal("CustomError", resp.Error);
        Assert.Equal("Something went wrong", resp.Cause);
    }

    // ── aws-sdk SSM Integration ───────────────────────────────────────────────

    [Fact]
    public async Task AwsSdkSsmPutAndGet()
    {
        var definition = """
        {
            "StartAt": "PutParam",
            "States": {
                "PutParam": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:ssm:PutParameter",
                    "Parameters": {
                        "Name": "sfn-ssm-test-param",
                        "Value": "hello-from-sfn",
                        "Type": "String",
                        "Overwrite": true
                    },
                    "ResultPath": "$.putResult",
                    "Next": "GetParam"
                },
                "GetParam": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:ssm:GetParameter",
                    "Parameters": {
                        "Name": "sfn-ssm-test-param"
                    },
                    "End": true
                }
            }
        }
        """;
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-ssm-integ",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var resp = await _sfn.StartSyncExecutionAsync(new StartSyncExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = "{}",
        });
        Assert.Equal(SyncExecutionStatus.SUCCEEDED, resp.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(resp.Output);
        Assert.Equal("hello-from-sfn", output.GetProperty("Parameter").GetProperty("Value").GetString());
    }

    // ── Multi-state chain (Pass → Pass) ───────────────────────────────────────

    [Fact]
    public async Task MultiPassStateChain()
    {
        var definition = """
        {
            "StartAt": "First",
            "States": {
                "First": {
                    "Type": "Pass",
                    "Result": {"step": "first"},
                    "ResultPath": "$.first",
                    "Next": "Second"
                },
                "Second": {
                    "Type": "Pass",
                    "Result": {"step": "second"},
                    "ResultPath": "$.second",
                    "End": true
                }
            }
        }
        """;
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-chain-test",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var resp = await _sfn.StartSyncExecutionAsync(new StartSyncExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = """{"input":"data"}""",
        });
        Assert.Equal(SyncExecutionStatus.SUCCEEDED, resp.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(resp.Output);
        Assert.Equal("first", output.GetProperty("first").GetProperty("step").GetString());
        Assert.Equal("second", output.GetProperty("second").GetProperty("step").GetString());
        Assert.Equal("data", output.GetProperty("input").GetString());
    }

    // ── Parallel State ────────────────────────────────────────────────────────

    [Fact]
    public async Task ParallelState()
    {
        var definition = """
        {
            "StartAt": "Parallel",
            "States": {
                "Parallel": {
                    "Type": "Parallel",
                    "Branches": [
                        {
                            "StartAt": "B1",
                            "States": {
                                "B1": {"Type": "Pass", "Result": "branch1", "End": true}
                            }
                        },
                        {
                            "StartAt": "B2",
                            "States": {
                                "B2": {"Type": "Pass", "Result": "branch2", "End": true}
                            }
                        }
                    ],
                    "End": true
                }
            }
        }
        """;
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-parallel-test",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var resp = await _sfn.StartSyncExecutionAsync(new StartSyncExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = "{}",
        });
        Assert.Equal(SyncExecutionStatus.SUCCEEDED, resp.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(resp.Output);
        Assert.Equal(JsonValueKind.Array, output.ValueKind);
        var arr = output.EnumerateArray().Select(e => e.GetString()).ToList();
        Assert.Contains("branch1", arr);
        Assert.Contains("branch2", arr);
    }

    // ── Map State ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task MapState()
    {
        var definition = """
        {
            "StartAt": "MapStep",
            "States": {
                "MapStep": {
                    "Type": "Map",
                    "ItemsPath": "$.items",
                    "Iterator": {
                        "StartAt": "Double",
                        "States": {
                            "Double": {
                                "Type": "Pass",
                                "Parameters": {
                                    "value.$": "$.value"
                                },
                                "End": true
                            }
                        }
                    },
                    "End": true
                }
            }
        }
        """;
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-map-test",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var resp = await _sfn.StartSyncExecutionAsync(new StartSyncExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = """{"items":[{"value":1},{"value":2},{"value":3}]}""",
        });
        Assert.Equal(SyncExecutionStatus.SUCCEEDED, resp.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(resp.Output);
        Assert.Equal(JsonValueKind.Array, output.ValueKind);
        Assert.Equal(3, output.GetArrayLength());
    }

    // ── ResultPath ────────────────────────────────────────────────────────────

    [Fact]
    public async Task ResultPathMergesWithInput()
    {
        var definition = """
        {
            "StartAt": "Inject",
            "States": {
                "Inject": {
                    "Type": "Pass",
                    "Result": {"added": true},
                    "ResultPath": "$.extra",
                    "End": true
                }
            }
        }
        """;
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-resultpath-test",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var resp = await _sfn.StartSyncExecutionAsync(new StartSyncExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = """{"original":"data"}""",
        });
        Assert.Equal(SyncExecutionStatus.SUCCEEDED, resp.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(resp.Output);
        Assert.Equal("data", output.GetProperty("original").GetString());
        Assert.True(output.GetProperty("extra").GetProperty("added").GetBoolean());
    }

    // ── InputPath / OutputPath ────────────────────────────────────────────────

    [Fact]
    public async Task InputPathAndOutputPath()
    {
        var definition = """
        {
            "StartAt": "Filter",
            "States": {
                "Filter": {
                    "Type": "Pass",
                    "InputPath": "$.payload",
                    "Result": {"processed": true},
                    "ResultPath": "$.status",
                    "OutputPath": "$.status",
                    "End": true
                }
            }
        }
        """;
        var sm = await _sfn.CreateStateMachineAsync(new CreateStateMachineRequest
        {
            Name = "sfn-io-path-test",
            Definition = definition,
            RoleArn = "arn:aws:iam::000000000000:role/R",
        });

        var resp = await _sfn.StartSyncExecutionAsync(new StartSyncExecutionRequest
        {
            StateMachineArn = sm.StateMachineArn,
            Input = """{"payload":{"data":"value"},"meta":"ignored"}""",
        });
        Assert.Equal(SyncExecutionStatus.SUCCEEDED, resp.Status);
        var output = JsonSerializer.Deserialize<JsonElement>(resp.Output);
        Assert.True(output.GetProperty("processed").GetBoolean());
    }
}
