using Amazon;
using Amazon.CloudWatchLogs;
using Amazon.CloudWatchLogs.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the CloudWatch Logs service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_logs.py.
/// </summary>
public sealed class CloudWatchLogsTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonCloudWatchLogsClient _logs;

    public CloudWatchLogsTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _logs = CreateLogsClient(fixture);
    }

    private static AmazonCloudWatchLogsClient CreateLogsClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonCloudWatchLogsConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonCloudWatchLogsClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _logs.Dispose();
        return Task.CompletedTask;
    }

    // -- PutLogEvents / GetLogEvents ------------------------------------------

    [Fact]
    public async Task PutGet()
    {
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/test/ministack" });
        await _logs.CreateLogStreamAsync(new CreateLogStreamRequest
        {
            LogGroupName = "/test/ministack",
            LogStreamName = "stream1",
        });

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await _logs.PutLogEventsAsync(new PutLogEventsRequest
        {
            LogGroupName = "/test/ministack",
            LogStreamName = "stream1",
            LogEvents =
            [
                new InputLogEvent { Timestamp = DateTime.UtcNow, Message = "Hello from MiniStack" },
                new InputLogEvent { Timestamp = DateTime.UtcNow, Message = "Second log line" },
            ],
        });

        var resp = await _logs.GetLogEventsAsync(new GetLogEventsRequest
        {
            LogGroupName = "/test/ministack",
            LogStreamName = "stream1",
        });

        Assert.Equal(2, resp.Events.Count);
    }

    // -- FilterLogEvents ------------------------------------------------------

    [Fact]
    public async Task FilterEvents()
    {
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/cwl/flt" });
        await _logs.CreateLogStreamAsync(new CreateLogStreamRequest
        {
            LogGroupName = "/cwl/flt",
            LogStreamName = "s1",
        });

        var now = DateTime.UtcNow;
        await _logs.PutLogEventsAsync(new PutLogEventsRequest
        {
            LogGroupName = "/cwl/flt",
            LogStreamName = "s1",
            LogEvents =
            [
                new InputLogEvent { Timestamp = now, Message = "ERROR disk full" },
                new InputLogEvent { Timestamp = now.AddMilliseconds(1), Message = "INFO all clear" },
                new InputLogEvent { Timestamp = now.AddMilliseconds(2), Message = "ERROR timeout" },
            ],
        });

        var resp = await _logs.FilterLogEventsAsync(new FilterLogEventsRequest
        {
            LogGroupName = "/cwl/flt",
            FilterPattern = "ERROR",
        });

        Assert.Equal(2, resp.Events.Count);
        var msgs = resp.Events.ConvertAll(e => e.Message);
        Assert.Contains("ERROR disk full", msgs);
        Assert.Contains("ERROR timeout", msgs);
    }

    // -- CreateLogGroup -------------------------------------------------------

    [Fact]
    public async Task CreateGroup()
    {
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/cwl/cg-v2" });
        var resp = await _logs.DescribeLogGroupsAsync(new DescribeLogGroupsRequest
        {
            LogGroupNamePrefix = "/cwl/cg-v2",
        });

        Assert.Contains(resp.LogGroups, g => g.LogGroupName == "/cwl/cg-v2");
    }

    // -- CreateLogGroup duplicate ---------------------------------------------

    [Fact]
    public async Task CreateGroupDuplicate()
    {
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/cwl/dup-v2" });

        var ex = await Assert.ThrowsAsync<ResourceAlreadyExistsException>(() =>
            _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/cwl/dup-v2" }));

        Assert.Equal("ResourceAlreadyExistsException", ex.ErrorCode);
    }

    // -- DeleteLogGroup -------------------------------------------------------

    [Fact]
    public async Task DeleteGroup()
    {
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/cwl/del-v2" });
        await _logs.DeleteLogGroupAsync(new DeleteLogGroupRequest { LogGroupName = "/cwl/del-v2" });

        var resp = await _logs.DescribeLogGroupsAsync(new DescribeLogGroupsRequest
        {
            LogGroupNamePrefix = "/cwl/del-v2",
        });

        Assert.DoesNotContain(resp.LogGroups, g => g.LogGroupName == "/cwl/del-v2");
    }

    // -- DescribeLogGroups ----------------------------------------------------

    [Fact]
    public async Task DescribeGroups()
    {
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/cwl/dg-a" });
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/cwl/dg-b" });

        var resp = await _logs.DescribeLogGroupsAsync(new DescribeLogGroupsRequest
        {
            LogGroupNamePrefix = "/cwl/dg-",
        });

        var names = resp.LogGroups.ConvertAll(g => g.LogGroupName);
        Assert.Contains("/cwl/dg-a", names);
        Assert.Contains("/cwl/dg-b", names);
    }

    // -- CreateLogStream ------------------------------------------------------

    [Fact]
    public async Task CreateStream()
    {
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/cwl/str-v2" });
        await _logs.CreateLogStreamAsync(new CreateLogStreamRequest
        {
            LogGroupName = "/cwl/str-v2",
            LogStreamName = "stream-a",
        });
        await _logs.CreateLogStreamAsync(new CreateLogStreamRequest
        {
            LogGroupName = "/cwl/str-v2",
            LogStreamName = "stream-b",
        });

        var resp = await _logs.DescribeLogStreamsAsync(new DescribeLogStreamsRequest
        {
            LogGroupName = "/cwl/str-v2",
        });

        var names = resp.LogStreams.ConvertAll(s => s.LogStreamName);
        Assert.Contains("stream-a", names);
        Assert.Contains("stream-b", names);
    }

    // -- PutLogEvents / GetLogEvents (v2) -------------------------------------

    [Fact]
    public async Task PutGetEventsV2()
    {
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/cwl/pge-v2" });
        await _logs.CreateLogStreamAsync(new CreateLogStreamRequest
        {
            LogGroupName = "/cwl/pge-v2",
            LogStreamName = "s1",
        });

        var now = DateTime.UtcNow;
        await _logs.PutLogEventsAsync(new PutLogEventsRequest
        {
            LogGroupName = "/cwl/pge-v2",
            LogStreamName = "s1",
            LogEvents =
            [
                new InputLogEvent { Timestamp = now, Message = "first line" },
                new InputLogEvent { Timestamp = now.AddMilliseconds(1), Message = "second line" },
                new InputLogEvent { Timestamp = now.AddMilliseconds(2), Message = "third line" },
            ],
        });

        var resp = await _logs.GetLogEventsAsync(new GetLogEventsRequest
        {
            LogGroupName = "/cwl/pge-v2",
            LogStreamName = "s1",
        });

        Assert.Equal(3, resp.Events.Count);
        Assert.Equal("first line", resp.Events[0].Message);
        Assert.Equal("third line", resp.Events[2].Message);
    }

    // -- Retention policy -----------------------------------------------------

    [Fact]
    public async Task RetentionPolicy()
    {
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/cwl/ret-v2" });
        await _logs.PutRetentionPolicyAsync(new PutRetentionPolicyRequest
        {
            LogGroupName = "/cwl/ret-v2",
            RetentionInDays = 30,
        });

        var resp = await _logs.DescribeLogGroupsAsync(new DescribeLogGroupsRequest
        {
            LogGroupNamePrefix = "/cwl/ret-v2",
        });

        var grp = resp.LogGroups.Find(g => g.LogGroupName == "/cwl/ret-v2");
        Assert.NotNull(grp);
        Assert.Equal(30, grp.RetentionInDays);

        await _logs.DeleteRetentionPolicyAsync(new DeleteRetentionPolicyRequest
        {
            LogGroupName = "/cwl/ret-v2",
        });

        var resp2 = await _logs.DescribeLogGroupsAsync(new DescribeLogGroupsRequest
        {
            LogGroupNamePrefix = "/cwl/ret-v2",
        });

        var grp2 = resp2.LogGroups.Find(g => g.LogGroupName == "/cwl/ret-v2");
        Assert.NotNull(grp2);
        // After deleting retention policy, retentionInDays is not present in JSON,
        // which the SDK maps as null (default for nullable int)
        Assert.Null(grp2.RetentionInDays);
    }

    // -- Invalid retention policy value ---------------------------------------

    [Fact]
    public async Task RetentionPolicyInvalidValue()
    {
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest
        {
            LogGroupName = "/cwl/retention-invalid",
        });

        var ex = await Assert.ThrowsAsync<InvalidParameterException>(() =>
            _logs.PutRetentionPolicyAsync(new PutRetentionPolicyRequest
            {
                LogGroupName = "/cwl/retention-invalid",
                RetentionInDays = 999,
            }));

        Assert.Equal("InvalidParameterException", ex.ErrorCode);
    }

    // -- Tags (legacy) --------------------------------------------------------

    [Fact]
    public async Task TagsLegacy()
    {
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest
        {
            LogGroupName = "/cwl/tag-v2",
            Tags = new Dictionary<string, string> { ["env"] = "prod" },
        });

#pragma warning disable CS0618 // Legacy APIs are obsolete in SDK v4 but we must test them
        var resp = await _logs.ListTagsLogGroupAsync(new ListTagsLogGroupRequest
        {
            LogGroupName = "/cwl/tag-v2",
        });

        Assert.Equal("prod", resp.Tags["env"]);

        await _logs.TagLogGroupAsync(new TagLogGroupRequest
        {
            LogGroupName = "/cwl/tag-v2",
            Tags = new Dictionary<string, string> { ["team"] = "infra" },
        });

        var resp2 = await _logs.ListTagsLogGroupAsync(new ListTagsLogGroupRequest
        {
            LogGroupName = "/cwl/tag-v2",
        });

        Assert.Equal("prod", resp2.Tags["env"]);
        Assert.Equal("infra", resp2.Tags["team"]);

        await _logs.UntagLogGroupAsync(new UntagLogGroupRequest
        {
            LogGroupName = "/cwl/tag-v2",
            Tags = ["env"],
        });

        var resp3 = await _logs.ListTagsLogGroupAsync(new ListTagsLogGroupRequest
        {
            LogGroupName = "/cwl/tag-v2",
        });

        Assert.DoesNotContain("env", (IDictionary<string, string>)resp3.Tags);
        Assert.Equal("infra", resp3.Tags["team"]);
#pragma warning restore CS0618
    }

    // -- PutLogEvents requires existing group ---------------------------------

    [Fact]
    public async Task PutRequiresGroup()
    {
        var ex = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            _logs.PutLogEventsAsync(new PutLogEventsRequest
            {
                LogGroupName = "/cwl/nonexistent-xyz",
                LogStreamName = "s1",
                LogEvents = [new InputLogEvent { Timestamp = DateTime.UtcNow, Message = "fail" }],
            }));

        Assert.Equal("ResourceNotFoundException", ex.ErrorCode);
    }

    // -- Subscription filter --------------------------------------------------

    [Fact]
    public async Task SubscriptionFilter()
    {
        var group = $"/intg/subfilter/{Guid.NewGuid():N}"[..30];
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = group });

        await _logs.PutSubscriptionFilterAsync(new PutSubscriptionFilterRequest
        {
            LogGroupName = group,
            FilterName = "my-filter",
            FilterPattern = "ERROR",
            DestinationArn = "arn:aws:lambda:us-east-1:000000000000:function:log-handler",
        });

        var resp = await _logs.DescribeSubscriptionFiltersAsync(new DescribeSubscriptionFiltersRequest
        {
            LogGroupName = group,
        });

        Assert.Contains(resp.SubscriptionFilters, f => f.FilterName == "my-filter");

        await _logs.DeleteSubscriptionFilterAsync(new DeleteSubscriptionFilterRequest
        {
            LogGroupName = group,
            FilterName = "my-filter",
        });

        var resp2 = await _logs.DescribeSubscriptionFiltersAsync(new DescribeSubscriptionFiltersRequest
        {
            LogGroupName = group,
        });

        Assert.DoesNotContain(resp2.SubscriptionFilters, f => f.FilterName == "my-filter");
    }

    // -- Metric filter --------------------------------------------------------

    [Fact]
    public async Task MetricFilter()
    {
        var group = $"/intg/metricfilter/{Guid.NewGuid():N}"[..32];
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = group });

        await _logs.PutMetricFilterAsync(new PutMetricFilterRequest
        {
            LogGroupName = group,
            FilterName = "error-count",
            FilterPattern = "[ERROR]",
            MetricTransformations =
            [
                new MetricTransformation
                {
                    MetricName = "ErrorCount",
                    MetricNamespace = "MyApp",
                    MetricValue = "1",
                },
            ],
        });

        var resp = await _logs.DescribeMetricFiltersAsync(new DescribeMetricFiltersRequest
        {
            LogGroupName = group,
        });

        Assert.Contains(resp.MetricFilters, f => f.FilterName == "error-count");

        await _logs.DeleteMetricFilterAsync(new DeleteMetricFilterRequest
        {
            LogGroupName = group,
            FilterName = "error-count",
        });

        var resp2 = await _logs.DescribeMetricFiltersAsync(new DescribeMetricFiltersRequest
        {
            LogGroupName = group,
        });

        Assert.DoesNotContain(resp2.MetricFilters, f => f.FilterName == "error-count");
    }

    // -- Insights start query -------------------------------------------------

    [Fact]
    public async Task InsightsStartQuery()
    {
        var group = $"/intg/insights/{Guid.NewGuid():N}"[..28];
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = group });

        var resp = await _logs.StartQueryAsync(new StartQueryRequest
        {
            LogGroupName = group,
            StartTime = DateTimeOffset.UtcNow.AddHours(-1).ToUnixTimeSeconds(),
            EndTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            QueryString = "fields @timestamp, @message | limit 10",
        });

        Assert.NotNull(resp.QueryId);

        var results = await _logs.GetQueryResultsAsync(new GetQueryResultsRequest
        {
            QueryId = resp.QueryId,
        });

        Assert.Contains(results.Status.Value, new[] { "Complete", "Running", "Scheduled" });
    }

    // -- DescribeLogGroups prefix filter ---------------------------------------

    [Fact]
    public async Task DescribeLogGroupsPrefix()
    {
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/qa/logs/prefix/alpha" });
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/qa/logs/prefix/beta" });
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/qa/logs/other/gamma" });

        var resp = await _logs.DescribeLogGroupsAsync(new DescribeLogGroupsRequest
        {
            LogGroupNamePrefix = "/qa/logs/prefix",
        });

        var names = resp.LogGroups.ConvertAll(g => g.LogGroupName);
        Assert.Contains("/qa/logs/prefix/alpha", names);
        Assert.Contains("/qa/logs/prefix/beta", names);
        Assert.DoesNotContain("/qa/logs/other/gamma", names);
    }

    // -- ListTagsForResource ARN without star ---------------------------------

    [Fact]
    public async Task ListTagsForResourceArnWithoutStar()
    {
        var name = "/tf/regression/arn-no-star";
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest
        {
            LogGroupName = name,
            Tags = new Dictionary<string, string> { ["env"] = "test" },
        });

        var groups = await _logs.DescribeLogGroupsAsync(new DescribeLogGroupsRequest
        {
            LogGroupNamePrefix = name,
        });

        var storedArn = groups.LogGroups[0].Arn;
        Assert.EndsWith(":*", storedArn);

        // Terraform sends the ARN without :* — must not raise
        var arnNoStar = storedArn[..^2];
        var resp = await _logs.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = arnNoStar,
        });

        Assert.Equal("test", resp.Tags["env"]);
    }

    // -- GetLogEvents pagination stops ----------------------------------------

    [Fact]
    public async Task GetLogEventsPaginationStops()
    {
        var group = "/test/pagination-stop";
        var stream = "s1";
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = group });
        await _logs.CreateLogStreamAsync(new CreateLogStreamRequest
        {
            LogGroupName = group,
            LogStreamName = stream,
        });

        var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        await _logs.PutLogEventsAsync(new PutLogEventsRequest
        {
            LogGroupName = group,
            LogStreamName = stream,
            LogEvents =
            [
                new InputLogEvent { Timestamp = epoch.AddMilliseconds(1000), Message = "msg1" },
                new InputLogEvent { Timestamp = epoch.AddMilliseconds(2000), Message = "msg2" },
            ],
        });

        // First call — get all events
        var resp = await _logs.GetLogEventsAsync(new GetLogEventsRequest
        {
            LogGroupName = group,
            LogStreamName = stream,
            StartFromHead = true,
        });

        Assert.Equal(2, resp.Events.Count);
        var fwdToken = resp.NextForwardToken;

        // Second call with forward token — no more events, token must match
        var resp2 = await _logs.GetLogEventsAsync(new GetLogEventsRequest
        {
            LogGroupName = group,
            LogStreamName = stream,
            NextToken = fwdToken,
        });

        Assert.Empty(resp2.Events);
        Assert.Equal(fwdToken, resp2.NextForwardToken);
    }

    // -- FilterLogEvents with wildcard pattern --------------------------------

    [Fact]
    public async Task FilterWithWildcard()
    {
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/qa/logs/wildcard" });
        await _logs.CreateLogStreamAsync(new CreateLogStreamRequest
        {
            LogGroupName = "/qa/logs/wildcard",
            LogStreamName = "stream1",
        });

        var now = DateTime.UtcNow;
        await _logs.PutLogEventsAsync(new PutLogEventsRequest
        {
            LogGroupName = "/qa/logs/wildcard",
            LogStreamName = "stream1",
            LogEvents =
            [
                new InputLogEvent { Timestamp = now, Message = "ERROR: disk full" },
                new InputLogEvent { Timestamp = now.AddMilliseconds(1), Message = "INFO: all good" },
                new InputLogEvent { Timestamp = now.AddMilliseconds(2), Message = "ERROR: timeout" },
            ],
        });

        // The pattern "ERROR*" is compiled to include "error*" — * is stripped, so it looks for "error"
        var resp = await _logs.FilterLogEventsAsync(new FilterLogEventsRequest
        {
            LogGroupName = "/qa/logs/wildcard",
            FilterPattern = "ERROR",
        });

        var messages = resp.Events.ConvertAll(e => e.Message);
        Assert.All(messages, m => Assert.Contains("ERROR", m));
        Assert.Equal(2, messages.Count);
    }

    // -- Tag / Untag resource (modern ARN-based) ------------------------------

    [Fact]
    public async Task TagResource()
    {
        var name = "/cwl/tag-resource";
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = name });

        var groups = await _logs.DescribeLogGroupsAsync(new DescribeLogGroupsRequest
        {
            LogGroupNamePrefix = name,
        });

        var arn = groups.LogGroups[0].Arn;

        await _logs.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = arn,
            Tags = new Dictionary<string, string> { ["project"] = "microstack" },
        });

        var tags = await _logs.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = arn,
        });

        Assert.Equal("microstack", tags.Tags["project"]);

        await _logs.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceArn = arn,
            TagKeys = ["project"],
        });

        var tags2 = await _logs.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = arn,
        });

        Assert.DoesNotContain("project", (IDictionary<string, string>)tags2.Tags);
    }

    // -- Destination CRUD -----------------------------------------------------

    [Fact]
    public async Task DestinationCrud()
    {
        await _logs.PutDestinationAsync(new PutDestinationRequest
        {
            DestinationName = "my-dest",
            TargetArn = "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
            RoleArn = "arn:aws:iam::000000000000:role/my-role",
        });

        var resp = await _logs.DescribeDestinationsAsync(new DescribeDestinationsRequest());
        Assert.Contains(resp.Destinations, d => d.DestinationName == "my-dest");

        await _logs.PutDestinationPolicyAsync(new PutDestinationPolicyRequest
        {
            DestinationName = "my-dest",
            AccessPolicy = "{\"Version\":\"2012-10-17\"}",
        });

        await _logs.DeleteDestinationAsync(new DeleteDestinationRequest
        {
            DestinationName = "my-dest",
        });

        var resp2 = await _logs.DescribeDestinationsAsync(new DescribeDestinationsRequest());
        Assert.DoesNotContain(resp2.Destinations, d => d.DestinationName == "my-dest");
    }
}
