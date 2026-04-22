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
public sealed class CloudWatchLogsTests(MicroStackFixture fixture) : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly AmazonCloudWatchLogsClient _logs = CreateLogsClient(fixture);

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

    public async ValueTask InitializeAsync()
    {
        await fixture.HttpClient.PostAsync("/_microstack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _logs.Dispose();
        return ValueTask.CompletedTask;
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

        resp.Events.Count.ShouldBe(2);
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

        resp.Events.Count.ShouldBe(2);
        var msgs = resp.Events.ConvertAll(e => e.Message);
        msgs.ShouldContain("ERROR disk full");
        msgs.ShouldContain("ERROR timeout");
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

        resp.LogGroups.ShouldContain(g => g.LogGroupName == "/cwl/cg-v2");
    }

    // -- CreateLogGroup duplicate ---------------------------------------------

    [Fact]
    public async Task CreateGroupDuplicate()
    {
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/cwl/dup-v2" });

        var ex = await Should.ThrowAsync<ResourceAlreadyExistsException>(() =>
            _logs.CreateLogGroupAsync(new CreateLogGroupRequest { LogGroupName = "/cwl/dup-v2" }));

        ex.ErrorCode.ShouldBe("ResourceAlreadyExistsException");
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

        resp.LogGroups.ShouldNotContain(g => g.LogGroupName == "/cwl/del-v2");
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
        names.ShouldContain("/cwl/dg-a");
        names.ShouldContain("/cwl/dg-b");
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
        names.ShouldContain("stream-a");
        names.ShouldContain("stream-b");
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

        resp.Events.Count.ShouldBe(3);
        resp.Events[0].Message.ShouldBe("first line");
        resp.Events[2].Message.ShouldBe("third line");
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
        grp.ShouldNotBeNull();
        grp.RetentionInDays.ShouldBe(30);

        await _logs.DeleteRetentionPolicyAsync(new DeleteRetentionPolicyRequest
        {
            LogGroupName = "/cwl/ret-v2",
        });

        var resp2 = await _logs.DescribeLogGroupsAsync(new DescribeLogGroupsRequest
        {
            LogGroupNamePrefix = "/cwl/ret-v2",
        });

        var grp2 = resp2.LogGroups.Find(g => g.LogGroupName == "/cwl/ret-v2");
        grp2.ShouldNotBeNull();
        // After deleting retention policy, retentionInDays is not present in JSON,
        // which the SDK maps as null (default for nullable int)
        grp2.RetentionInDays.ShouldBeNull();
    }

    // -- Invalid retention policy value ---------------------------------------

    [Fact]
    public async Task RetentionPolicyInvalidValue()
    {
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest
        {
            LogGroupName = "/cwl/retention-invalid",
        });

        var ex = await Should.ThrowAsync<InvalidParameterException>(() =>
            _logs.PutRetentionPolicyAsync(new PutRetentionPolicyRequest
            {
                LogGroupName = "/cwl/retention-invalid",
                RetentionInDays = 999,
            }));

        ex.ErrorCode.ShouldBe("InvalidParameterException");
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

        resp.Tags["env"].ShouldBe("prod");

        await _logs.TagLogGroupAsync(new TagLogGroupRequest
        {
            LogGroupName = "/cwl/tag-v2",
            Tags = new Dictionary<string, string> { ["team"] = "infra" },
        });

        var resp2 = await _logs.ListTagsLogGroupAsync(new ListTagsLogGroupRequest
        {
            LogGroupName = "/cwl/tag-v2",
        });

        resp2.Tags["env"].ShouldBe("prod");
        resp2.Tags["team"].ShouldBe("infra");

        await _logs.UntagLogGroupAsync(new UntagLogGroupRequest
        {
            LogGroupName = "/cwl/tag-v2",
            Tags = ["env"],
        });

        var resp3 = await _logs.ListTagsLogGroupAsync(new ListTagsLogGroupRequest
        {
            LogGroupName = "/cwl/tag-v2",
        });

        resp3.Tags.ShouldNotContainKey("env");
        resp3.Tags["team"].ShouldBe("infra");
#pragma warning restore CS0618
    }

    // -- PutLogEvents requires existing group ---------------------------------

    [Fact]
    public async Task PutRequiresGroup()
    {
        var ex = await Should.ThrowAsync<ResourceNotFoundException>(() =>
            _logs.PutLogEventsAsync(new PutLogEventsRequest
            {
                LogGroupName = "/cwl/nonexistent-xyz",
                LogStreamName = "s1",
                LogEvents = [new InputLogEvent { Timestamp = DateTime.UtcNow, Message = "fail" }],
            }));

        ex.ErrorCode.ShouldBe("ResourceNotFoundException");
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

        resp.SubscriptionFilters.ShouldContain(f => f.FilterName == "my-filter");

        await _logs.DeleteSubscriptionFilterAsync(new DeleteSubscriptionFilterRequest
        {
            LogGroupName = group,
            FilterName = "my-filter",
        });

        var resp2 = await _logs.DescribeSubscriptionFiltersAsync(new DescribeSubscriptionFiltersRequest
        {
            LogGroupName = group,
        });

        resp2.SubscriptionFilters.ShouldNotContain(f => f.FilterName == "my-filter");
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

        resp.MetricFilters.ShouldContain(f => f.FilterName == "error-count");

        await _logs.DeleteMetricFilterAsync(new DeleteMetricFilterRequest
        {
            LogGroupName = group,
            FilterName = "error-count",
        });

        var resp2 = await _logs.DescribeMetricFiltersAsync(new DescribeMetricFiltersRequest
        {
            LogGroupName = group,
        });

        resp2.MetricFilters.ShouldNotContain(f => f.FilterName == "error-count");
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

        resp.QueryId.ShouldNotBeNull();

        var results = await _logs.GetQueryResultsAsync(new GetQueryResultsRequest
        {
            QueryId = resp.QueryId,
        });

        new[] { "Complete", "Running", "Scheduled" }.ShouldContain(results.Status.Value);
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
        names.ShouldContain("/qa/logs/prefix/alpha");
        names.ShouldContain("/qa/logs/prefix/beta");
        names.ShouldNotContain("/qa/logs/other/gamma");
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
        storedArn.ShouldEndWith(":*");

        // Terraform sends the ARN without :* — must not raise
        var arnNoStar = storedArn[..^2];
        var resp = await _logs.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = arnNoStar,
        });

        resp.Tags["env"].ShouldBe("test");
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

        resp.Events.Count.ShouldBe(2);
        var fwdToken = resp.NextForwardToken;

        // Second call with forward token — no more events, token must match
        var resp2 = await _logs.GetLogEventsAsync(new GetLogEventsRequest
        {
            LogGroupName = group,
            LogStreamName = stream,
            NextToken = fwdToken,
        });

        resp2.Events.ShouldBeEmpty();
        resp2.NextForwardToken.ShouldBe(fwdToken);
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
        messages.ShouldAllBe(m => m.Contains("ERROR"));
        messages.Count.ShouldBe(2);
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

        tags.Tags["project"].ShouldBe("microstack");

        await _logs.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceArn = arn,
            TagKeys = ["project"],
        });

        var tags2 = await _logs.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = arn,
        });

        tags2.Tags.ShouldNotContainKey("project");
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
        resp.Destinations.ShouldContain(d => d.DestinationName == "my-dest");

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
        resp2.Destinations.ShouldNotContain(d => d.DestinationName == "my-dest");
    }
}
