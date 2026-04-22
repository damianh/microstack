using Amazon;
using Amazon.CloudWatch;
using Amazon.CloudWatch.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the CloudWatch Metrics service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_cloudwatch.py.
/// </summary>
public sealed class CloudWatchTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonCloudWatchClient _cw;

    public CloudWatchTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _cw = CreateCloudWatchClient(fixture);
    }

    private static AmazonCloudWatchClient CreateCloudWatchClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonCloudWatchConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonCloudWatchClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async ValueTask InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _cw.Dispose();
        return ValueTask.CompletedTask;
    }

    // ── PutMetricData + ListMetrics ──────────────────────────────────────────

    [Fact]
    public async Task PutMetricDataAndListMetrics()
    {
        await _cw.PutMetricDataAsync(new PutMetricDataRequest
        {
            Namespace = "MyApp",
            MetricData =
            [
                new MetricDatum { MetricName = "RequestCount", Value = 42.0, Unit = StandardUnit.Count },
                new MetricDatum { MetricName = "Latency", Value = 123.5, Unit = StandardUnit.Milliseconds },
            ],
        });

        var resp = await _cw.ListMetricsAsync(new ListMetricsRequest { Namespace = "MyApp" });
        var names = resp.Metrics.ConvertAll(m => m.MetricName);
        names.ShouldContain("RequestCount");
        names.ShouldContain("Latency");
    }

    // ── PutMetricAlarm + DescribeAlarms ──────────────────────────────────────

    [Fact]
    public async Task PutMetricAlarmAndDescribeAlarms()
    {
        await _cw.PutMetricAlarmAsync(new PutMetricAlarmRequest
        {
            AlarmName = "high-latency",
            MetricName = "Latency",
            Namespace = "MyApp",
            Statistic = Statistic.Average,
            Period = 60,
            EvaluationPeriods = 1,
            Threshold = 500.0,
            ComparisonOperator = ComparisonOperator.GreaterThanThreshold,
        });

        var resp = await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            AlarmNames = ["high-latency"],
        });

        resp.MetricAlarms.ShouldHaveSingleItem();
    }

    // ── Dashboard CRUD ──────────────────────────────────────────────────────

    [Fact]
    public async Task DashboardCrud()
    {
        var body = JsonSerializer.Serialize(new
        {
            widgets = new[]
            {
                new { type = "text", properties = new { markdown = "Hello" } },
            },
        });

        await _cw.PutDashboardAsync(new PutDashboardRequest
        {
            DashboardName = "test-dash",
            DashboardBody = body,
        });

        var getResp = await _cw.GetDashboardAsync(new GetDashboardRequest
        {
            DashboardName = "test-dash",
        });

        getResp.DashboardName.ShouldBe("test-dash");
        getResp.DashboardBody.ShouldNotBeNull();

        var listResp = await _cw.ListDashboardsAsync(new ListDashboardsRequest());
        listResp.DashboardEntries.ShouldContain(d => d.DashboardName == "test-dash");

        await _cw.DeleteDashboardsAsync(new DeleteDashboardsRequest
        {
            DashboardNames = ["test-dash"],
        });

        var listResp2 = await _cw.ListDashboardsAsync(new ListDashboardsRequest());
        listResp2.DashboardEntries.ShouldNotContain(d => d.DashboardName == "test-dash");
    }

    // ── PutMetricData with Dimensions + ListMetrics filtered ─────────────────

    [Fact]
    public async Task PutListMetricsWithDimensions()
    {
        await _cw.PutMetricDataAsync(new PutMetricDataRequest
        {
            Namespace = "CWv2",
            MetricData =
            [
                new MetricDatum
                {
                    MetricName = "Reqs",
                    Value = 100.0,
                    Unit = StandardUnit.Count,
                    Dimensions = [new Dimension { Name = "API", Value = "/users" }],
                },
                new MetricDatum { MetricName = "Errs", Value = 5.0, Unit = StandardUnit.Count },
            ],
        });

        var resp = await _cw.ListMetricsAsync(new ListMetricsRequest { Namespace = "CWv2" });
        var names = resp.Metrics.ConvertAll(m => m.MetricName);
        names.ShouldContain("Reqs");
        names.ShouldContain("Errs");

        var filtered = await _cw.ListMetricsAsync(new ListMetricsRequest
        {
            Namespace = "CWv2",
            MetricName = "Reqs",
        });
        filtered.Metrics.ShouldAllBe(m => m.MetricName == "Reqs");
    }

    // ── GetMetricStatistics ─────────────────────────────────────────────────

    [Fact]
    public async Task GetMetricStatistics()
    {
        await _cw.PutMetricDataAsync(new PutMetricDataRequest
        {
            Namespace = "CWStat2",
            MetricData =
            [
                new MetricDatum { MetricName = "Duration", Value = 100.0, Unit = StandardUnit.Milliseconds },
                new MetricDatum { MetricName = "Duration", Value = 200.0, Unit = StandardUnit.Milliseconds },
            ],
        });

        var now = DateTimeOffset.UtcNow;
        var resp = await _cw.GetMetricStatisticsAsync(new GetMetricStatisticsRequest
        {
            Namespace = "CWStat2",
            MetricName = "Duration",
            Period = 60,
            StartTime = now.AddMinutes(-10).UtcDateTime,
            EndTime = now.AddMinutes(10).UtcDateTime,
            Statistics = [Statistic.Average, Statistic.Sum, Statistic.SampleCount, Statistic.Minimum, Statistic.Maximum],
        });

        resp.Datapoints.ShouldNotBeEmpty();
        var dp = resp.Datapoints[0];
        (dp.Average > 0).ShouldBe(true);
        (dp.Sum > 0).ShouldBe(true);
        (dp.SampleCount > 0).ShouldBe(true);
        (dp.Minimum > 0).ShouldBe(true);
        (dp.Maximum > 0).ShouldBe(true);
    }

    // ── PutMetricAlarm with details ─────────────────────────────────────────

    [Fact]
    public async Task PutMetricAlarmDetails()
    {
        await _cw.PutMetricAlarmAsync(new PutMetricAlarmRequest
        {
            AlarmName = "cw-v2-high-err",
            MetricName = "Errors",
            Namespace = "CWv2Alarms",
            Statistic = Statistic.Sum,
            Period = 300,
            EvaluationPeriods = 2,
            Threshold = 10.0,
            ComparisonOperator = ComparisonOperator.GreaterThanOrEqualToThreshold,
            AlarmActions = ["arn:aws:sns:us-east-1:000000000000:alarm-topic"],
            AlarmDescription = "Fires when errors >= 10",
        });

        var resp = await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            AlarmNames = ["cw-v2-high-err"],
        });

        var alarm = resp.MetricAlarms[0];
        alarm.AlarmName.ShouldBe("cw-v2-high-err");
        alarm.Threshold.ShouldBe(10.0);
        alarm.ComparisonOperator.ShouldBe(ComparisonOperator.GreaterThanOrEqualToThreshold);
        alarm.EvaluationPeriods.ShouldBe(2);
    }

    // ── DescribeAlarms with prefix filter ────────────────────────────────────

    [Fact]
    public async Task DescribeAlarmsWithPrefix()
    {
        for (var i = 0; i < 3; i++)
        {
            await _cw.PutMetricAlarmAsync(new PutMetricAlarmRequest
            {
                AlarmName = $"cw-da-v2-{i}",
                MetricName = "M",
                Namespace = "N",
                Statistic = Statistic.Sum,
                Period = 60,
                EvaluationPeriods = 1,
                Threshold = i,
                ComparisonOperator = ComparisonOperator.GreaterThanThreshold,
            });
        }

        var resp = await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            AlarmNamePrefix = "cw-da-v2-",
        });

        var names = resp.MetricAlarms.ConvertAll(a => a.AlarmName);
        for (var i = 0; i < 3; i++)
            names.ShouldContain($"cw-da-v2-{i}");
    }

    // ── DeleteAlarms ────────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteAlarms()
    {
        await _cw.PutMetricAlarmAsync(new PutMetricAlarmRequest
        {
            AlarmName = "cw-del-v2",
            MetricName = "M",
            Namespace = "N",
            Statistic = Statistic.Sum,
            Period = 60,
            EvaluationPeriods = 1,
            Threshold = 1.0,
            ComparisonOperator = ComparisonOperator.GreaterThanThreshold,
        });

        await _cw.DeleteAlarmsAsync(new DeleteAlarmsRequest
        {
            AlarmNames = ["cw-del-v2"],
        });

        var resp = await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            AlarmNames = ["cw-del-v2"],
        });

        resp.MetricAlarms.ShouldBeEmpty();
    }

    // ── SetAlarmState ───────────────────────────────────────────────────────

    [Fact]
    public async Task SetAlarmState()
    {
        await _cw.PutMetricAlarmAsync(new PutMetricAlarmRequest
        {
            AlarmName = "cw-state-v2",
            MetricName = "M",
            Namespace = "N",
            Statistic = Statistic.Sum,
            Period = 60,
            EvaluationPeriods = 1,
            Threshold = 1.0,
            ComparisonOperator = ComparisonOperator.GreaterThanThreshold,
        });

        var initial = (await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            AlarmNames = ["cw-state-v2"],
        })).MetricAlarms[0];
        initial.StateValue.ShouldBe(StateValue.INSUFFICIENT_DATA);

        await _cw.SetAlarmStateAsync(new SetAlarmStateRequest
        {
            AlarmName = "cw-state-v2",
            StateValue = StateValue.ALARM,
            StateReason = "Manual trigger for testing",
        });

        var after = (await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            AlarmNames = ["cw-state-v2"],
        })).MetricAlarms[0];
        after.StateValue.ShouldBe(StateValue.ALARM);
        after.StateReason.ShouldBe("Manual trigger for testing");
    }

    // ── GetMetricData ───────────────────────────────────────────────────────

    [Fact]
    public async Task GetMetricData()
    {
        await _cw.PutMetricDataAsync(new PutMetricDataRequest
        {
            Namespace = "CWData2",
            MetricData =
            [
                new MetricDatum { MetricName = "Hits", Value = 42.0, Unit = StandardUnit.Count },
            ],
        });

        var now = DateTimeOffset.UtcNow;
        var resp = await _cw.GetMetricDataAsync(new GetMetricDataRequest
        {
            MetricDataQueries =
            [
                new MetricDataQuery
                {
                    Id = "q1",
                    MetricStat = new MetricStat
                    {
                        Metric = new Amazon.CloudWatch.Model.Metric
                        {
                            Namespace = "CWData2",
                            MetricName = "Hits",
                        },
                        Period = 60,
                        Stat = "Sum",
                    },
                    ReturnData = true,
                },
            ],
            StartTime = now.AddMinutes(-10).UtcDateTime,
            EndTime = now.AddMinutes(10).UtcDateTime,
        });

        resp.MetricDataResults.ShouldHaveSingleItem();
        resp.MetricDataResults[0].Id.ShouldBe("q1");
        resp.MetricDataResults[0].StatusCode.ShouldBe(StatusCode.Complete);
        resp.MetricDataResults[0].Values.ShouldNotBeEmpty();
    }

    // ── Tags CRUD ───────────────────────────────────────────────────────────

    [Fact]
    public async Task TagsCrud()
    {
        await _cw.PutMetricAlarmAsync(new PutMetricAlarmRequest
        {
            AlarmName = "cw-tag-v2",
            MetricName = "M",
            Namespace = "N",
            Statistic = Statistic.Sum,
            Period = 60,
            EvaluationPeriods = 1,
            Threshold = 1.0,
            ComparisonOperator = ComparisonOperator.GreaterThanThreshold,
        });

        var arn = (await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            AlarmNames = ["cw-tag-v2"],
        })).MetricAlarms[0].AlarmArn;

        await _cw.TagResourceAsync(new TagResourceRequest
        {
            ResourceARN = arn,
            Tags =
            [
                new Tag { Key = "env", Value = "prod" },
                new Tag { Key = "team", Value = "sre" },
            ],
        });

        var resp = await _cw.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceARN = arn,
        });
        var tagMap = resp.Tags.ToDictionary(t => t.Key, t => t.Value);
        tagMap["env"].ShouldBe("prod");
        tagMap["team"].ShouldBe("sre");

        await _cw.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceARN = arn,
            TagKeys = ["env"],
        });

        var resp2 = await _cw.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceARN = arn,
        });
        resp2.Tags.ShouldNotContain(t => t.Key == "env");
        resp2.Tags.ShouldContain(t => t.Key == "team");
    }

    // ── CompositeAlarm ──────────────────────────────────────────────────────

    [Fact]
    public async Task CompositeAlarm()
    {
        var childName = $"intg-child-alarm-{Guid.NewGuid():N}"[..30];
        var compositeName = $"intg-comp-alarm-{Guid.NewGuid():N}"[..30];

        await _cw.PutMetricAlarmAsync(new PutMetricAlarmRequest
        {
            AlarmName = childName,
            ComparisonOperator = ComparisonOperator.GreaterThanThreshold,
            EvaluationPeriods = 1,
            MetricName = "CPUUtilization",
            Namespace = "AWS/EC2",
            Period = 60,
            Statistic = Statistic.Average,
            Threshold = 80.0,
        });

        var childArn = (await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            AlarmNames = [childName],
        })).MetricAlarms[0].AlarmArn;

        await _cw.PutCompositeAlarmAsync(new PutCompositeAlarmRequest
        {
            AlarmName = compositeName,
            AlarmRule = $"ALARM({childArn})",
            AlarmDescription = "composite test",
        });

        var resp = await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            AlarmNames = [compositeName],
            AlarmTypes = [AlarmType.CompositeAlarm],
        });

        resp.CompositeAlarms.ShouldContain(a => a.AlarmName == compositeName);

        await _cw.DeleteAlarmsAsync(new DeleteAlarmsRequest
        {
            AlarmNames = [childName, compositeName],
        });
    }

    // ── DescribeAlarmsForMetric ─────────────────────────────────────────────

    [Fact]
    public async Task DescribeAlarmsForMetric()
    {
        var alarmName = $"intg-afm-{Guid.NewGuid():N}"[..20];

        await _cw.PutMetricAlarmAsync(new PutMetricAlarmRequest
        {
            AlarmName = alarmName,
            ComparisonOperator = ComparisonOperator.GreaterThanThreshold,
            EvaluationPeriods = 1,
            MetricName = "NetworkIn",
            Namespace = "AWS/EC2",
            Period = 60,
            Statistic = Statistic.Sum,
            Threshold = 1000.0,
        });

        var resp = await _cw.DescribeAlarmsForMetricAsync(new DescribeAlarmsForMetricRequest
        {
            MetricName = "NetworkIn",
            Namespace = "AWS/EC2",
        });

        resp.MetricAlarms.ShouldContain(a => a.AlarmName == alarmName);

        await _cw.DeleteAlarmsAsync(new DeleteAlarmsRequest { AlarmNames = [alarmName] });
    }

    // ── DescribeAlarmHistory ────────────────────────────────────────────────

    [Fact]
    public async Task DescribeAlarmHistory()
    {
        var alarmName = $"intg-hist-{Guid.NewGuid():N}"[..20];

        await _cw.PutMetricAlarmAsync(new PutMetricAlarmRequest
        {
            AlarmName = alarmName,
            ComparisonOperator = ComparisonOperator.GreaterThanThreshold,
            EvaluationPeriods = 1,
            MetricName = "DiskReadOps",
            Namespace = "AWS/EC2",
            Period = 60,
            Statistic = Statistic.Average,
            Threshold = 50.0,
        });

        await _cw.SetAlarmStateAsync(new SetAlarmStateRequest
        {
            AlarmName = alarmName,
            StateValue = StateValue.ALARM,
            StateReason = "test",
        });

        var resp = await _cw.DescribeAlarmHistoryAsync(new DescribeAlarmHistoryRequest
        {
            AlarmName = alarmName,
        });

        resp.AlarmHistoryItems.ShouldNotBeEmpty();

        await _cw.DeleteAlarmsAsync(new DeleteAlarmsRequest { AlarmNames = [alarmName] });
    }

    // ── GetMetricData time range ────────────────────────────────────────────

    [Fact]
    public async Task GetMetricDataTimeRange()
    {
        var now = DateTimeOffset.UtcNow;

        await _cw.PutMetricDataAsync(new PutMetricDataRequest
        {
            Namespace = "qa/cw",
            MetricData =
            [
                new MetricDatum { MetricName = "Requests", Value = 100.0, Unit = StandardUnit.Count },
            ],
        });

        var resp = await _cw.GetMetricDataAsync(new GetMetricDataRequest
        {
            MetricDataQueries =
            [
                new MetricDataQuery
                {
                    Id = "m1",
                    MetricStat = new MetricStat
                    {
                        Metric = new Amazon.CloudWatch.Model.Metric
                        {
                            Namespace = "qa/cw",
                            MetricName = "Requests",
                        },
                        Period = 60,
                        Stat = "Sum",
                    },
                },
            ],
            StartTime = now.AddHours(-2).UtcDateTime,
            EndTime = now.AddMinutes(5).UtcDateTime,
        });

        var result = resp.MetricDataResults.Find(r => r.Id == "m1");
        result.ShouldNotBeNull();
        result.StatusCode.ShouldBe(StatusCode.Complete);
        result.Values.ShouldNotBeEmpty();
        (result.Values.Sum() >= 100.0).ShouldBe(true);
    }

    // ── Alarm state transitions ─────────────────────────────────────────────

    [Fact]
    public async Task AlarmStateTransitions()
    {
        await _cw.PutMetricAlarmAsync(new PutMetricAlarmRequest
        {
            AlarmName = "qa-cw-state-alarm",
            MetricName = "Errors",
            Namespace = "qa/cw",
            Statistic = Statistic.Sum,
            Period = 60,
            EvaluationPeriods = 1,
            Threshold = 10.0,
            ComparisonOperator = ComparisonOperator.GreaterThanThreshold,
        });

        await _cw.SetAlarmStateAsync(new SetAlarmStateRequest
        {
            AlarmName = "qa-cw-state-alarm",
            StateValue = StateValue.ALARM,
            StateReason = "Testing",
        });

        var alarms = (await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            AlarmNames = ["qa-cw-state-alarm"],
        })).MetricAlarms;
        alarms[0].StateValue.ShouldBe(StateValue.ALARM);

        await _cw.SetAlarmStateAsync(new SetAlarmStateRequest
        {
            AlarmName = "qa-cw-state-alarm",
            StateValue = StateValue.OK,
            StateReason = "Resolved",
        });

        var alarms2 = (await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            AlarmNames = ["qa-cw-state-alarm"],
        })).MetricAlarms;
        alarms2[0].StateValue.ShouldBe(StateValue.OK);
    }

    // ── ListMetrics namespace filter ────────────────────────────────────────

    [Fact]
    public async Task ListMetricsNamespaceFilter()
    {
        await _cw.PutMetricDataAsync(new PutMetricDataRequest
        {
            Namespace = "qa/ns-a",
            MetricData = [new MetricDatum { MetricName = "MetA", Value = 1.0 }],
        });
        await _cw.PutMetricDataAsync(new PutMetricDataRequest
        {
            Namespace = "qa/ns-b",
            MetricData = [new MetricDatum { MetricName = "MetB", Value = 1.0 }],
        });

        var resp = await _cw.ListMetricsAsync(new ListMetricsRequest { Namespace = "qa/ns-a" });
        var names = resp.Metrics.ConvertAll(m => m.MetricName);
        names.ShouldContain("MetA");
        names.ShouldNotContain("MetB");
    }

    // ── PutMetricData with Values/Counts array ──────────────────────────────

    [Fact]
    public async Task PutMetricDataWithValuesAndCounts()
    {
        await _cw.PutMetricDataAsync(new PutMetricDataRequest
        {
            Namespace = "qa/cw-multi",
            MetricData =
            [
                new MetricDatum
                {
                    MetricName = "Latency",
                    Values = [10.0, 20.0, 30.0],
                    Counts = [1.0, 2.0, 1.0],
                    Unit = StandardUnit.Milliseconds,
                },
            ],
        });

        var resp = await _cw.ListMetricsAsync(new ListMetricsRequest { Namespace = "qa/cw-multi" });
        resp.Metrics.ShouldContain(m => m.MetricName == "Latency");
    }

    // ── EnableAlarmActions / DisableAlarmActions ─────────────────────────────

    [Fact]
    public async Task EnableDisableAlarmActions()
    {
        await _cw.PutMetricAlarmAsync(new PutMetricAlarmRequest
        {
            AlarmName = "cw-en-dis",
            MetricName = "M",
            Namespace = "N",
            Statistic = Statistic.Sum,
            Period = 60,
            EvaluationPeriods = 1,
            Threshold = 1.0,
            ComparisonOperator = ComparisonOperator.GreaterThanThreshold,
        });

        await _cw.DisableAlarmActionsAsync(new DisableAlarmActionsRequest
        {
            AlarmNames = ["cw-en-dis"],
        });

        var disabled = (await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            AlarmNames = ["cw-en-dis"],
        })).MetricAlarms[0];
        disabled.ActionsEnabled.ShouldBe(false);

        await _cw.EnableAlarmActionsAsync(new EnableAlarmActionsRequest
        {
            AlarmNames = ["cw-en-dis"],
        });

        var enabled = (await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            AlarmNames = ["cw-en-dis"],
        })).MetricAlarms[0];
        enabled.ActionsEnabled.ShouldBe(true);
    }

    // ── SetAlarmState returns error for non-existent alarm ──────────────────

    [Fact]
    public async Task SetAlarmStateNotFoundReturnsError()
    {
        var ex = await Should.ThrowAsync<Amazon.CloudWatch.Model.ResourceNotFoundException>(
            () => _cw.SetAlarmStateAsync(new SetAlarmStateRequest
            {
                AlarmName = "nonexistent-alarm-xyz",
                StateValue = StateValue.ALARM,
                StateReason = "test",
            }));

        ex.Message.ShouldContain("nonexistent-alarm-xyz");
    }

    // ── Dashboard not found ─────────────────────────────────────────────────

    [Fact]
    public async Task GetDashboardNotFound()
    {
        await Should.ThrowAsync<Amazon.CloudWatch.Model.DashboardNotFoundErrorException>(
            () => _cw.GetDashboardAsync(new GetDashboardRequest
            {
                DashboardName = "no-such-dashboard",
            }));
    }

    // ── ListDashboards with prefix ──────────────────────────────────────────

    [Fact]
    public async Task ListDashboardsWithPrefix()
    {
        await _cw.PutDashboardAsync(new PutDashboardRequest
        {
            DashboardName = "prefix-a-dash",
            DashboardBody = "{}",
        });
        await _cw.PutDashboardAsync(new PutDashboardRequest
        {
            DashboardName = "prefix-b-dash",
            DashboardBody = "{}",
        });
        await _cw.PutDashboardAsync(new PutDashboardRequest
        {
            DashboardName = "other-dash",
            DashboardBody = "{}",
        });

        var resp = await _cw.ListDashboardsAsync(new ListDashboardsRequest
        {
            DashboardNamePrefix = "prefix-",
        });

        var names = resp.DashboardEntries.ConvertAll(d => d.DashboardName);
        names.ShouldContain("prefix-a-dash");
        names.ShouldContain("prefix-b-dash");
        names.ShouldNotContain("other-dash");
    }

    // ── GetMetricStatistics returns empty for unknown metric ────────────────

    [Fact]
    public async Task GetMetricStatisticsEmptyForUnknownMetric()
    {
        var now = DateTimeOffset.UtcNow;
        var resp = await _cw.GetMetricStatisticsAsync(new GetMetricStatisticsRequest
        {
            Namespace = "Unknown/Namespace",
            MetricName = "UnknownMetric",
            Period = 60,
            StartTime = now.AddMinutes(-10).UtcDateTime,
            EndTime = now.AddMinutes(10).UtcDateTime,
            Statistics = [Statistic.Average],
        });

        resp.Datapoints.ShouldBeEmpty();
    }

    // ── DescribeAlarms with StateValue filter ───────────────────────────────

    [Fact]
    public async Task DescribeAlarmsWithStateFilter()
    {
        await _cw.PutMetricAlarmAsync(new PutMetricAlarmRequest
        {
            AlarmName = "cw-state-filter-alarm",
            MetricName = "M",
            Namespace = "N",
            Statistic = Statistic.Sum,
            Period = 60,
            EvaluationPeriods = 1,
            Threshold = 1.0,
            ComparisonOperator = ComparisonOperator.GreaterThanThreshold,
        });

        // Initially INSUFFICIENT_DATA
        var resp = await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            StateValue = StateValue.INSUFFICIENT_DATA,
        });
        resp.MetricAlarms.ShouldContain(a => a.AlarmName == "cw-state-filter-alarm");

        // Set to ALARM
        await _cw.SetAlarmStateAsync(new SetAlarmStateRequest
        {
            AlarmName = "cw-state-filter-alarm",
            StateValue = StateValue.ALARM,
            StateReason = "test",
        });

        var respAlarm = await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            StateValue = StateValue.ALARM,
        });
        respAlarm.MetricAlarms.ShouldContain(a => a.AlarmName == "cw-state-filter-alarm");

        var respInsufficient = await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            StateValue = StateValue.INSUFFICIENT_DATA,
        });
        respInsufficient.MetricAlarms.ShouldNotContain(a => a.AlarmName == "cw-state-filter-alarm");
    }
}
