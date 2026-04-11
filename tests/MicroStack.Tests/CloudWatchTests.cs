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

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _cw.Dispose();
        return Task.CompletedTask;
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
        Assert.Contains("RequestCount", names);
        Assert.Contains("Latency", names);
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

        Assert.Single(resp.MetricAlarms);
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

        Assert.Equal("test-dash", getResp.DashboardName);
        Assert.NotNull(getResp.DashboardBody);

        var listResp = await _cw.ListDashboardsAsync(new ListDashboardsRequest());
        Assert.Contains(listResp.DashboardEntries, d => d.DashboardName == "test-dash");

        await _cw.DeleteDashboardsAsync(new DeleteDashboardsRequest
        {
            DashboardNames = ["test-dash"],
        });

        var listResp2 = await _cw.ListDashboardsAsync(new ListDashboardsRequest());
        Assert.DoesNotContain(listResp2.DashboardEntries, d => d.DashboardName == "test-dash");
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
        Assert.Contains("Reqs", names);
        Assert.Contains("Errs", names);

        var filtered = await _cw.ListMetricsAsync(new ListMetricsRequest
        {
            Namespace = "CWv2",
            MetricName = "Reqs",
        });
        Assert.All(filtered.Metrics, m => Assert.Equal("Reqs", m.MetricName));
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

        Assert.NotEmpty(resp.Datapoints);
        var dp = resp.Datapoints[0];
        Assert.True(dp.Average > 0);
        Assert.True(dp.Sum > 0);
        Assert.True(dp.SampleCount > 0);
        Assert.True(dp.Minimum > 0);
        Assert.True(dp.Maximum > 0);
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
        Assert.Equal("cw-v2-high-err", alarm.AlarmName);
        Assert.Equal(10.0, alarm.Threshold);
        Assert.Equal(ComparisonOperator.GreaterThanOrEqualToThreshold, alarm.ComparisonOperator);
        Assert.Equal(2, alarm.EvaluationPeriods);
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
            Assert.Contains($"cw-da-v2-{i}", names);
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

        Assert.Empty(resp.MetricAlarms);
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
        Assert.Equal(StateValue.INSUFFICIENT_DATA, initial.StateValue);

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
        Assert.Equal(StateValue.ALARM, after.StateValue);
        Assert.Equal("Manual trigger for testing", after.StateReason);
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

        Assert.Single(resp.MetricDataResults);
        Assert.Equal("q1", resp.MetricDataResults[0].Id);
        Assert.Equal(StatusCode.Complete, resp.MetricDataResults[0].StatusCode);
        Assert.NotEmpty(resp.MetricDataResults[0].Values);
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
        Assert.Equal("prod", tagMap["env"]);
        Assert.Equal("sre", tagMap["team"]);

        await _cw.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceARN = arn,
            TagKeys = ["env"],
        });

        var resp2 = await _cw.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceARN = arn,
        });
        Assert.DoesNotContain(resp2.Tags, t => t.Key == "env");
        Assert.Contains(resp2.Tags, t => t.Key == "team");
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

        Assert.Contains(resp.CompositeAlarms, a => a.AlarmName == compositeName);

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

        Assert.Contains(resp.MetricAlarms, a => a.AlarmName == alarmName);

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

        Assert.NotEmpty(resp.AlarmHistoryItems);

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
        Assert.NotNull(result);
        Assert.Equal(StatusCode.Complete, result.StatusCode);
        Assert.NotEmpty(result.Values);
        Assert.True(result.Values.Sum() >= 100.0);
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
        Assert.Equal(StateValue.ALARM, alarms[0].StateValue);

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
        Assert.Equal(StateValue.OK, alarms2[0].StateValue);
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
        Assert.Contains("MetA", names);
        Assert.DoesNotContain("MetB", names);
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
        Assert.Contains(resp.Metrics, m => m.MetricName == "Latency");
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
        Assert.False(disabled.ActionsEnabled);

        await _cw.EnableAlarmActionsAsync(new EnableAlarmActionsRequest
        {
            AlarmNames = ["cw-en-dis"],
        });

        var enabled = (await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            AlarmNames = ["cw-en-dis"],
        })).MetricAlarms[0];
        Assert.True(enabled.ActionsEnabled);
    }

    // ── SetAlarmState returns error for non-existent alarm ──────────────────

    [Fact]
    public async Task SetAlarmStateNotFoundReturnsError()
    {
        var ex = await Assert.ThrowsAsync<Amazon.CloudWatch.Model.ResourceNotFoundException>(
            () => _cw.SetAlarmStateAsync(new SetAlarmStateRequest
            {
                AlarmName = "nonexistent-alarm-xyz",
                StateValue = StateValue.ALARM,
                StateReason = "test",
            }));

        Assert.Contains("nonexistent-alarm-xyz", ex.Message);
    }

    // ── Dashboard not found ─────────────────────────────────────────────────

    [Fact]
    public async Task GetDashboardNotFound()
    {
        await Assert.ThrowsAsync<Amazon.CloudWatch.Model.DashboardNotFoundErrorException>(
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
        Assert.Contains("prefix-a-dash", names);
        Assert.Contains("prefix-b-dash", names);
        Assert.DoesNotContain("other-dash", names);
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

        Assert.Empty(resp.Datapoints);
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
        Assert.Contains(resp.MetricAlarms, a => a.AlarmName == "cw-state-filter-alarm");

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
        Assert.Contains(respAlarm.MetricAlarms, a => a.AlarmName == "cw-state-filter-alarm");

        var respInsufficient = await _cw.DescribeAlarmsAsync(new DescribeAlarmsRequest
        {
            StateValue = StateValue.INSUFFICIENT_DATA,
        });
        Assert.DoesNotContain(respInsufficient.MetricAlarms, a => a.AlarmName == "cw-state-filter-alarm");
    }
}
