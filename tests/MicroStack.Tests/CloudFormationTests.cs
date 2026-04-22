using Amazon;
using Amazon.CloudFormation;
using Amazon.CloudFormation.Model;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.SQS;
using Amazon.SimpleNotificationService;
using Amazon.IdentityManagement;
using Amazon.Lambda;
using Amazon.SimpleSystemsManagement;
using Amazon.DynamoDBv2;
using Amazon.Kinesis;
using Amazon.ECR;
using Amazon.ElasticLoadBalancingV2;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the CloudFormation service handler.
/// Uses the AWS SDK for .NET v4 pointed at the in-process MicroStack server.
/// Mirrors coverage from ministack/tests/test_cfn.py.
/// </summary>
public sealed class CloudFormationTests(MicroStackFixture fixture) : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly AmazonCloudFormationClient _cfn = CreateClient<AmazonCloudFormationClient, AmazonCloudFormationConfig>(fixture);
    private readonly AmazonS3Client _s3 = CreateClient<AmazonS3Client, AmazonS3Config>(fixture);
    private readonly AmazonSQSClient _sqs = CreateClient<AmazonSQSClient, AmazonSQSConfig>(fixture);
    private readonly AmazonSimpleNotificationServiceClient _sns = CreateClient<AmazonSimpleNotificationServiceClient, AmazonSimpleNotificationServiceConfig>(fixture);
    private readonly AmazonIdentityManagementServiceClient _iam = CreateClient<AmazonIdentityManagementServiceClient, AmazonIdentityManagementServiceConfig>(fixture);
    private readonly AmazonLambdaClient _lambda = CreateClient<AmazonLambdaClient, AmazonLambdaConfig>(fixture);
    private readonly AmazonSimpleSystemsManagementClient _ssm = CreateClient<AmazonSimpleSystemsManagementClient, AmazonSimpleSystemsManagementConfig>(fixture);
    private readonly AmazonDynamoDBClient _ddb = CreateClient<AmazonDynamoDBClient, AmazonDynamoDBConfig>(fixture);
    private readonly AmazonKinesisClient _kinesis = CreateClient<AmazonKinesisClient, AmazonKinesisConfig>(fixture);
    private readonly AmazonECRClient _ecr = CreateClient<AmazonECRClient, AmazonECRConfig>(fixture);
    private readonly AmazonElasticLoadBalancingV2Client _elbv2 = CreateClient<AmazonElasticLoadBalancingV2Client, AmazonElasticLoadBalancingV2Config>(fixture);

    private static TClient CreateClient<TClient, TConfig>(MicroStackFixture fixture)
        where TClient : AmazonServiceClient
        where TConfig : ClientConfig, new()
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new TConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return (TClient)Activator.CreateInstance(
            typeof(TClient),
            new BasicAWSCredentials("test", "test"),
            config)!;
    }

    public async ValueTask InitializeAsync()
    {
        await fixture.HttpClient.PostAsync("/_microstack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _cfn.Dispose();
        _s3.Dispose();
        _sqs.Dispose();
        _sns.Dispose();
        _iam.Dispose();
        _lambda.Dispose();
        _ssm.Dispose();
        _ddb.Dispose();
        _kinesis.Dispose();
        _ecr.Dispose();
        _elbv2.Dispose();
        return ValueTask.CompletedTask;
    }

    private static string ToJson(object obj)
    {
        return System.Text.Json.JsonSerializer.Serialize(obj);
    }

    // ── Basic stack lifecycle ────────────────────────────────────────────────────

    [Fact]
    public async Task CreateDescribeDeleteStack()
    {
        var template = ToJson(new
        {
            AWSTemplateFormatVersion = "2010-09-09",
            Resources = new
            {
                Bucket = new
                {
                    Type = "AWS::S3::Bucket",
                    Properties = new { BucketName = "cfn-t01-bucket" },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-t01",
            TemplateBody = template,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest
        {
            StackName = "cfn-t01",
        });
        desc.Stacks.ShouldHaveSingleItem();
        desc.Stacks[0].StackStatus.Value.ShouldBe("CREATE_COMPLETE");

        // Verify S3 bucket was created
        await AssertBucketExists("cfn-t01-bucket");

        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = "cfn-t01" });

        // Bucket should be deleted
        await AssertBucketDoesNotExist("cfn-t01-bucket");
    }

    [Fact]
    public async Task StackWithParameters()
    {
        var template = ToJson(new
        {
            AWSTemplateFormatVersion = "2010-09-09",
            Parameters = new Dictionary<string, object>
            {
                ["QueueName"] = new { Type = "String", Default = "cfn-t02-default" },
            },
            Resources = new Dictionary<string, object>
            {
                ["Queue"] = new
                {
                    Type = "AWS::SQS::Queue",
                    Properties = new Dictionary<string, object>
                    {
                        ["QueueName"] = new { Ref = "QueueName" },
                    },
                },
            },
        });

        // With default parameter
        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-t02a",
            TemplateBody = template,
        });

        var queues = await _sqs.ListQueuesAsync("cfn-t02-default");
        queues.QueueUrls.ShouldContain(u => u.Contains("cfn-t02-default"));

        // With custom parameter
        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-t02b",
            TemplateBody = template,
            Parameters =
            [
                new Parameter { ParameterKey = "QueueName", ParameterValue = "cfn-t02-custom" },
            ],
        });

        queues = await _sqs.ListQueuesAsync("cfn-t02-custom");
        queues.QueueUrls.ShouldContain(u => u.Contains("cfn-t02-custom"));
    }

    [Fact]
    public async Task IntrinsicRefGetAtt()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["MyQueue"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::SQS::Queue",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["QueueName"] = "cfn-t03-queue",
                    },
                },
                ["Param"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::SSM::Parameter",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["Name"] = "cfn-t03-param",
                        ["Type"] = "String",
                        ["Value"] = new Dictionary<string, object>
                        {
                            ["Fn::GetAtt"] = new[] { "MyQueue", "Arn" },
                        },
                    },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-t03",
            TemplateBody = template,
        });

        var param = await _ssm.GetParameterAsync(new Amazon.SimpleSystemsManagement.Model.GetParameterRequest
        {
            Name = "cfn-t03-param",
        });
        param.Parameter.Value.ShouldStartWith("arn:aws:sqs:");
    }

    [Fact]
    public async Task ConditionsCreateAndSkip()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Parameters"] = new Dictionary<string, object>
            {
                ["Create"] = new { Type = "String", Default = "yes" },
            },
            ["Conditions"] = new Dictionary<string, object>
            {
                ["ShouldCreate"] = new Dictionary<string, object>
                {
                    ["Fn::Equals"] = new object[] { new Dictionary<string, object> { ["Ref"] = "Create" }, "yes" },
                },
            },
            ["Resources"] = new Dictionary<string, object>
            {
                ["Bucket"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::S3::Bucket",
                    ["Condition"] = "ShouldCreate",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["BucketName"] = "cfn-t04-cond",
                    },
                },
            },
        });

        // Condition true — bucket created
        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-t04a",
            TemplateBody = template,
        });
        await AssertBucketExists("cfn-t04-cond");

        // Delete first stack so bucket name is freed
        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = "cfn-t04a" });

        // Condition false — bucket NOT created
        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-t04b",
            TemplateBody = template,
            Parameters =
            [
                new Parameter { ParameterKey = "Create", ParameterValue = "no" },
            ],
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = "cfn-t04b" });
        desc.Stacks[0].StackStatus.Value.ShouldBe("CREATE_COMPLETE");

        await AssertBucketDoesNotExist("cfn-t04-cond");
    }

    [Fact]
    public async Task OutputsAndExports()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["Bucket"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::S3::Bucket",
                    ["Properties"] = new Dictionary<string, object> { ["BucketName"] = "cfn-t05-exports" },
                },
            },
            ["Outputs"] = new Dictionary<string, object>
            {
                ["BucketOut"] = new Dictionary<string, object>
                {
                    ["Value"] = new Dictionary<string, object> { ["Ref"] = "Bucket" },
                    ["Export"] = new Dictionary<string, object> { ["Name"] = "cfn-t05-bucket-export" },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-t05",
            TemplateBody = template,
        });

        var exports = await _cfn.ListExportsAsync(new ListExportsRequest());
        exports.Exports.ShouldContain(e => e.Name == "cfn-t05-bucket-export");
    }

    [Fact]
    public async Task FnSub()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["MyBucket"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::S3::Bucket",
                    ["Properties"] = new Dictionary<string, object> { ["BucketName"] = "cfn-t06-src" },
                },
                ["Param"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::SSM::Parameter",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["Name"] = "cfn-t06-param",
                        ["Type"] = "String",
                        ["Value"] = new Dictionary<string, object> { ["Fn::Sub"] = "${MyBucket}-replica" },
                    },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-t06",
            TemplateBody = template,
        });

        var param = await _ssm.GetParameterAsync(new Amazon.SimpleSystemsManagement.Model.GetParameterRequest
        {
            Name = "cfn-t06-param",
        });
        param.Parameter.Value.ShouldBe("cfn-t06-src-replica");
    }

    [Fact]
    public async Task MultiResourceDependencies()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["Role"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::IAM::Role",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["RoleName"] = "cfn-t07-role",
                        ["AssumeRolePolicyDocument"] = new Dictionary<string, object>
                        {
                            ["Version"] = "2012-10-17",
                            ["Statement"] = new[]
                            {
                                new Dictionary<string, object>
                                {
                                    ["Effect"] = "Allow",
                                    ["Principal"] = new Dictionary<string, object> { ["Service"] = "lambda.amazonaws.com" },
                                    ["Action"] = "sts:AssumeRole",
                                },
                            },
                        },
                    },
                },
                ["Func"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::Lambda::Function",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["FunctionName"] = "cfn-t07-func",
                        ["Runtime"] = "python3.9",
                        ["Handler"] = "index.handler",
                        ["Role"] = new Dictionary<string, object>
                        {
                            ["Fn::GetAtt"] = new[] { "Role", "Arn" },
                        },
                        ["Code"] = new Dictionary<string, object> { ["ZipFile"] = "def handler(e,c): return {}" },
                    },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-t07",
            TemplateBody = template,
        });

        var role = await _iam.GetRoleAsync(new Amazon.IdentityManagement.Model.GetRoleRequest
        {
            RoleName = "cfn-t07-role",
        });
        var func = await _lambda.GetFunctionAsync(new Amazon.Lambda.Model.GetFunctionRequest
        {
            FunctionName = "cfn-t07-func",
        });
        func.Configuration.Role.ShouldBe(role.Role.Arn);
    }

    // ── Change set lifecycle ─────────────────────────────────────────────────────

    [Fact]
    public async Task ChangeSetLifecycle()
    {
        var template = ToJson(new
        {
            AWSTemplateFormatVersion = "2010-09-09",
            Resources = new
            {
                Bucket = new
                {
                    Type = "AWS::S3::Bucket",
                    Properties = new { BucketName = "cfn-t08-cs" },
                },
            },
        });

        await _cfn.CreateChangeSetAsync(new CreateChangeSetRequest
        {
            StackName = "cfn-t08",
            ChangeSetName = "cfn-t08-cs1",
            TemplateBody = template,
            ChangeSetType = ChangeSetType.CREATE,
        });

        var cs = await _cfn.DescribeChangeSetAsync(new DescribeChangeSetRequest
        {
            StackName = "cfn-t08",
            ChangeSetName = "cfn-t08-cs1",
        });
        cs.ChangeSetName.ShouldBe("cfn-t08-cs1");

        await _cfn.ExecuteChangeSetAsync(new ExecuteChangeSetRequest
        {
            StackName = "cfn-t08",
            ChangeSetName = "cfn-t08-cs1",
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = "cfn-t08" });
        desc.Stacks[0].StackStatus.Value.ShouldBe("CREATE_COMPLETE");
    }

    // ── Update stack ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task UpdateStack()
    {
        var templateV1 = ToJson(new
        {
            AWSTemplateFormatVersion = "2010-09-09",
            Resources = new
            {
                BucketA = new
                {
                    Type = "AWS::S3::Bucket",
                    Properties = new { BucketName = "cfn-t09-a" },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-t09",
            TemplateBody = templateV1,
        });

        var templateV2 = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["BucketA"] = new
                {
                    Type = "AWS::S3::Bucket",
                    Properties = new { BucketName = "cfn-t09-a" },
                },
                ["BucketB"] = new
                {
                    Type = "AWS::S3::Bucket",
                    Properties = new { BucketName = "cfn-t09-b" },
                },
            },
        });

        await _cfn.UpdateStackAsync(new UpdateStackRequest
        {
            StackName = "cfn-t09",
            TemplateBody = templateV2,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = "cfn-t09" });
        desc.Stacks[0].StackStatus.Value.ShouldBe("UPDATE_COMPLETE");

        await _s3.GetBucketLocationAsync("cfn-t09-a");
        await _s3.GetBucketLocationAsync("cfn-t09-b");
    }

    // ── Delete nonexistent stack ─────────────────────────────────────────────────

    [Fact]
    public async Task DeleteNonexistentStackSucceeds()
    {
        // AWS returns 200 for deleting non-existent stacks
        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = "cfn-nonexistent-xyz" });

        // But describing it should fail
        await Should.ThrowAsync<AmazonCloudFormationException>(
            () => _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = "cfn-nonexistent-xyz" }));
    }

    // ── Validate template ────────────────────────────────────────────────────────

    [Fact]
    public async Task ValidateTemplate()
    {
        var validTemplate = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Parameters"] = new Dictionary<string, object>
            {
                ["Env"] = new { Type = "String", Default = "dev" },
            },
            ["Resources"] = new Dictionary<string, object>
            {
                ["Bucket"] = new
                {
                    Type = "AWS::S3::Bucket",
                    Properties = new { BucketName = "cfn-t11-validate" },
                },
            },
        });

        var result = await _cfn.ValidateTemplateAsync(new ValidateTemplateRequest
        {
            TemplateBody = validTemplate,
        });
        result.Parameters.ShouldContain(p => p.ParameterKey == "Env");

        // Invalid template (no Resources)
        var invalidTemplate = ToJson(new { AWSTemplateFormatVersion = "2010-09-09" });
        await Should.ThrowAsync<AmazonCloudFormationException>(
            () => _cfn.ValidateTemplateAsync(new ValidateTemplateRequest
            {
                TemplateBody = invalidTemplate,
            }));
    }

    // ── List stacks ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task ListStacks()
    {
        foreach (var name in new[] { "cfn-t12-a", "cfn-t12-b" })
        {
            var template = ToJson(new Dictionary<string, object>
            {
                ["AWSTemplateFormatVersion"] = "2010-09-09",
                ["Resources"] = new Dictionary<string, object>
                {
                    ["Bucket"] = new
                    {
                        Type = "AWS::S3::Bucket",
                        Properties = new { BucketName = $"{name}-bucket" },
                    },
                },
            });
            await _cfn.CreateStackAsync(new CreateStackRequest
            {
                StackName = name,
                TemplateBody = template,
            });
        }

        var summaries = await _cfn.ListStacksAsync(new ListStacksRequest());
        var names = summaries.StackSummaries.Select(s => s.StackName).ToList();
        names.ShouldContain("cfn-t12-a");
        names.ShouldContain("cfn-t12-b");
    }

    // ── Stack events ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task StackEvents()
    {
        var template = ToJson(new
        {
            AWSTemplateFormatVersion = "2010-09-09",
            Resources = new
            {
                Bucket = new
                {
                    Type = "AWS::S3::Bucket",
                    Properties = new { BucketName = "cfn-t13-events" },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-t13",
            TemplateBody = template,
        });

        var events = await _cfn.DescribeStackEventsAsync(new DescribeStackEventsRequest
        {
            StackName = "cfn-t13",
        });
        events.StackEvents.ShouldNotBeEmpty();
        events.StackEvents.ShouldAllBe(e => e.ResourceStatus != null);
    }

    // ── Rollback on failure ──────────────────────────────────────────────────────

    [Fact]
    public async Task RollbackOnFailure()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["Bucket"] = new
                {
                    Type = "AWS::S3::Bucket",
                    Properties = new { BucketName = "cfn-t15-rollback" },
                },
                ["Bad"] = new
                {
                    Type = "AWS::Fake::Nope",
                    Properties = new Dictionary<string, object>(),
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-t15",
            TemplateBody = template,
            DisableRollback = false,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = "cfn-t15" });
        desc.Stacks[0].StackStatus.Value.ShouldBe("ROLLBACK_COMPLETE");

        // Bucket should have been rolled back
        await AssertBucketDoesNotExist("cfn-t15-rollback");
    }

    // ── Update rollback on failure ───────────────────────────────────────────────

    [Fact]
    public async Task UpdateRollbackOnFailure()
    {
        var templateV1 = ToJson(new
        {
            AWSTemplateFormatVersion = "2010-09-09",
            Resources = new
            {
                Bucket = new
                {
                    Type = "AWS::S3::Bucket",
                    Properties = new { BucketName = "cfn-t18-orig" },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-t18",
            TemplateBody = templateV1,
        });

        var templateV2 = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["Bucket"] = new
                {
                    Type = "AWS::S3::Bucket",
                    Properties = new { BucketName = "cfn-t18-orig" },
                },
                ["Bad"] = new
                {
                    Type = "AWS::Fake::Nope",
                    Properties = new Dictionary<string, object>(),
                },
            },
        });

        await _cfn.UpdateStackAsync(new UpdateStackRequest
        {
            StackName = "cfn-t18",
            TemplateBody = templateV2,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = "cfn-t18" });
        desc.Stacks[0].StackStatus.Value.ShouldBe("UPDATE_ROLLBACK_COMPLETE");

        // Original bucket should still exist
        await _s3.GetBucketLocationAsync("cfn-t18-orig");
    }

    // ── Kinesis stream via CloudFormation ─────────────────────────────────────────

    [Fact]
    public async Task KinesisStream()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["DataStream"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::Kinesis::Stream",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["Name"] = "cfn-kinesis-cfn-test",
                        ["ShardCount"] = 2,
                    },
                },
            },
            ["Outputs"] = new Dictionary<string, object>
            {
                ["StreamArn"] = new Dictionary<string, object>
                {
                    ["Value"] = new Dictionary<string, object>
                    {
                        ["Fn::GetAtt"] = new[] { "DataStream", "Arn" },
                    },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-t-kinesis",
            TemplateBody = template,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = "cfn-t-kinesis" });
        desc.Stacks[0].StackStatus.Value.ShouldBe("CREATE_COMPLETE");

        var streamDesc = await _kinesis.DescribeStreamAsync(new Amazon.Kinesis.Model.DescribeStreamRequest
        {
            StreamName = "cfn-kinesis-cfn-test",
        });
        streamDesc.StreamDescription.StreamStatus.Value.ShouldBe("ACTIVE");
        streamDesc.StreamDescription.Shards.Count.ShouldBe(2);

        var outputs = desc.Stacks[0].Outputs.ToDictionary(o => o.OutputKey, o => o.OutputValue);
        outputs["StreamArn"].ShouldBe(streamDesc.StreamDescription.StreamARN);

        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = "cfn-t-kinesis" });

        await Should.ThrowAsync<Amazon.Kinesis.Model.ResourceNotFoundException>(
            () => _kinesis.DescribeStreamAsync(new Amazon.Kinesis.Model.DescribeStreamRequest
            {
                StreamName = "cfn-kinesis-cfn-test",
            }));
    }

    // ── Wait condition (no-op resource) ──────────────────────────────────────────

    [Fact]
    public async Task WaitConditionNoOp()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["Handle"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::CloudFormation::WaitConditionHandle",
                },
                ["Wait"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::CloudFormation::WaitCondition",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["Handle"] = new Dictionary<string, object> { ["Ref"] = "Handle" },
                        ["Timeout"] = "10",
                    },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-wait",
            TemplateBody = template,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = "cfn-wait" });
        desc.Stacks[0].StackStatus.Value.ShouldBe("CREATE_COMPLETE");

        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = "cfn-wait" });
    }

    // ── Multi-resource stack (S3 + Lambda + DynamoDB) ────────────────────────────

    [Fact]
    public async Task MultiResourceStackWithS3LambdaDynamoDB()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["MyBucket"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::S3::Bucket",
                    ["Properties"] = new Dictionary<string, object> { ["BucketName"] = "intg-cfn-full-bkt" },
                },
                ["MyTable"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::DynamoDB::Table",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["TableName"] = "intg-cfn-full-tbl",
                        ["KeySchema"] = new[] { new { AttributeName = "pk", KeyType = "HASH" } },
                        ["AttributeDefinitions"] = new[] { new { AttributeName = "pk", AttributeType = "S" } },
                        ["BillingMode"] = "PAY_PER_REQUEST",
                    },
                },
                ["MyFunction"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::Lambda::Function",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["FunctionName"] = "intg-cfn-full-fn",
                        ["Runtime"] = "python3.11",
                        ["Handler"] = "index.handler",
                        ["Role"] = "arn:aws:iam::000000000000:role/cfn-role",
                        ["Code"] = new Dictionary<string, object>
                        {
                            ["ZipFile"] = "import json\ndef handler(event, context):\n    return {'statusCode': 200, 'body': json.dumps(event)}\n",
                        },
                    },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "intg-cfn-full-stack",
            TemplateBody = template,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest
        {
            StackName = "intg-cfn-full-stack",
        });
        desc.Stacks[0].StackStatus.Value.ShouldBe("CREATE_COMPLETE");

        // Verify DynamoDB table
        var tables = await _ddb.ListTablesAsync();
        tables.TableNames.ShouldContain("intg-cfn-full-tbl");

        // Verify describe stack resources
        var resources = await _cfn.DescribeStackResourcesAsync(new DescribeStackResourcesRequest
        {
            StackName = "intg-cfn-full-stack",
        });
        var resourceTypes = resources.StackResources.Select(r => r.ResourceType).ToHashSet();
        resourceTypes.ShouldContain("AWS::S3::Bucket");
        resourceTypes.ShouldContain("AWS::DynamoDB::Table");
        resourceTypes.ShouldContain("AWS::Lambda::Function");

        // Delete and verify
        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = "intg-cfn-full-stack" });

        var stacks = await _cfn.DescribeStacksAsync(new DescribeStacksRequest());
        var stackList = stacks.Stacks ?? [];
        var active = stackList
            .Where(s => s.StackName == "intg-cfn-full-stack" && !s.StackStatus.Value.Contains("DELETE"))
            .ToList();
        active.ShouldBeEmpty();
    }

    // ── CDK bootstrap resources ──────────────────────────────────────────────────

    [Fact]
    public async Task CdkBootstrapResources()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["StagingBucket"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::S3::Bucket",
                    ["Properties"] = new Dictionary<string, object> { ["BucketName"] = "cdk-bootstrap-v44" },
                },
                ["ContainerRepo"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::ECR::Repository",
                    ["Properties"] = new Dictionary<string, object> { ["RepositoryName"] = "cdk-assets-v44" },
                },
                ["DeployRole"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::IAM::Role",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["RoleName"] = "cdk-deploy-v44",
                        ["AssumeRolePolicyDocument"] = new Dictionary<string, object>
                        {
                            ["Version"] = "2012-10-17",
                            ["Statement"] = Array.Empty<object>(),
                        },
                    },
                },
                ["FileKey"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::KMS::Key",
                    ["Properties"] = new Dictionary<string, object> { ["Description"] = "CDK file assets key" },
                },
                ["KeyAlias"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::KMS::Alias",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["AliasName"] = "alias/cdk-key-v44",
                        ["TargetKeyId"] = "dummy",
                    },
                },
                ["BootstrapVersion"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::SSM::Parameter",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["Name"] = "/cdk-bootstrap/v44/version",
                        ["Type"] = "String",
                        ["Value"] = "27",
                    },
                },
                ["DeployPolicy"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::IAM::ManagedPolicy",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["ManagedPolicyName"] = "cdk-policy-v44",
                        ["PolicyDocument"] = new Dictionary<string, object>
                        {
                            ["Version"] = "2012-10-17",
                            ["Statement"] = Array.Empty<object>(),
                        },
                    },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "CDKToolkit-v44",
            TemplateBody = template,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = "CDKToolkit-v44" });
        desc.Stacks[0].StackStatus.Value.ShouldBe("CREATE_COMPLETE");

        // Verify S3 bucket
        await _s3.GetBucketLocationAsync("cdk-bootstrap-v44");

        // Verify ECR repo
        var repos = await _ecr.DescribeRepositoriesAsync(new Amazon.ECR.Model.DescribeRepositoriesRequest());
        repos.Repositories.ShouldContain(r => r.RepositoryName == "cdk-assets-v44");

        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = "CDKToolkit-v44" });
    }

    // ── Auto-name S3 follows AWS pattern ─────────────────────────────────────────

    [Fact]
    public async Task AutoNameS3FollowsAwsPattern()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["MyBucket"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::S3::Bucket",
                    ["Properties"] = new Dictionary<string, object>(),
                },
            },
            ["Outputs"] = new Dictionary<string, object>
            {
                ["BucketName"] = new Dictionary<string, object>
                {
                    ["Value"] = new Dictionary<string, object> { ["Ref"] = "MyBucket" },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-autoname-s3",
            TemplateBody = template,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = "cfn-autoname-s3" });
        desc.Stacks[0].StackStatus.Value.ShouldBe("CREATE_COMPLETE");

        var bucketName = desc.Stacks[0].Outputs.First(o => o.OutputKey == "BucketName").OutputValue;
        bucketName.ToLowerInvariant().ShouldBe(bucketName); // Must be lowercase
        bucketName.ShouldStartWith("cfn-autoname-s3-mybucket-");
        (bucketName.Length <= 63).ShouldBe(true, $"S3 name too long: {bucketName.Length}");

        // Verify bucket actually exists
        await _s3.GetBucketLocationAsync(bucketName);

        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = "cfn-autoname-s3" });
    }

    // ── Auto-name SQS follows AWS pattern ────────────────────────────────────────

    [Fact]
    public async Task AutoNameSqsFollowsAwsPattern()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["MyQueue"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::SQS::Queue",
                    ["Properties"] = new Dictionary<string, object>(),
                },
            },
            ["Outputs"] = new Dictionary<string, object>
            {
                ["QueueName"] = new Dictionary<string, object>
                {
                    ["Value"] = new Dictionary<string, object>
                    {
                        ["Fn::GetAtt"] = new[] { "MyQueue", "QueueName" },
                    },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-autoname-sqs",
            TemplateBody = template,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = "cfn-autoname-sqs" });
        desc.Stacks[0].StackStatus.Value.ShouldBe("CREATE_COMPLETE");

        var queueName = desc.Stacks[0].Outputs.First(o => o.OutputKey == "QueueName").OutputValue;
        queueName.ShouldStartWith("cfn-autoname-sqs-MyQueue-");
        (queueName.Length <= 80).ShouldBe(true);

        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = "cfn-autoname-sqs" });
    }

    // ── Auto-name DynamoDB follows AWS pattern ───────────────────────────────────

    [Fact]
    public async Task AutoNameDynamoDbFollowsAwsPattern()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["MyTable"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::DynamoDB::Table",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["AttributeDefinitions"] = new[] { new { AttributeName = "pk", AttributeType = "S" } },
                        ["KeySchema"] = new[] { new { AttributeName = "pk", KeyType = "HASH" } },
                        ["BillingMode"] = "PAY_PER_REQUEST",
                    },
                },
            },
            ["Outputs"] = new Dictionary<string, object>
            {
                ["TableName"] = new Dictionary<string, object>
                {
                    ["Value"] = new Dictionary<string, object> { ["Ref"] = "MyTable" },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-autoname-ddb",
            TemplateBody = template,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = "cfn-autoname-ddb" });
        desc.Stacks[0].StackStatus.Value.ShouldBe("CREATE_COMPLETE");

        var tableName = desc.Stacks[0].Outputs.First(o => o.OutputKey == "TableName").OutputValue;
        tableName.ShouldStartWith("cfn-autoname-ddb-MyTable-");
        (tableName.Length <= 255).ShouldBe(true);

        // Verify table exists
        await _ddb.DescribeTableAsync(tableName);

        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = "cfn-autoname-ddb" });
    }

    // ── Explicit name not overridden ─────────────────────────────────────────────

    [Fact]
    public async Task ExplicitNameNotOverridden()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["MyBucket"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::S3::Bucket",
                    ["Properties"] = new Dictionary<string, object> { ["BucketName"] = "cfn-explicit-name-test" },
                },
            },
            ["Outputs"] = new Dictionary<string, object>
            {
                ["BucketName"] = new Dictionary<string, object>
                {
                    ["Value"] = new Dictionary<string, object> { ["Ref"] = "MyBucket" },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-explicit-name",
            TemplateBody = template,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = "cfn-explicit-name" });
        var bucketName = desc.Stacks[0].Outputs.First(o => o.OutputKey == "BucketName").OutputValue;
        bucketName.ShouldBe("cfn-explicit-name-test");

        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = "cfn-explicit-name" });
    }

    // ── S3 bucket policy ─────────────────────────────────────────────────────────

    [Fact]
    public async Task S3BucketPolicy()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["Bucket"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::S3::Bucket",
                    ["Properties"] = new Dictionary<string, object> { ["BucketName"] = "cfn-policy-test" },
                },
                ["Policy"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::S3::BucketPolicy",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["Bucket"] = "cfn-policy-test",
                        ["PolicyDocument"] = new Dictionary<string, object>
                        {
                            ["Version"] = "2012-10-17",
                            ["Statement"] = new[]
                            {
                                new Dictionary<string, object>
                                {
                                    ["Effect"] = "Allow",
                                    ["Principal"] = "*",
                                    ["Action"] = "s3:GetObject",
                                    ["Resource"] = "arn:aws:s3:::cfn-policy-test/*",
                                },
                            },
                        },
                    },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-s3-policy",
            TemplateBody = template,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = "cfn-s3-policy" });
        desc.Stacks[0].StackStatus.Value.ShouldBe("CREATE_COMPLETE");

        var policy = await _s3.GetBucketPolicyAsync("cfn-policy-test");
        policy.Policy.ShouldContain("s3:GetObject");

        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = "cfn-s3-policy" });
    }

    // ── SecretsManager generate secret string ────────────────────────────────────

    [Fact]
    public async Task SecretsManagerGenerateSecretString()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["MySecret"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::SecretsManager::Secret",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["Name"] = "intg-cfn-gensecret",
                        ["GenerateSecretString"] = new Dictionary<string, object>
                        {
                            ["PasswordLength"] = 20,
                            ["SecretStringTemplate"] = "{\"username\":\"admin\"}",
                            ["GenerateStringKey"] = "password",
                        },
                    },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "intg-cfn-gensecret-stack",
            TemplateBody = template,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest
        {
            StackName = "intg-cfn-gensecret-stack",
        });
        desc.Stacks[0].StackStatus.Value.ShouldBe("CREATE_COMPLETE");
    }

    // ── ELBv2 LoadBalancer and Listener ──────────────────────────────────────────

    [Fact]
    public async Task ElbV2LoadBalancerAndListener()
    {
        var uid = Guid.NewGuid().ToString("N")[..8];
        var stackName = $"cfn-elbv2-{uid}";
        var lbName = $"cfn-alb-{uid}";

        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["Alb"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::ElasticLoadBalancingV2::LoadBalancer",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["Name"] = lbName,
                        ["Type"] = "application",
                        ["Scheme"] = "internal",
                        ["SecurityGroups"] = new[] { "sg-cfn12345" },
                        ["Subnets"] = new[] { "subnet-cfn-a", "subnet-cfn-b" },
                    },
                },
                ["AlbListener"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::ElasticLoadBalancingV2::Listener",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["LoadBalancerArn"] = new Dictionary<string, object> { ["Ref"] = "Alb" },
                        ["Port"] = 443,
                        ["Protocol"] = "HTTPS",
                        ["DefaultActions"] = new[]
                        {
                            new Dictionary<string, object>
                            {
                                ["Type"] = "fixed-response",
                                ["FixedResponseConfig"] = new Dictionary<string, object>
                                {
                                    ["StatusCode"] = "404",
                                    ["ContentType"] = "application/json",
                                    ["MessageBody"] = "{\"status\":404}",
                                },
                            },
                        },
                    },
                },
            },
            ["Outputs"] = new Dictionary<string, object>
            {
                ["AlbDnsName"] = new Dictionary<string, object>
                {
                    ["Value"] = new Dictionary<string, object>
                    {
                        ["Fn::GetAtt"] = new[] { "Alb", "DNSName" },
                    },
                },
                ["AlbListenerArn"] = new Dictionary<string, object>
                {
                    ["Value"] = new Dictionary<string, object> { ["Ref"] = "AlbListener" },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = stackName,
            TemplateBody = template,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = stackName });
        desc.Stacks[0].StackStatus.Value.ShouldBe("CREATE_COMPLETE");

        var outputs = desc.Stacks[0].Outputs.ToDictionary(o => o.OutputKey, o => o.OutputValue);
        outputs["AlbDnsName"].ShouldEndWith(".elb.amazonaws.com");
        outputs["AlbListenerArn"].ShouldContain(":listener/app/");

        var lbs = await _elbv2.DescribeLoadBalancersAsync(
            new Amazon.ElasticLoadBalancingV2.Model.DescribeLoadBalancersRequest
            {
                Names = [lbName],
            });
        lbs.LoadBalancers.ShouldHaveSingleItem();
        lbs.LoadBalancers[0].Scheme.Value.ShouldBe("internal");

        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = stackName });
    }

    // ── GetTemplate ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetTemplate()
    {
        var template = ToJson(new
        {
            AWSTemplateFormatVersion = "2010-09-09",
            Resources = new
            {
                Bucket = new
                {
                    Type = "AWS::S3::Bucket",
                    Properties = new { BucketName = "cfn-gettemplate-bucket" },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-gettemplate",
            TemplateBody = template,
        });

        var result = await _cfn.GetTemplateAsync(new GetTemplateRequest
        {
            StackName = "cfn-gettemplate",
        });
        result.TemplateBody.ShouldNotBeEmpty();
        result.TemplateBody.ShouldContain("cfn-gettemplate-bucket");

        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = "cfn-gettemplate" });
    }

    // ── DescribeStackResource ────────────────────────────────────────────────────

    [Fact]
    public async Task DescribeStackResource()
    {
        var template = ToJson(new
        {
            AWSTemplateFormatVersion = "2010-09-09",
            Resources = new
            {
                Bucket = new
                {
                    Type = "AWS::S3::Bucket",
                    Properties = new { BucketName = "cfn-describe-res-bucket" },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-describe-res",
            TemplateBody = template,
        });

        var result = await _cfn.DescribeStackResourceAsync(new DescribeStackResourceRequest
        {
            StackName = "cfn-describe-res",
            LogicalResourceId = "Bucket",
        });
        result.StackResourceDetail.ResourceType.ShouldBe("AWS::S3::Bucket");
        result.StackResourceDetail.LogicalResourceId.ShouldBe("Bucket");
        result.StackResourceDetail.PhysicalResourceId.ShouldNotBeEmpty();

        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = "cfn-describe-res" });
    }

    // ── ListStackResources ───────────────────────────────────────────────────────

    [Fact]
    public async Task ListStackResources()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["BucketA"] = new
                {
                    Type = "AWS::S3::Bucket",
                    Properties = new { BucketName = "cfn-list-res-a" },
                },
                ["BucketB"] = new
                {
                    Type = "AWS::S3::Bucket",
                    Properties = new { BucketName = "cfn-list-res-b" },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-list-res",
            TemplateBody = template,
        });

        var result = await _cfn.ListStackResourcesAsync(new ListStackResourcesRequest
        {
            StackName = "cfn-list-res",
        });
        result.StackResourceSummaries.Count.ShouldBe(2);

        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = "cfn-list-res" });
    }

    // ── GetTemplateSummary ───────────────────────────────────────────────────────

    [Fact]
    public async Task GetTemplateSummary()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Parameters"] = new Dictionary<string, object>
            {
                ["Env"] = new { Type = "String", Default = "dev" },
            },
            ["Resources"] = new Dictionary<string, object>
            {
                ["Bucket"] = new
                {
                    Type = "AWS::S3::Bucket",
                    Properties = new { BucketName = "cfn-summary-bucket" },
                },
            },
        });

        var result = await _cfn.GetTemplateSummaryAsync(new GetTemplateSummaryRequest
        {
            TemplateBody = template,
        });

        result.Parameters.ShouldContain(p => p.ParameterKey == "Env");
        result.ResourceTypes.ShouldContain(t => t == "AWS::S3::Bucket");
    }

    // ── ImportValue with nonexistent export ──────────────────────────────────────

    [Fact]
    public async Task ImportNonexistentExportFails()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["Param"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::SSM::Parameter",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["Name"] = "cfn-t16-param",
                        ["Type"] = "String",
                        ["Value"] = new Dictionary<string, object>
                        {
                            ["Fn::ImportValue"] = "NonExistentExport123",
                        },
                    },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-t16",
            TemplateBody = template,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = "cfn-t16" });
        var status = desc.Stacks[0].StackStatus.Value;
        (status == "CREATE_FAILED" || status == "ROLLBACK_COMPLETE").ShouldBe(true, $"Expected CREATE_FAILED or ROLLBACK_COMPLETE, got {status}");
    }

    // ── EC2 Launch Template via CloudFormation ───────────────────────────────────

    [Fact]
    public async Task Ec2LaunchTemplate()
    {
        var template = ToJson(new Dictionary<string, object>
        {
            ["AWSTemplateFormatVersion"] = "2010-09-09",
            ["Resources"] = new Dictionary<string, object>
            {
                ["MyLT"] = new Dictionary<string, object>
                {
                    ["Type"] = "AWS::EC2::LaunchTemplate",
                    ["Properties"] = new Dictionary<string, object>
                    {
                        ["LaunchTemplateName"] = "cfn-lt-test",
                        ["LaunchTemplateData"] = new Dictionary<string, object>
                        {
                            ["InstanceType"] = "t3.medium",
                            ["ImageId"] = "ami-cfn123",
                        },
                    },
                },
            },
        });

        await _cfn.CreateStackAsync(new CreateStackRequest
        {
            StackName = "cfn-lt-stack",
            TemplateBody = template,
        });

        var desc = await _cfn.DescribeStacksAsync(new DescribeStacksRequest { StackName = "cfn-lt-stack" });
        desc.Stacks[0].StackStatus.Value.ShouldBe("CREATE_COMPLETE");

        await _cfn.DeleteStackAsync(new DeleteStackRequest { StackName = "cfn-lt-stack" });
    }

    private async Task AssertBucketExists(string bucketName)
    {
        var buckets = await _s3.ListBucketsAsync();
        var list = buckets.Buckets ?? [];
        list.ShouldContain(b => b.BucketName == bucketName);
    }

    private async Task AssertBucketDoesNotExist(string bucketName)
    {
        var buckets = await _s3.ListBucketsAsync();
        var list = buckets.Buckets ?? [];
        list.ShouldNotContain(b => b.BucketName == bucketName);
    }
}
