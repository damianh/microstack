using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for multi-tenancy: dynamic Account ID derived from AWS_ACCESS_KEY_ID.
///
/// When the access key is a 12-digit number, MicroStack uses it as the Account ID
/// in all ARN generation. Non-numeric keys (like "test") fall back to the default
/// 000000000000.
///
/// Mirrors coverage from ministack/tests/test_multitenancy.py.
/// </summary>
public sealed class MultiTenancyTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;

    public MultiTenancyTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync() => Task.CompletedTask;

    // ── Client factory helpers ────────────────────────────────────────────────

    private AmazonSecurityTokenServiceClient CreateStsClient(string accessKey)
    {
        var innerHandler = _fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonSecurityTokenServiceConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonSecurityTokenServiceClient(
            new BasicAWSCredentials(accessKey, "test"), config);
    }

    private AmazonSQSClient CreateSqsClient(string accessKey)
    {
        var innerHandler = _fixture.Factory.Server.CreateHandler();
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

        return new AmazonSQSClient(new BasicAWSCredentials(accessKey, "test"), config);
    }

    private AmazonS3Client CreateS3Client(string accessKey)
    {
        var innerHandler = _fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonS3Config
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            ForcePathStyle = true,
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonS3Client(new BasicAWSCredentials(accessKey, "test"), config);
    }

    private AmazonDynamoDBClient CreateDdbClient(string accessKey)
    {
        var innerHandler = _fixture.Factory.Server.CreateHandler();
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

        return new AmazonDynamoDBClient(new BasicAWSCredentials(accessKey, "test"), config);
    }

    // ── STS GetCallerIdentity ────────────────────────────────────────────────

    [Fact]
    public async Task DefaultAccountId()
    {
        using var sts = CreateStsClient("test");
        var resp = await sts.GetCallerIdentityAsync(new GetCallerIdentityRequest());
        Assert.Equal("000000000000", resp.Account);
    }

    [Fact]
    public async Task TwelveDigitAccessKeyBecomesAccountId()
    {
        using var sts = CreateStsClient("123456789012");
        var resp = await sts.GetCallerIdentityAsync(new GetCallerIdentityRequest());
        Assert.Equal("123456789012", resp.Account);
    }

    [Fact]
    public async Task DifferentTwelveDigitKeysGetDifferentAccounts()
    {
        using var stsA = CreateStsClient("111111111111");
        using var stsB = CreateStsClient("222222222222");
        Assert.Equal("111111111111", (await stsA.GetCallerIdentityAsync(new GetCallerIdentityRequest())).Account);
        Assert.Equal("222222222222", (await stsB.GetCallerIdentityAsync(new GetCallerIdentityRequest())).Account);
    }

    [Fact]
    public async Task NonTwelveDigitNumericFallsBack()
    {
        using var sts = CreateStsClient("12345");
        var resp = await sts.GetCallerIdentityAsync(new GetCallerIdentityRequest());
        Assert.Equal("000000000000", resp.Account);
    }

    // ── SQS: queue ARN uses dynamic account ──────────────────────────────────

    [Fact]
    public async Task SqsQueueArnUsesDynamicAccount()
    {
        using var sqs = CreateSqsClient("048408301323");
        var q = await sqs.CreateQueueAsync("mt-test-queue");
        try
        {
            var attrs = await sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
            {
                QueueUrl = q.QueueUrl,
                AttributeNames = ["QueueArn"],
            });
            var arn = attrs.Attributes["QueueArn"];
            Assert.Contains("048408301323", arn);
        }
        finally
        {
            await sqs.DeleteQueueAsync(q.QueueUrl);
        }
    }

    // ── SQS: queues isolated by account ──────────────────────────────────────

    [Fact]
    public async Task SqsQueuesIsolatedByAccount()
    {
        using var sqsA = CreateSqsClient("111111111111");
        using var sqsB = CreateSqsClient("222222222222");

        var qA = await sqsA.CreateQueueAsync("isolation-test");
        try
        {
            var qB = await sqsB.CreateQueueAsync("isolation-test");
            try
            {
                // Both should get their own queue with their own account in the ARN
                var attrsA = await sqsA.GetQueueAttributesAsync(new GetQueueAttributesRequest
                {
                    QueueUrl = qA.QueueUrl,
                    AttributeNames = ["QueueArn"],
                });
                var attrsB = await sqsB.GetQueueAttributesAsync(new GetQueueAttributesRequest
                {
                    QueueUrl = qB.QueueUrl,
                    AttributeNames = ["QueueArn"],
                });

                Assert.Contains("111111111111", attrsA.Attributes["QueueArn"]);
                Assert.Contains("222222222222", attrsB.Attributes["QueueArn"]);
            }
            finally
            {
                await sqsB.DeleteQueueAsync(qB.QueueUrl);
            }
        }
        finally
        {
            await sqsA.DeleteQueueAsync(qA.QueueUrl);
        }
    }

    // ── S3: buckets isolated by account ──────────────────────────────────────

    [Fact]
    public async Task S3BucketsIsolatedByAccount()
    {
        using var s3A = CreateS3Client("111111111111");
        using var s3B = CreateS3Client("222222222222");

        await s3A.PutBucketAsync("mt-bucket-test");
        try
        {
            // Account B should not see Account A's bucket
            var listB = await s3B.ListBucketsAsync();
            var bucketsB = listB.Buckets ?? [];
            Assert.DoesNotContain(bucketsB, b => b.BucketName == "mt-bucket-test");

            // Account A should see its own bucket
            var listA = await s3A.ListBucketsAsync();
            var bucketsA = listA.Buckets ?? [];
            Assert.Contains(bucketsA, b => b.BucketName == "mt-bucket-test");
        }
        finally
        {
            await s3A.DeleteBucketAsync("mt-bucket-test");
        }
    }

    // ── DynamoDB: tables isolated by account ─────────────────────────────────

    [Fact]
    public async Task DynamoDbTablesIsolatedByAccount()
    {
        using var ddbA = CreateDdbClient("111111111111");
        using var ddbB = CreateDdbClient("222222222222");

        await ddbA.CreateTableAsync(new CreateTableRequest
        {
            TableName = "mt-table-test",
            KeySchema = [new KeySchemaElement { AttributeName = "pk", KeyType = KeyType.HASH }],
            AttributeDefinitions = [new AttributeDefinition { AttributeName = "pk", AttributeType = ScalarAttributeType.S }],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        });

        try
        {
            // Account B should not see Account A's table
            var listB = await ddbB.ListTablesAsync();
            Assert.DoesNotContain(listB.TableNames, t => t == "mt-table-test");

            // Account A should see its own table
            var listA = await ddbA.ListTablesAsync();
            Assert.Contains("mt-table-test", listA.TableNames);
        }
        finally
        {
            await ddbA.DeleteTableAsync("mt-table-test");
        }
    }

    // ── Reset clears all accounts ────────────────────────────────────────────

    [Fact]
    public async Task ResetClearsAllAccounts()
    {
        using var sqsA = CreateSqsClient("111111111111");
        using var sqsB = CreateSqsClient("222222222222");

        await sqsA.CreateQueueAsync("reset-test-a");
        await sqsB.CreateQueueAsync("reset-test-b");

        // Verify both queues exist
        var listA = await sqsA.ListQueuesAsync(new ListQueuesRequest());
        var listB = await sqsB.ListQueuesAsync(new ListQueuesRequest());
        Assert.Contains(listA.QueueUrls, u => u.Contains("reset-test-a"));
        Assert.Contains(listB.QueueUrls, u => u.Contains("reset-test-b"));

        // Reset
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);

        // Both should be empty after reset
        listA = await sqsA.ListQueuesAsync(new ListQueuesRequest());
        listB = await sqsB.ListQueuesAsync(new ListQueuesRequest());
        Assert.DoesNotContain(listA.QueueUrls, u => u.Contains("reset-test-a"));
        Assert.DoesNotContain(listB.QueueUrls, u => u.Contains("reset-test-b"));
    }
}
