using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.Route53;
using Amazon.Route53.Model;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for Unicode handling across services.
/// Verifies that Unicode characters roundtrip correctly through S3, SQS, DynamoDB,
/// SecretsManager, SSM, and Route53.
///
/// Mirrors coverage from ministack/tests/test_unicode.py.
/// </summary>
public sealed class UnicodeTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonS3Client _s3;
    private readonly AmazonSQSClient _sqs;
    private readonly AmazonDynamoDBClient _ddb;
    private readonly AmazonSecretsManagerClient _sm;
    private readonly AmazonSimpleSystemsManagementClient _ssm;
    private readonly AmazonRoute53Client _r53;

    public UnicodeTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _s3 = CreateS3Client(fixture);
        _sqs = CreateSqsClient(fixture);
        _ddb = CreateDdbClient(fixture);
        _sm = CreateSmClient(fixture);
        _ssm = CreateSsmClient(fixture);
        _r53 = CreateR53Client(fixture);
    }

    public async ValueTask InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _s3.Dispose();
        _sqs.Dispose();
        _ddb.Dispose();
        _sm.Dispose();
        _ssm.Dispose();
        _r53.Dispose();
        return ValueTask.CompletedTask;
    }

    // ── Client factory helpers ────────────────────────────────────────────────

    private static AmazonS3Client CreateS3Client(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
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

        return new AmazonS3Client(new BasicAWSCredentials("test", "test"), config);
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

    private static AmazonSecretsManagerClient CreateSmClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonSecretsManagerConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonSecretsManagerClient(new BasicAWSCredentials("test", "test"), config);
    }

    private static AmazonSimpleSystemsManagementClient CreateSsmClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonSimpleSystemsManagementConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonSimpleSystemsManagementClient(new BasicAWSCredentials("test", "test"), config);
    }

    private static AmazonRoute53Client CreateR53Client(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonRoute53Config
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonRoute53Client(new BasicAWSCredentials("test", "test"), config);
    }

    // ── S3 Unicode tests ─────────────────────────────────────────────────────

    [Fact]
    public async Task UnicodeS3ObjectKey()
    {
        await _s3.PutBucketAsync("unicode-keys");
        var key = "données/résumé/文件.txt";
        var bodyText = "Ünïcödé cöntënt 日本語";
        var bodyBytes = System.Text.Encoding.UTF8.GetBytes(bodyText);

        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "unicode-keys",
            Key = key,
            InputStream = new MemoryStream(bodyBytes),
        });

        var resp = await _s3.GetObjectAsync("unicode-keys", key);
        using var reader = new StreamReader(resp.ResponseStream);
        var content = await reader.ReadToEndAsync();
        content.ShouldBe(bodyText);
    }

    [Fact]
    public async Task UnicodeS3Metadata()
    {
        await _s3.PutBucketAsync("unicode-meta");

        // S3 metadata values must be ASCII per AWS/botocore; encode non-ASCII with percent-encoding
        var putReq = new PutObjectRequest
        {
            BucketName = "unicode-meta",
            Key = "file.bin",
            InputStream = new MemoryStream("data"u8.ToArray()),
        };
        putReq.Metadata.Add("filename", Uri.EscapeDataString("résumé.pdf"));
        putReq.Metadata.Add("author", Uri.EscapeDataString("Ñoño"));
        await _s3.PutObjectAsync(putReq);

        var head = await _s3.GetObjectMetadataAsync("unicode-meta", "file.bin");
        Uri.UnescapeDataString(head.Metadata["filename"]).ShouldBe("résumé.pdf");
        Uri.UnescapeDataString(head.Metadata["author"]).ShouldBe("Ñoño");
    }

    // ── DynamoDB Unicode test ────────────────────────────────────────────────

    [Fact]
    public async Task UnicodeDynamoDbItem()
    {
        var table = "unicode-ddb";
        await _ddb.CreateTableAsync(new CreateTableRequest
        {
            TableName = table,
            KeySchema = [new KeySchemaElement { AttributeName = "pk", KeyType = KeyType.HASH }],
            AttributeDefinitions = [new AttributeDefinition { AttributeName = "pk", AttributeType = ScalarAttributeType.S }],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        });

        await _ddb.PutItemAsync(new PutItemRequest
        {
            TableName = table,
            Item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() { S = "ключ" },
                ["value"] = new() { S = "значение 日本語 مرحبا" },
            },
        });

        var resp = await _ddb.GetItemAsync(new GetItemRequest
        {
            TableName = table,
            Key = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() { S = "ключ" },
            },
        });

        resp.Item["value"].S.ShouldBe("значение 日本語 مرحبا");
    }

    // ── SQS Unicode test ─────────────────────────────────────────────────────

    [Fact]
    public async Task UnicodeSqsMessage()
    {
        var q = await _sqs.CreateQueueAsync("unicode-sqs");
        var msg = "こんにちは世界 héllo wörld";

        await _sqs.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl = q.QueueUrl,
            MessageBody = msg,
        });

        var recv = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = q.QueueUrl,
            MaxNumberOfMessages = 1,
        });

        recv.Messages.ShouldHaveSingleItem();
        recv.Messages[0].Body.ShouldBe(msg);
    }

    // ── SecretsManager Unicode test ──────────────────────────────────────────

    [Fact]
    public async Task UnicodeSecretsManager()
    {
        await _sm.CreateSecretAsync(new Amazon.SecretsManager.Model.CreateSecretRequest
        {
            Name = "unicode-secret",
            SecretString = "пароль: 密码",
        });

        var resp = await _sm.GetSecretValueAsync(new Amazon.SecretsManager.Model.GetSecretValueRequest
        {
            SecretId = "unicode-secret",
        });

        resp.SecretString.ShouldBe("пароль: 密码");
    }

    // ── SSM Unicode test ─────────────────────────────────────────────────────

    [Fact]
    public async Task UnicodeSsmParameter()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/unicode/param",
            Value = "값: τιμή",
            Type = ParameterType.String,
        });

        var resp = await _ssm.GetParameterAsync(new GetParameterRequest
        {
            Name = "/unicode/param",
        });

        resp.Parameter.Value.ShouldBe("값: τιμή");
    }

    // ── Route53 Unicode test ─────────────────────────────────────────────────

    [Fact]
    public async Task UnicodeRoute53ZoneComment()
    {
        var resp = await _r53.CreateHostedZoneAsync(new CreateHostedZoneRequest
        {
            Name = "unicode-zone.com",
            CallerReference = "ref-uc-1",
            HostedZoneConfig = new HostedZoneConfig
            {
                Comment = "zona en español — Ünïcödé",
            },
        });

        var zoneId = resp.HostedZone.Id.Split('/').Last();

        var get = await _r53.GetHostedZoneAsync(new GetHostedZoneRequest
        {
            Id = zoneId,
        });

        get.HostedZone.Config.Comment.ShouldBe("zona en español — Ünïcödé");
    }
}
