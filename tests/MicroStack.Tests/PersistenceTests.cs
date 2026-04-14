using Amazon;
using Amazon.Runtime;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using MicroStack.Internal;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests that verify GetState() / RestoreState() roundtrips
/// for services that implement persistence.
///
/// Pattern: create resource via SDK → GetState() → Reset() → verify gone → RestoreState() → verify back.
///
/// Port of ministack/tests/test_ministack_persist.py.
/// </summary>
public sealed class PersistenceTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly ServiceRegistry _registry;
    private readonly AmazonSQSClient _sqs;
    private readonly AmazonSimpleNotificationServiceClient _sns;
    private readonly AmazonSimpleSystemsManagementClient _ssm;
    private readonly AmazonSecretsManagerClient _sm;

    public PersistenceTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _registry = (ServiceRegistry)fixture.Factory.Services.GetService(typeof(ServiceRegistry))!;
        _sqs = CreateSqsClient(fixture);
        _sns = CreateSnsClient(fixture);
        _ssm = CreateSsmClient(fixture);
        _sm = CreateSmClient(fixture);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _sqs.Dispose();
        _sns.Dispose();
        _ssm.Dispose();
        _sm.Dispose();
        return Task.CompletedTask;
    }

    // ── SDK client helpers ──────────────────────────────────────────────────────

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

        return new AmazonSimpleSystemsManagementClient(
            new BasicAWSCredentials("test", "test"), config);
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

        return new AmazonSecretsManagerClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    // ── SQS persistence roundtrip ───────────────────────────────────────────────

    [Fact]
    public async Task SqsPersistRoundtrip()
    {
        // Create a queue
        var created = await _sqs.CreateQueueAsync("persist-sqs-q");
        created.QueueUrl.ShouldNotBeEmpty();

        // Send a message so there's data in the queue
        await _sqs.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl = created.QueueUrl,
            MessageBody = "persist-test-body",
        });

        // Get state
        var handler = _registry.Resolve("sqs")!;
        var state = handler.GetState();
        state.ShouldNotBeNull();

        // Reset and verify queue is gone
        handler.Reset();
        await Should.ThrowAsync<QueueDoesNotExistException>(
            () => _sqs.GetQueueUrlAsync("persist-sqs-q"));

        // Restore and verify queue is back
        handler.RestoreState(state.Value);
        var restored = await _sqs.GetQueueUrlAsync("persist-sqs-q");
        restored.QueueUrl.ShouldBe(created.QueueUrl);
    }

    // ── SNS persistence roundtrip ───────────────────────────────────────────────

    [Fact]
    public async Task SnsPersistRoundtrip()
    {
        // Create a topic
        var created = await _sns.CreateTopicAsync("persist-sns-topic");
        created.TopicArn.ShouldNotBeEmpty();

        // Get state
        var handler = _registry.Resolve("sns")!;
        var state = handler.GetState();
        state.ShouldNotBeNull();

        // Reset and verify topic is gone
        handler.Reset();
        var topics = await _sns.ListTopicsAsync();
        (topics.Topics ?? []).ShouldNotContain(t => t.TopicArn == created.TopicArn);

        // Restore and verify topic is back
        handler.RestoreState(state.Value);
        var restoredTopics = await _sns.ListTopicsAsync();
        restoredTopics.Topics.ShouldContain(t => t.TopicArn == created.TopicArn);
    }

    // ── SSM persistence roundtrip ───────────────────────────────────────────────

    [Fact]
    public async Task SsmPersistRoundtrip()
    {
        // Put a parameter
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/persist/ssm-key",
            Value = "persist-val",
            Type = ParameterType.String,
        });

        // Verify it exists
        var got = await _ssm.GetParameterAsync(new GetParameterRequest { Name = "/persist/ssm-key" });
        got.Parameter.Value.ShouldBe("persist-val");

        // Get state
        var handler = _registry.Resolve("ssm")!;
        var state = handler.GetState();
        state.ShouldNotBeNull();

        // Reset and verify parameter is gone
        handler.Reset();
        await Should.ThrowAsync<ParameterNotFoundException>(
            () => _ssm.GetParameterAsync(new GetParameterRequest { Name = "/persist/ssm-key" }));

        // Restore and verify parameter is back
        handler.RestoreState(state.Value);
        var restored = await _ssm.GetParameterAsync(new GetParameterRequest { Name = "/persist/ssm-key" });
        restored.Parameter.Value.ShouldBe("persist-val");
    }

    // ── SecretsManager persistence roundtrip ────────────────────────────────────

    [Fact]
    public async Task SecretsManagerPersistRoundtrip()
    {
        // Create a secret
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "persist-sm-secret",
            SecretString = "s3cr3t-persist",
        });

        // Verify it exists
        var got = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = "persist-sm-secret",
        });
        got.SecretString.ShouldBe("s3cr3t-persist");

        // Get state
        var handler = _registry.Resolve("secretsmanager")!;
        var state = handler.GetState();
        state.ShouldNotBeNull();

        // Reset and verify secret is gone
        handler.Reset();
        await Should.ThrowAsync<Amazon.SecretsManager.Model.ResourceNotFoundException>(
            () => _sm.GetSecretValueAsync(new GetSecretValueRequest
            {
                SecretId = "persist-sm-secret",
            }));

        // Restore and verify secret is back
        handler.RestoreState(state.Value);
        var restored = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = "persist-sm-secret",
        });
        restored.SecretString.ShouldBe("s3cr3t-persist");
    }

    // ── GetState returns null for unimplemented services ────────────────────────

    [Theory]
    [InlineData("dynamodb")]
    [InlineData("s3")]
    [InlineData("ec2")]
    [InlineData("lambda")]
    [InlineData("states")]
    [InlineData("kms")]
    [InlineData("route53")]
    [InlineData("events")]
    [InlineData("kinesis")]
    public void GetStateReturnsNullForUnimplementedServices(string serviceName)
    {
        var handler = _registry.Resolve(serviceName);
        handler.ShouldNotBeNull();
        handler.GetState().ShouldBeNull();
    }

    // ── Reset clears all state ──────────────────────────────────────────────────

    [Fact]
    public async Task ResetEndpointClearsAllState()
    {
        // Create resources in multiple services
        await _sqs.CreateQueueAsync("persist-reset-q");
        await _sns.CreateTopicAsync("persist-reset-topic");
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/persist/reset-key",
            Value = "reset-val",
            Type = ParameterType.String,
        });

        // Hit the reset endpoint
        var response = await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
        response.StatusCode.ShouldBe(HttpStatusCode.OK);

        // Verify all resources are gone
        await Should.ThrowAsync<QueueDoesNotExistException>(
            () => _sqs.GetQueueUrlAsync("persist-reset-q"));

        var topics = await _sns.ListTopicsAsync();
        (topics.Topics ?? []).ShouldBeEmpty();

        await Should.ThrowAsync<ParameterNotFoundException>(
            () => _ssm.GetParameterAsync(new GetParameterRequest { Name = "/persist/reset-key" }));
    }

    // ── SQS state includes queue name-to-URL mapping ────────────────────────────

    [Fact]
    public async Task SqsStateIncludesQueueNameMapping()
    {
        await _sqs.CreateQueueAsync("persist-name-map-q");

        var handler = _registry.Resolve("sqs")!;
        var state = handler.GetState();
        state.ShouldNotBeNull();

        // State should be a JsonElement with "Queues" and "Names" keys
        state.Value.ValueKind.ShouldBe(System.Text.Json.JsonValueKind.Object);
        state.Value.TryGetProperty("Queues", out _).ShouldBe(true);
        state.Value.TryGetProperty("Names", out _).ShouldBe(true);
    }

    // ── SNS state includes topics key ───────────────────────────────────────────

    [Fact]
    public async Task SnsStateIncludesTopicsKey()
    {
        await _sns.CreateTopicAsync("persist-state-topic");

        var handler = _registry.Resolve("sns")!;
        var state = handler.GetState();
        state.ShouldNotBeNull();

        state.Value.ValueKind.ShouldBe(System.Text.Json.JsonValueKind.Object);
        state.Value.TryGetProperty("Topics", out _).ShouldBe(true);
    }

    // ── SSM state includes parameters key ───────────────────────────────────────

    [Fact]
    public async Task SsmStateIncludesParametersKey()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/persist/state-key",
            Value = "state-val",
            Type = ParameterType.String,
        });

        var handler = _registry.Resolve("ssm")!;
        var state = handler.GetState();
        state.ShouldNotBeNull();

        state.Value.ValueKind.ShouldBe(System.Text.Json.JsonValueKind.Object);
        state.Value.TryGetProperty("Parameters", out _).ShouldBe(true);
    }

    // ── SecretsManager state includes secrets key ───────────────────────────────

    [Fact]
    public async Task SecretsManagerStateIncludesSecretsKey()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "persist-state-secret",
            SecretString = "state-val",
        });

        var handler = _registry.Resolve("secretsmanager")!;
        var state = handler.GetState();
        state.ShouldNotBeNull();

        state.Value.ValueKind.ShouldBe(System.Text.Json.JsonValueKind.Object);
        state.Value.TryGetProperty("Secrets", out _).ShouldBe(true);
    }
}
