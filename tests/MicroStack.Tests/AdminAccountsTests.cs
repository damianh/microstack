using System.Diagnostics;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace MicroStack.Tests;

public sealed class AdminAccountsTests
{
    private const string Default = "000000000000";
    private const string First = "111111111111";
    private const string Second = "222222222222";
    private const string Empty = "333333333333";
    private const string AccountsUrl = "/_microstack/admin/v1/accounts";

    [Fact]
    public async Task Accounts_FollowResources_NotRequests_AndDisappearOnDeletionAndReset()
    {
        using var fixture = new MicroStackFixture();
        (await Accounts(fixture)).ShouldBe([Default]);

        using var queue = await Aws(fixture, Second, "sqs", "AmazonSQS.CreateQueue", """{"QueueName":"known-queue"}""");
        var queueUrl = queue.RootElement.GetProperty("QueueUrl").GetString()!;
        using var table = await Aws(fixture, First, "dynamodb", "DynamoDB_20120810.CreateTable", """
            {"TableName":"known-table","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
             "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"}
            """);
        using var otherQueue = await Aws(fixture, First, "sqs", "AmazonSQS.CreateQueue", """{"QueueName":"known-queue"}""");
        var otherQueueUrl = otherQueue.RootElement.GetProperty("QueueUrl").GetString()!;
        (await Accounts(fixture)).ShouldBe([Default, First, Second]);

        using var list = await Aws(fixture, Empty, "sqs", "AmazonSQS.ListQueues", "{}");
        using var ec2Read = new HttpRequestMessage(HttpMethod.Post, "/");
        ec2Read.Headers.TryAddWithoutValidation("Authorization",
            $"AWS4-HMAC-SHA256 Credential={Empty}/20260913/us-east-1/ec2/aws4_request, SignedHeaders=host, Signature=test");
        ec2Read.Content = new StringContent("Action=DescribeVpcs", Encoding.UTF8, "application/x-www-form-urlencoded");
        using var ec2Response = await fixture.HttpClient.SendAsync(ec2Read);
        ec2Response.EnsureSuccessStatusCode();
        using var browse = await fixture.HttpClient.GetAsync(
            $"/_microstack/admin/v1/services/sqs/resources?accountId={Empty}");
        browse.EnsureSuccessStatusCode();
        using var scopedAccounts = await fixture.HttpClient.GetAsync(AccountsUrl + "?accountId=" + Empty);
        scopedAccounts.EnsureSuccessStatusCode();
        (await Accounts(fixture)).ShouldBe([Default, First, Second]);

        using var deletedQueue = await Aws(fixture, Second, "sqs", "AmazonSQS.DeleteQueue",
            $$"""{"QueueUrl":"{{queueUrl}}"}""");
        (await Accounts(fixture)).ShouldBe([Default, First]);
        using var deletedTable = await Aws(fixture, First, "dynamodb", "DynamoDB_20120810.DeleteTable",
            """{"TableName":"known-table"}""");
        (await Accounts(fixture)).ShouldBe([Default, First]);
        using var deletedOtherQueue = await Aws(fixture, First, "sqs", "AmazonSQS.DeleteQueue",
            $$"""{"QueueUrl":"{{otherQueueUrl}}"}""");
        (await Accounts(fixture)).ShouldBe([Default]);

        using var recreated = await Aws(fixture, Second, "sqs", "AmazonSQS.CreateQueue", """{"QueueName":"known-again"}""");
        using var reset = await fixture.HttpClient.PostAsync("/_microstack/reset", null);
        reset.EnsureSuccessStatusCode();
        (await Accounts(fixture)).ShouldBe([Default]);
    }

    [Fact]
    public async Task ConfiguredDefault_IsAlwaysIncluded_AndDiscoveryDoesNotChangeScope()
    {
        using var fixture = new MicroStackFixture();
        var options = fixture.Factory.Services.GetRequiredService<MicroStackOptions>();
        options.DefaultAccountId = "999999999999";
        using (AccountContext.BeginScope(First))
        {
            (await Accounts(fixture)).ShouldBe(["999999999999"]);
            AccountContext.GetAccountId().ShouldBe(First);
        }
    }

    [Fact]
    public async Task InstanceGlobalResources_DoNotInventOwners()
    {
        using var fixture = new MicroStackFixture();
        using var stream = await Aws(fixture, First, "firehose", "Firehose_20150804.CreateDeliveryStream",
            """{"DeliveryStreamName":"global-stream"}""");
        using var workgroup = await Aws(fixture, First, "athena", "AmazonAthena.CreateWorkGroup",
            """{"Name":"global-workgroup"}""");
        using var bus = await Aws(fixture, Second, "events", "AWSEvents.CreateEventBus",
            """{"Name":"global-bus"}""");
        (await Accounts(fixture)).ShouldBe([Default]);

        using var query = await Aws(fixture, Second, "athena", "AmazonAthena.CreateNamedQuery",
            """{"Name":"retained-query","Database":"db","QueryString":"select 1"}""");
        (await Accounts(fixture)).ShouldBe([Default, Second]);
    }

    [Fact]
    public async Task EmptyLayerContainers_AndSecondaryTags_DoNotKeepDeletedOwners()
    {
        using var fixture = new MicroStackFixture();
        using var layer = await Aws(fixture, First, "lambda", null, "{}",
            "/2018-10-31/layers/known-layer/versions");
        (await Accounts(fixture)).ShouldBe([Default, First]);
        using var deleted = await Aws(fixture, First, "lambda", null, null,
            "/2018-10-31/layers/known-layer/versions/1", HttpMethod.Delete);
        (await Accounts(fixture)).ShouldBe([Default]);

        using var parameter = await Aws(fixture, Second, "ssm", "AmazonSSM.PutParameter",
            """{"Name":"/known/parameter","Value":"value","Type":"String","Tags":[{"Key":"purpose","Value":"test"}]}""");
        (await Accounts(fixture)).ShouldBe([Default, Second]);
        using var removed = await Aws(fixture, Second, "ssm", "AmazonSSM.DeleteParameter",
            """{"Name":"/known/parameter"}""");
        (await Accounts(fixture)).ShouldBe([Default]);
    }

    [Fact]
    public async Task Ec2_SyntheticDefaultsDoNotKeepAccountAfterLastCustomResourceIsDeleted()
    {
        using var fixture = new MicroStackFixture();
        using var createdRequest = Ec2("Action=CreateKeyPair&KeyName=known-key");
        using var created = await fixture.HttpClient.SendAsync(createdRequest);
        created.EnsureSuccessStatusCode();
        (await Accounts(fixture)).ShouldBe([Default, First]);

        using var deletedRequest = Ec2("Action=DeleteKeyPair&KeyName=known-key");
        using var deleted = await fixture.HttpClient.SendAsync(deletedRequest);
        deleted.EnsureSuccessStatusCode();
        (await Accounts(fixture)).ShouldBe([Default]);

        static HttpRequestMessage Ec2(string body)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, "/");
            request.Headers.TryAddWithoutValidation("Authorization",
                $"AWS4-HMAC-SHA256 Credential={First}/20260913/us-east-1/ec2/aws4_request, SignedHeaders=host, Signature=test");
            request.Content = new StringContent(body, Encoding.UTF8, "application/x-www-form-urlencoded");
            return request;
        }
    }

    [Theory]
    [InlineData("/restapis", """{"name":"rest-api"}""")]
    [InlineData("/v2/apis", """{"name":"http-api","protocolType":"HTTP"}""")]
    [InlineData("/apikeys", """{"name":"standalone-key","enabled":true}""")]
    public async Task CompositeApiGateway_DiscoversBothVersionsAndStandaloneResources(string path, string body)
    {
        using var fixture = new MicroStackFixture();
        using var created = await Aws(fixture, First, "apigateway", null, body, path);
        (await Accounts(fixture)).ShouldBe([Default, First]);
    }

    [Fact]
    public async Task PersistedDictionaryRestore_DiscoversAccountsInFreshHost_WithoutLeakingOldOwners()
    {
        var directory = Path.Combine(Directory.GetCurrentDirectory(), "TestResults", "known-accounts-" + Guid.NewGuid());
        var options = new MicroStackOptions { PersistState = true, StateDir = directory };
        try
        {
            using (var original = new MicroStackFixture())
            {
                using var queue = await Aws(original, First, "sqs", "AmazonSQS.CreateQueue", """{"QueueName":"persisted"}""");
                using var second = await Aws(original, Second, "sqs", "AmazonSQS.CreateQueue", """{"QueueName":"persisted"}""");
                new StatePersistence(NullLogger<StatePersistence>.Instance, Registry(original), options).SaveAll();
            }

            using var restored = new MicroStackFixture();
            (await Accounts(restored)).ShouldBe([Default]);
            var persistence = new StatePersistence(NullLogger<StatePersistence>.Instance, Registry(restored), options);
            persistence.RestoreAll();
            (await Accounts(restored)).ShouldBe([Default, First, Second]);
            using var queues = await Aws(restored, First, "sqs", "AmazonSQS.ListQueues", "{}");
            queues.RootElement.GetProperty("QueueUrls").GetArrayLength().ShouldBe(1);
            Registry(restored).ResetAll();
            persistence.DeleteAll();
            (await Accounts(restored)).ShouldBe([Default]);
            persistence.RestoreAll();
            (await Accounts(restored)).ShouldBe([Default]);
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task InvalidRestoredIds_AreExcluded_WithoutReadingPayloadsOrPersistence()
    {
        using var fixture = new MicroStackFixture();
        var source = new RetainedAccountSource();
        source.State.FromRaw(new[] { First, Second, "short", "１２３４５６７８９０１２", "11111111111x", "11111111111\n", null! }
            .Select(account => new KeyValuePair<(string, string), string>((account, "resource"), "not inspected")));
        Registry(fixture).Register(source);
        (await Accounts(fixture)).ShouldBe([Default, First, Second]);
        source.State.Clear();
        (await Accounts(fixture)).ShouldBe([Default]);
    }

    [Fact]
    public void DictionaryEnumeration_FollowsAllMutationPaths_WithoutAddingReadAccounts()
    {
        var state = new AccountScopedDictionary<string, List<int>>();
        using (AccountContext.BeginScope(Empty))
        {
            state.ContainsKey("missing").ShouldBeFalse();
            state.TryGetValue("missing", out _).ShouldBeFalse();
            state.Values.ShouldBeEmpty();
            state.GetAccountIds().ShouldBeEmpty();
        }
        using (AccountContext.BeginScope(First))
        {
            state.TryAdd("a", [1]).ShouldBeTrue();
            state.GetOrAdd("b", _ => []).ShouldBeEmpty();
            state.AddOrUpdate("c", [3], (_, value) => value);
            state["d"] = [4];
        }
        state.GetAccountIds().ShouldBe([First]);
        state.GetAccountIds(value => value.Count > 0).ShouldBe([First]);
        using (AccountContext.BeginScope(First))
        {
            state.TryRemove("a", out _).ShouldBeTrue();
            state.TryRemove("c", out _).ShouldBeTrue();
            state.TryRemove("d", out _).ShouldBeTrue();
        }
        state.GetAccountIds(value => value.Count > 0).ShouldBeEmpty();
        state.FromRaw([new KeyValuePair<(string, string), List<int>>((Second, "restored"), [2])]);
        state.GetAccountIds().ShouldBe([Second]);
        state.FromRaw([]);
        state.GetAccountIds().ShouldBeEmpty();
    }

    [Theory]
    [InlineData("")]
    [InlineData("123")]
    [InlineData("１２３４５６７８９０１２")]
    [InlineData("12345678901x")]
    public void InvalidDefaultConfiguration_IsRejected(string value) =>
        Should.Throw<ArgumentException>(() => new MicroStackOptions { DefaultAccountId = value });

    [Fact]
    public void DisabledSources_DoNotContributeRetainedAccounts()
    {
        var registry = new ServiceRegistry(new MicroStackOptions { Services = "sqs" });
        var source = new RetainedAccountSource();
        using (AccountContext.BeginScope(First))
            source.State["resource"] = "retained";
        registry.Register(source);
        registry.GetKnownAccountIds(Default).ShouldBe([Default]);
    }

    [Fact]
    public async Task ProgramStartup_RestoresKnownAccounts_BeforeServingRequests()
    {
        var directory = Path.Combine(Directory.GetCurrentDirectory(), "TestResults", "accounts-startup-" + Guid.NewGuid());
        try
        {
            using (var fixture = new MicroStackFixture())
            {
                using var queue = await Aws(fixture, First, "sqs", "AmazonSQS.CreateQueue", """{"QueueName":"startup"}""");
                var options = new MicroStackOptions { PersistState = true, StateDir = directory };
                new StatePersistence(NullLogger<StatePersistence>.Instance, Registry(fixture), options).SaveAll();
            }

            var start = new ProcessStartInfo("dotnet")
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                WorkingDirectory = AppContext.BaseDirectory,
            };
            start.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, "MicroStack.dll"));
            start.Environment["DOTNET_ENVIRONMENT"] = "Testing";
            start.Environment["ASPNETCORE_ENVIRONMENT"] = "Testing";
            start.Environment["ASPNETCORE_URLS"] = "http://127.0.0.1:0";
            start.Environment["MICROSTACK_ACCOUNT_ID"] = Default;
            start.Environment["PERSIST_STATE"] = "1";
            start.Environment["STATE_DIR"] = directory;
            start.Environment["SERVICES"] = "sqs";
            using var process = Process.Start(start)!;
            var errors = process.StandardError.ReadToEndAsync();
            try
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                string? address = null;
                while (await process.StandardOutput.ReadLineAsync(timeout.Token) is { } line)
                {
                    const string marker = "Now listening on: ";
                    var index = line.IndexOf(marker, StringComparison.Ordinal);
                    if (index < 0)
                        continue;
                    address = line[(index + marker.Length)..].Trim();
                    break;
                }
                address.ShouldNotBeNull();
                var output = process.StandardOutput.ReadToEndAsync();
                using var client = new HttpClient(new SocketsHttpHandler { UseProxy = false })
                {
                    BaseAddress = new Uri(address),
                };
                (await client.GetFromJsonAsync(AccountsUrl, AdminJsonContext.Default.StringArray))
                    .ShouldBe([Default, First]);
                using var reset = await client.PostAsync("/_microstack/reset", null);
                reset.EnsureSuccessStatusCode();
                (await client.GetFromJsonAsync(AccountsUrl, AdminJsonContext.Default.StringArray))
                    .ShouldBe([Default]);
                File.Exists(Path.Combine(directory, "sqs.json")).ShouldBeFalse();
                process.Kill();
                await process.WaitForExitAsync();
                await output;
                (await errors).ShouldBeEmpty();
            }
            finally
            {
                if (!process.HasExited)
                {
                    process.Kill();
                    await process.WaitForExitAsync();
                }
            }
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    private static ServiceRegistry Registry(MicroStackFixture fixture) =>
        fixture.Factory.Services.GetRequiredService<ServiceRegistry>();

    private static async Task<string[]> Accounts(MicroStackFixture fixture)
    {
        using var response = await fixture.HttpClient.GetAsync(AccountsUrl);
        response.EnsureSuccessStatusCode();
        response.Content.Headers.ContentType!.MediaType.ShouldBe("application/json");
        response.Headers.CacheControl!.NoStore.ShouldBeTrue();
        return (await response.Content.ReadFromJsonAsync(AdminJsonContext.Default.StringArray))!;
    }

    private static async Task<JsonDocument> Aws(
        MicroStackFixture fixture, string account, string service, string? target, string? body,
        string path = "/", HttpMethod? method = null)
    {
        using var request = new HttpRequestMessage(method ?? HttpMethod.Post, path);
        request.Headers.TryAddWithoutValidation("Authorization",
            $"AWS4-HMAC-SHA256 Credential={account}/20260913/us-east-1/{service}/aws4_request, SignedHeaders=host, Signature=test");
        if (target is not null)
            request.Headers.TryAddWithoutValidation("X-Amz-Target", target);
        if (body is not null)
            request.Content = new StringContent(body, Encoding.UTF8, "application/x-amz-json-1.1");
        using var response = await fixture.HttpClient.SendAsync(request);
        var json = await response.Content.ReadAsStringAsync();
        response.IsSuccessStatusCode.ShouldBeTrue(json);
        return JsonDocument.Parse(string.IsNullOrWhiteSpace(json) ? "{}" : json);
    }

    private sealed class RetainedAccountSource : IServiceHandler, IKnownAccountSource
    {
        internal AccountScopedDictionary<string, string> State { get; } = new();
        public string ServiceName => "retained-account-test";
        public IEnumerable<string> GetKnownAccountIds() => State.GetAccountIds();
        public Task<ServiceResponse> HandleAsync(ServiceRequest request) => throw new NotSupportedException();
        public void Reset() => State.Clear();
        public JsonElement? GetState() => throw new InvalidOperationException("Discovery must not serialize state.");
        public void RestoreState(JsonElement state) => throw new NotSupportedException();
    }
}
