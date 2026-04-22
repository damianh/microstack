using System.Diagnostics;
using System.IO.Compression;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.Lambda;
using Amazon.Lambda.Model;
using Amazon.Runtime;
using Amazon.SQS;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the Lambda service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Covers control plane operations: Function CRUD, Versioning, Aliases, Layers,
/// Tags, Permissions, Concurrency, Function URLs, ESM CRUD, Invoke stubs,
/// Event Invoke Config, Provisioned Concurrency.
/// </summary>
public sealed class LambdaTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonLambdaClient _lambda;

    private const string LambdaRole = "arn:aws:iam::000000000000:role/lambda-role";
    private const string PythonCode = "def handler(event, context):\n    return {\"statusCode\": 200, \"body\": \"ok\"}\n";

    public LambdaTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _lambda = CreateClient(fixture);
    }

    private static AmazonLambdaClient CreateClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonLambdaConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonLambdaClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async ValueTask InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _lambda.Dispose();
        return ValueTask.CompletedTask;
    }

    private static MemoryStream MakeZip(string filename, string content)
    {
        var ms = new MemoryStream();
        using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, leaveOpen: true))
        {
            var entry = archive.CreateEntry(filename);
            using var writer = new StreamWriter(entry.Open());
            writer.Write(content);
        }

        ms.Position = 0;
        return ms;
    }

    private async Task<CreateFunctionResponse> CreateTestFunction(string name)
    {
        using var zip = MakeZip("index.py", PythonCode);
        return await _lambda.CreateFunctionAsync(new CreateFunctionRequest
        {
            FunctionName = name,
            Runtime = Runtime.Python39,
            Role = LambdaRole,
            Handler = "index.handler",
            Code = new FunctionCode { ZipFile = zip },
        });
    }

    // -- CreateFunction --------------------------------------------------------

    [Fact]
    public async Task CreateFunction()
    {
        using var zip = MakeZip("index.py", PythonCode);
        var result = await _lambda.CreateFunctionAsync(new CreateFunctionRequest
        {
            FunctionName = "test-func",
            Runtime = Runtime.Python39,
            Role = LambdaRole,
            Handler = "index.handler",
            Code = new FunctionCode { ZipFile = zip },
        });

        result.FunctionName.ShouldBe("test-func");
        result.FunctionArn.ShouldContain("test-func");
        result.Runtime?.Value.ShouldBe("python3.9");
        result.Handler.ShouldBe("index.handler");
        (result.CodeSize > 0).ShouldBe(true);
        result.CodeSha256.ShouldNotBeEmpty();
        result.Version.ShouldBe("$LATEST");
        result.State.ShouldBe(State.Active);
    }

    [Fact]
    public async Task CreateFunctionDuplicateNameFails()
    {
        await CreateTestFunction("dup-func");

        var ex = await Should.ThrowAsync<ResourceConflictException>(() =>
            CreateTestFunction("dup-func"));

        ex.Message.ShouldContain("already exist");
    }

    // -- GetFunction -----------------------------------------------------------

    [Fact]
    public async Task GetFunction()
    {
        await CreateTestFunction("get-func");

        var result = await _lambda.GetFunctionAsync(new GetFunctionRequest
        {
            FunctionName = "get-func",
        });

        result.Configuration.FunctionName.ShouldBe("get-func");
        result.Code.ShouldNotBeNull();
        result.Tags.ShouldNotBeNull();
    }

    [Fact]
    public async Task GetFunctionNotFound()
    {
        await Should.ThrowAsync<ResourceNotFoundException>(() =>
            _lambda.GetFunctionAsync(new GetFunctionRequest
            {
                FunctionName = "nonexistent-func",
            }));
    }

    // -- ListFunctions ---------------------------------------------------------

    [Fact]
    public async Task ListFunctions()
    {
        await CreateTestFunction("list-func-a");
        await CreateTestFunction("list-func-b");

        var result = await _lambda.ListFunctionsAsync(new ListFunctionsRequest());

        (result.Functions.Count >= 2).ShouldBe(true);
        result.Functions.ShouldContain(f => f.FunctionName == "list-func-a");
        result.Functions.ShouldContain(f => f.FunctionName == "list-func-b");
    }

    // -- DeleteFunction --------------------------------------------------------

    [Fact]
    public async Task DeleteFunction()
    {
        await CreateTestFunction("del-func");

        await _lambda.DeleteFunctionAsync(new DeleteFunctionRequest
        {
            FunctionName = "del-func",
        });

        await Should.ThrowAsync<ResourceNotFoundException>(() =>
            _lambda.GetFunctionAsync(new GetFunctionRequest
            {
                FunctionName = "del-func",
            }));
    }

    // -- UpdateFunctionCode ----------------------------------------------------

    [Fact]
    public async Task UpdateFunctionCode()
    {
        var create = await CreateTestFunction("update-code-func");
        var originalCodeSize = create.CodeSize;
        var originalSha = create.CodeSha256;

        using var newZip = MakeZip("index.py", PythonCode + "\n# more code\n");
        var result = await _lambda.UpdateFunctionCodeAsync(new UpdateFunctionCodeRequest
        {
            FunctionName = "update-code-func",
            ZipFile = newZip,
        });

        result.CodeSize.ShouldNotBe(originalCodeSize);
        result.CodeSha256.ShouldNotBe(originalSha);
        result.FunctionName.ShouldBe("update-code-func");
    }

    // -- UpdateFunctionConfiguration -------------------------------------------

    [Fact]
    public async Task UpdateFunctionConfiguration()
    {
        await CreateTestFunction("update-config-func");

        var result = await _lambda.UpdateFunctionConfigurationAsync(new UpdateFunctionConfigurationRequest
        {
            FunctionName = "update-config-func",
            Handler = "new_handler.handler",
            Description = "Updated description",
            Timeout = 30,
            MemorySize = 256,
            Environment = new Amazon.Lambda.Model.Environment
            {
                Variables = new Dictionary<string, string> { ["KEY"] = "value" },
            },
        });

        result.Handler.ShouldBe("new_handler.handler");
        result.Description.ShouldBe("Updated description");
        result.Timeout.ShouldBe(30);
        result.MemorySize.ShouldBe(256);
    }

    // -- Tags ------------------------------------------------------------------

    [Fact]
    public async Task Tags()
    {
        var create = await CreateTestFunction("tag-func");

        await _lambda.TagResourceAsync(new TagResourceRequest
        {
            Resource = create.FunctionArn,
            Tags = new Dictionary<string, string>
            {
                ["env"] = "prod",
                ["team"] = "platform",
            },
        });

        var tags = await _lambda.ListTagsAsync(new ListTagsRequest
        {
            Resource = create.FunctionArn,
        });

        (tags.Tags.Count >= 2).ShouldBe(true);
        tags.Tags["env"].ShouldBe("prod");
        tags.Tags["team"].ShouldBe("platform");

        await _lambda.UntagResourceAsync(new UntagResourceRequest
        {
            Resource = create.FunctionArn,
            TagKeys = ["team"],
        });

        var tags2 = await _lambda.ListTagsAsync(new ListTagsRequest
        {
            Resource = create.FunctionArn,
        });

        tags2.Tags.Keys.ShouldNotContain("team");
        tags2.Tags["env"].ShouldBe("prod");
    }

    // -- AddPermission / GetPolicy / RemovePermission --------------------------

    [Fact]
    public async Task AddPermission()
    {
        await CreateTestFunction("perm-func");

        var result = await _lambda.AddPermissionAsync(new AddPermissionRequest
        {
            FunctionName = "perm-func",
            StatementId = "stmt1",
            Action = "lambda:InvokeFunction",
            Principal = "s3.amazonaws.com",
        });

        result.Statement.ShouldNotBeNull();
        result.Statement.ShouldContain("stmt1");
    }

    [Fact]
    public async Task AddRemovePermission()
    {
        await CreateTestFunction("perm-crud-func");

        await _lambda.AddPermissionAsync(new AddPermissionRequest
        {
            FunctionName = "perm-crud-func",
            StatementId = "stmt-crud",
            Action = "lambda:InvokeFunction",
            Principal = "s3.amazonaws.com",
        });

        var policy = await _lambda.GetPolicyAsync(new GetPolicyRequest
        {
            FunctionName = "perm-crud-func",
        });

        policy.Policy.ShouldNotBeNull();
        policy.Policy.ShouldContain("stmt-crud");

        await _lambda.RemovePermissionAsync(new RemovePermissionRequest
        {
            FunctionName = "perm-crud-func",
            StatementId = "stmt-crud",
        });

        // After removal, there should be no statements → ResourceNotFound
        await Should.ThrowAsync<ResourceNotFoundException>(() =>
            _lambda.GetPolicyAsync(new GetPolicyRequest
            {
                FunctionName = "perm-crud-func",
            }));
    }

    // -- ListVersionsByFunction ------------------------------------------------

    [Fact]
    public async Task ListVersionsByFunction()
    {
        await CreateTestFunction("versions-func");

        var result = await _lambda.ListVersionsByFunctionAsync(new ListVersionsByFunctionRequest
        {
            FunctionName = "versions-func",
        });

        result.Versions.ShouldHaveSingleItem(); // Only $LATEST
        result.Versions[0].Version.ShouldBe("$LATEST");
    }

    // -- PublishVersion --------------------------------------------------------

    [Fact]
    public async Task PublishVersion()
    {
        await CreateTestFunction("publish-func");

        var published = await _lambda.PublishVersionAsync(new PublishVersionRequest
        {
            FunctionName = "publish-func",
        });

        published.Version.ShouldBe("1");
        published.FunctionArn.ShouldContain("publish-func");

        var versions = await _lambda.ListVersionsByFunctionAsync(new ListVersionsByFunctionRequest
        {
            FunctionName = "publish-func",
        });

        versions.Versions.Count.ShouldBe(2); // $LATEST + version 1
    }

    [Fact]
    public async Task PublishVersionWithCreate()
    {
        using var zip = MakeZip("index.py", PythonCode);
        var result = await _lambda.CreateFunctionAsync(new CreateFunctionRequest
        {
            FunctionName = "publish-at-create",
            Runtime = Runtime.Python39,
            Role = LambdaRole,
            Handler = "index.handler",
            Code = new FunctionCode { ZipFile = zip },
            Publish = true,
        });

        result.Version.ShouldBe("1");
    }

    // -- Invoke stubs ----------------------------------------------------------

    [Fact]
    public async Task InvokeEventTypeReturns202()
    {
        await CreateTestFunction("invoke-event-func");

        var result = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-event-func",
            InvocationType = InvocationType.Event,
        });

        result.StatusCode.ShouldBe(202);
    }

    [Fact]
    public async Task InvokeDryRunReturns204()
    {
        await CreateTestFunction("invoke-dry-func");

        var result = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-dry-func",
            InvocationType = InvocationType.DryRun,
        });

        result.StatusCode.ShouldBe(204);
    }

    // -- Alias CRUD ------------------------------------------------------------

    [Fact]
    public async Task AliasCrud()
    {
        await CreateTestFunction("alias-func");

        // Publish a version first
        var published = await _lambda.PublishVersionAsync(new PublishVersionRequest
        {
            FunctionName = "alias-func",
        });

        // Create alias
        var created = await _lambda.CreateAliasAsync(new CreateAliasRequest
        {
            FunctionName = "alias-func",
            Name = "prod",
            FunctionVersion = published.Version,
            Description = "Production alias",
        });

        created.Name.ShouldBe("prod");
        created.FunctionVersion.ShouldBe(published.Version);

        // Get alias
        var alias = await _lambda.GetAliasAsync(new GetAliasRequest
        {
            FunctionName = "alias-func",
            Name = "prod",
        });

        alias.Name.ShouldBe("prod");
        alias.FunctionVersion.ShouldBe(published.Version);

        // Update alias
        var updated = await _lambda.UpdateAliasAsync(new UpdateAliasRequest
        {
            FunctionName = "alias-func",
            Name = "prod",
            Description = "Updated prod alias",
        });

        updated.Description.ShouldBe("Updated prod alias");

        // List aliases
        var list = await _lambda.ListAliasesAsync(new ListAliasesRequest
        {
            FunctionName = "alias-func",
        });

        list.Aliases.ShouldHaveSingleItem();
        list.Aliases[0].Name.ShouldBe("prod");

        // Delete alias
        await _lambda.DeleteAliasAsync(new DeleteAliasRequest
        {
            FunctionName = "alias-func",
            Name = "prod",
        });

        var listAfter = await _lambda.ListAliasesAsync(new ListAliasesRequest
        {
            FunctionName = "alias-func",
        });

        listAfter.Aliases.ShouldBeEmpty();
    }

    // -- Function Concurrency --------------------------------------------------

    [Fact]
    public async Task FunctionConcurrency()
    {
        await CreateTestFunction("concurrency-func");

        // Put concurrency
        var put = await _lambda.PutFunctionConcurrencyAsync(new PutFunctionConcurrencyRequest
        {
            FunctionName = "concurrency-func",
            ReservedConcurrentExecutions = 10,
        });

        put.ReservedConcurrentExecutions.ShouldBe(10);

        // Get concurrency
        var get = await _lambda.GetFunctionConcurrencyAsync(new GetFunctionConcurrencyRequest
        {
            FunctionName = "concurrency-func",
        });

        get.ReservedConcurrentExecutions.ShouldBe(10);

        // Delete concurrency
        await _lambda.DeleteFunctionConcurrencyAsync(new DeleteFunctionConcurrencyRequest
        {
            FunctionName = "concurrency-func",
        });

        var getAfter = await _lambda.GetFunctionConcurrencyAsync(new GetFunctionConcurrencyRequest
        {
            FunctionName = "concurrency-func",
        });

        getAfter.ReservedConcurrentExecutions.ShouldBe(0);
    }

    // -- ListFunctions Pagination ----------------------------------------------

    [Fact]
    public async Task ListFunctionsPagination()
    {
        for (var i = 0; i < 5; i++)
        {
            await CreateTestFunction($"page-func-{i:D2}");
        }

        var page1 = await _lambda.ListFunctionsAsync(new ListFunctionsRequest
        {
            MaxItems = 2,
        });

        page1.Functions.Count.ShouldBe(2);
        page1.NextMarker.ShouldNotBeNull();

        var page2 = await _lambda.ListFunctionsAsync(new ListFunctionsRequest
        {
            MaxItems = 2,
            Marker = page1.NextMarker,
        });

        page2.Functions.Count.ShouldBe(2);

        // Pages should have different functions
        page2.Functions.ConvertAll(f => f.FunctionName).ShouldNotContain(page1.Functions[0].FunctionName);
    }

    // -- Layer tests -----------------------------------------------------------

    [Fact]
    public async Task LayerPublish()
    {
        using var zip = MakeZip("python/mylib.py", "# layer code");
        var result = await _lambda.PublishLayerVersionAsync(new PublishLayerVersionRequest
        {
            LayerName = "test-layer",
            Description = "A test layer",
            Content = new LayerVersionContentInput { ZipFile = zip },
            CompatibleRuntimes = [Runtime.Python39],
        });

        result.Version.ShouldBe(1);
        result.LayerArn.ShouldContain("test-layer");
        result.LayerVersionArn.ShouldContain("test-layer");
        result.Description.ShouldBe("A test layer");
    }

    [Fact]
    public async Task LayerGetVersion()
    {
        using var zip = MakeZip("python/mylib.py", "# layer code");
        var publish = await _lambda.PublishLayerVersionAsync(new PublishLayerVersionRequest
        {
            LayerName = "get-layer",
            Content = new LayerVersionContentInput { ZipFile = zip },
        });

        var result = await _lambda.GetLayerVersionAsync(new GetLayerVersionRequest
        {
            LayerName = "get-layer",
            VersionNumber = publish.Version,
        });

        result.Version.ShouldBe(publish.Version);
        result.Content.ShouldNotBeNull();
        (result.Content.CodeSize > 0).ShouldBe(true);
    }

    [Fact]
    public async Task LayerListVersions()
    {
        using var zip1 = MakeZip("python/mylib.py", "# v1");
        await _lambda.PublishLayerVersionAsync(new PublishLayerVersionRequest
        {
            LayerName = "list-ver-layer",
            Content = new LayerVersionContentInput { ZipFile = zip1 },
        });

        using var zip2 = MakeZip("python/mylib.py", "# v2");
        await _lambda.PublishLayerVersionAsync(new PublishLayerVersionRequest
        {
            LayerName = "list-ver-layer",
            Content = new LayerVersionContentInput { ZipFile = zip2 },
        });

        var result = await _lambda.ListLayerVersionsAsync(new ListLayerVersionsRequest
        {
            LayerName = "list-ver-layer",
        });

        result.LayerVersions.Count.ShouldBe(2);
    }

    [Fact]
    public async Task LayerListLayers()
    {
        using var zip = MakeZip("python/mylib.py", "# code");
        await _lambda.PublishLayerVersionAsync(new PublishLayerVersionRequest
        {
            LayerName = "listed-layer",
            Content = new LayerVersionContentInput { ZipFile = zip },
        });

        var result = await _lambda.ListLayersAsync(new ListLayersRequest());

        (result.Layers.Count >= 1).ShouldBe(true);
        result.Layers.ShouldContain(l => l.LayerName == "listed-layer");
    }

    [Fact]
    public async Task LayerDeleteVersion()
    {
        using var zip = MakeZip("python/mylib.py", "# code");
        var publish = await _lambda.PublishLayerVersionAsync(new PublishLayerVersionRequest
        {
            LayerName = "del-layer",
            Content = new LayerVersionContentInput { ZipFile = zip },
        });

        await _lambda.DeleteLayerVersionAsync(new DeleteLayerVersionRequest
        {
            LayerName = "del-layer",
            VersionNumber = publish.Version,
        });

        var versions = await _lambda.ListLayerVersionsAsync(new ListLayerVersionsRequest
        {
            LayerName = "del-layer",
        });

        versions.LayerVersions.ShouldBeEmpty();
    }

    // -- Function URL Config ---------------------------------------------------

    [Fact]
    public async Task FunctionUrlConfigCrud()
    {
        await CreateTestFunction("url-func");

        // Create
        var created = await _lambda.CreateFunctionUrlConfigAsync(new CreateFunctionUrlConfigRequest
        {
            FunctionName = "url-func",
            AuthType = FunctionUrlAuthType.NONE,
        });

        created.FunctionUrl.ShouldNotBeEmpty();
        created.AuthType.ShouldBe(FunctionUrlAuthType.NONE);
        created.FunctionArn.ShouldContain("url-func");

        // Get
        var got = await _lambda.GetFunctionUrlConfigAsync(new GetFunctionUrlConfigRequest
        {
            FunctionName = "url-func",
        });

        got.FunctionUrl.ShouldBe(created.FunctionUrl);

        // Update
        var updated = await _lambda.UpdateFunctionUrlConfigAsync(new UpdateFunctionUrlConfigRequest
        {
            FunctionName = "url-func",
            AuthType = FunctionUrlAuthType.AWS_IAM,
        });

        updated.AuthType.ShouldBe(FunctionUrlAuthType.AWS_IAM);

        // Delete
        await _lambda.DeleteFunctionUrlConfigAsync(new DeleteFunctionUrlConfigRequest
        {
            FunctionName = "url-func",
        });

        await Should.ThrowAsync<ResourceNotFoundException>(() =>
            _lambda.GetFunctionUrlConfigAsync(new GetFunctionUrlConfigRequest
            {
                FunctionName = "url-func",
            }));
    }

    // -- ESM CRUD --------------------------------------------------------------

    [Fact]
    public async Task EsmCrud()
    {
        await CreateTestFunction("esm-func");

        // Create
        var created = await _lambda.CreateEventSourceMappingAsync(new CreateEventSourceMappingRequest
        {
            FunctionName = "esm-func",
            EventSourceArn = "arn:aws:sqs:us-east-1:000000000000:my-queue",
            BatchSize = 5,
        });

        created.UUID.ShouldNotBeEmpty();
        created.BatchSize.ShouldBe(5);

        // Get
        var got = await _lambda.GetEventSourceMappingAsync(new GetEventSourceMappingRequest
        {
            UUID = created.UUID,
        });

        got.UUID.ShouldBe(created.UUID);

        // List
        var list = await _lambda.ListEventSourceMappingsAsync(new ListEventSourceMappingsRequest
        {
            FunctionName = "esm-func",
        });

        (list.EventSourceMappings.Count >= 1).ShouldBe(true);

        // Update
        var updated = await _lambda.UpdateEventSourceMappingAsync(new UpdateEventSourceMappingRequest
        {
            UUID = created.UUID,
            BatchSize = 20,
        });

        updated.BatchSize.ShouldBe(20);

        // Delete
        var deleted = await _lambda.DeleteEventSourceMappingAsync(new DeleteEventSourceMappingRequest
        {
            UUID = created.UUID,
        });

        deleted.UUID.ShouldNotBeNull();
    }

    // -- Unknown Path ----------------------------------------------------------

    [Fact]
    public async Task UnknownPathReturns404()
    {
        // Send a request to an unknown Lambda path that goes through the Lambda handler
        var response = await _fixture.HttpClient.GetAsync("/2015-03-31/functions/nonexistent-func-12345");

        response.StatusCode.ShouldBe(System.Net.HttpStatusCode.NotFound);
    }

    // -- Event Invoke Config ---------------------------------------------------

    [Fact]
    public async Task EventInvokeConfigCrud()
    {
        await CreateTestFunction("eic-func");

        // Put
        var put = await _lambda.PutFunctionEventInvokeConfigAsync(new PutFunctionEventInvokeConfigRequest
        {
            FunctionName = "eic-func",
            MaximumRetryAttempts = 1,
            MaximumEventAgeInSeconds = 3600,
        });

        put.MaximumRetryAttempts.ShouldBe(1);
        put.MaximumEventAgeInSeconds.ShouldBe(3600);

        // Get
        var get = await _lambda.GetFunctionEventInvokeConfigAsync(new GetFunctionEventInvokeConfigRequest
        {
            FunctionName = "eic-func",
        });

        get.MaximumRetryAttempts.ShouldBe(1);
        get.MaximumEventAgeInSeconds.ShouldBe(3600);

        // Delete
        await _lambda.DeleteFunctionEventInvokeConfigAsync(new DeleteFunctionEventInvokeConfigRequest
        {
            FunctionName = "eic-func",
        });

        // After delete, get should throw
        await Should.ThrowAsync<Amazon.Lambda.Model.ResourceNotFoundException>(() =>
            _lambda.GetFunctionEventInvokeConfigAsync(new GetFunctionEventInvokeConfigRequest
            {
                FunctionName = "eic-func",
            }));
    }

    // -- Provisioned Concurrency -----------------------------------------------

    [Fact]
    public async Task ProvisionedConcurrencyCrud()
    {
        await CreateTestFunction("pc-func");

        // Publish a version (provisioned concurrency needs a qualifier)
        var published = await _lambda.PublishVersionAsync(new PublishVersionRequest
        {
            FunctionName = "pc-func",
        });

        // Put
        var put = await _lambda.PutProvisionedConcurrencyConfigAsync(new PutProvisionedConcurrencyConfigRequest
        {
            FunctionName = "pc-func",
            Qualifier = published.Version,
            ProvisionedConcurrentExecutions = 5,
        });

        put.RequestedProvisionedConcurrentExecutions.ShouldBe(5);

        // Get
        var get = await _lambda.GetProvisionedConcurrencyConfigAsync(new GetProvisionedConcurrencyConfigRequest
        {
            FunctionName = "pc-func",
            Qualifier = published.Version,
        });

        get.RequestedProvisionedConcurrentExecutions.ShouldBe(5);
        get.Status?.Value.ShouldBe("READY");

        // Delete
        await _lambda.DeleteProvisionedConcurrencyConfigAsync(new DeleteProvisionedConcurrencyConfigRequest
        {
            FunctionName = "pc-func",
            Qualifier = published.Version,
        });

        // After delete, get should throw
        await Should.ThrowAsync<Amazon.Lambda.Model.ProvisionedConcurrencyConfigNotFoundException>(() =>
            _lambda.GetProvisionedConcurrencyConfigAsync(new GetProvisionedConcurrencyConfigRequest
            {
                FunctionName = "pc-func",
                Qualifier = published.Version,
            }));
    }

    // -- Function with Layer ---------------------------------------------------

    [Fact]
    public async Task FunctionWithLayer()
    {
        using var layerZip = MakeZip("python/mylib.py", "# layer code");
        var layerPublish = await _lambda.PublishLayerVersionAsync(new PublishLayerVersionRequest
        {
            LayerName = "func-layer",
            Content = new LayerVersionContentInput { ZipFile = layerZip },
        });

        using var funcZip = MakeZip("index.py", PythonCode);
        var result = await _lambda.CreateFunctionAsync(new CreateFunctionRequest
        {
            FunctionName = "func-with-layer",
            Runtime = Runtime.Python39,
            Role = LambdaRole,
            Handler = "index.handler",
            Code = new FunctionCode { ZipFile = funcZip },
            Layers = [layerPublish.LayerVersionArn],
        });

        result.FunctionName.ShouldBe("func-with-layer");
        result.Layers.ShouldNotBeNull();
        result.Layers.ShouldHaveSingleItem();
    }

    // -- PublishVersion Snapshot -----------------------------------------------

    [Fact]
    public async Task PublishVersionSnapshot()
    {
        await CreateTestFunction("snapshot-func");

        var published = await _lambda.PublishVersionAsync(new PublishVersionRequest
        {
            FunctionName = "snapshot-func",
        });

        // Update the $LATEST config
        await _lambda.UpdateFunctionConfigurationAsync(new UpdateFunctionConfigurationRequest
        {
            FunctionName = "snapshot-func",
            Description = "changed-after-publish",
        });

        // The published version should still have the original description
        var versionConfig = await _lambda.GetFunctionConfigurationAsync(new GetFunctionConfigurationRequest
        {
            FunctionName = "snapshot-func",
            Qualifier = published.Version,
        });

        versionConfig.Description.ShouldNotBe("changed-after-publish");
    }

    // -- Worker Pool Invocation Tests ------------------------------------------

    private static bool IsPythonAvailable()
    {
        try
        {
            var fileName = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "python" : "python3";
            using var process = Process.Start(new ProcessStartInfo
            {
                FileName = fileName,
                Arguments = "--version",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            });
            process?.WaitForExit(5000);
            return process?.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }

    private static bool IsNodeAvailable()
    {
        try
        {
            using var process = Process.Start(new ProcessStartInfo
            {
                FileName = "node",
                Arguments = "--version",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            });
            process?.WaitForExit(5000);
            return process?.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }

    private async Task<CreateFunctionResponse> CreatePythonFunction(string name, string code)
    {
        using var zip = MakeZip("index.py", code);
        return await _lambda.CreateFunctionAsync(new CreateFunctionRequest
        {
            FunctionName = name,
            Runtime = Runtime.Python39,
            Role = LambdaRole,
            Handler = "index.handler",
            Code = new FunctionCode { ZipFile = zip },
        });
    }

    private async Task<CreateFunctionResponse> CreatePythonFunctionWithEnv(string name, string code, Dictionary<string, string> envVars)
    {
        using var zip = MakeZip("index.py", code);
        return await _lambda.CreateFunctionAsync(new CreateFunctionRequest
        {
            FunctionName = name,
            Runtime = Runtime.Python39,
            Role = LambdaRole,
            Handler = "index.handler",
            Code = new FunctionCode { ZipFile = zip },
            Environment = new Amazon.Lambda.Model.Environment { Variables = envVars },
        });
    }

    private async Task<CreateFunctionResponse> CreateNodeFunction(string name, string code)
    {
        using var zip = MakeZip("index.js", code);
        return await _lambda.CreateFunctionAsync(new CreateFunctionRequest
        {
            FunctionName = name,
            Runtime = Runtime.Nodejs18X,
            Role = LambdaRole,
            Handler = "index.handler",
            Code = new FunctionCode { ZipFile = zip },
        });
    }

    [Fact]
    public async Task InvokePythonRequestResponse()
    {
        if (!IsPythonAvailable())
        {
            return;
        }

        const string code = """
            def handler(event, context):
                return {"statusCode": 200, "body": "hello " + event.get("name", "world")}
            """;

        await CreatePythonFunction("invoke-py-func", code);

        var result = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-py-func",
            Payload = """{"name": "Lambda"}""",
        });

        result.StatusCode.ShouldBe(200);
        result.FunctionError.ShouldBeNull();

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        doc.RootElement.GetProperty("statusCode").GetInt32().ShouldBe(200);
        doc.RootElement.GetProperty("body").GetString().ShouldBe("hello Lambda");
    }

    [Fact]
    public async Task InvokePythonReturnsPayload()
    {
        if (!IsPythonAvailable())
        {
            return;
        }

        const string code = """
            def handler(event, context):
                items = event.get("items", [])
                return {"count": len(items), "sum": sum(items)}
            """;

        await CreatePythonFunction("invoke-py-payload", code);

        var result = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-py-payload",
            Payload = """{"items": [1, 2, 3, 4, 5]}""",
        });

        result.StatusCode.ShouldBe(200);
        result.FunctionError.ShouldBeNull();

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        doc.RootElement.GetProperty("count").GetInt32().ShouldBe(5);
        doc.RootElement.GetProperty("sum").GetInt32().ShouldBe(15);
    }

    [Fact]
    public async Task InvokePythonWithEnvironmentVariables()
    {
        if (!IsPythonAvailable())
        {
            return;
        }

        const string code = """
            import os
            def handler(event, context):
                return {"myvar": os.environ.get("MY_VAR", "not-set"), "region": os.environ.get("MY_REGION", "not-set")}
            """;

        var envVars = new Dictionary<string, string>
        {
            ["MY_VAR"] = "hello-env",
            ["MY_REGION"] = "us-west-2",
        };

        await CreatePythonFunctionWithEnv("invoke-py-env", code, envVars);

        var result = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-py-env",
            Payload = "{}",
        });

        result.StatusCode.ShouldBe(200);
        result.FunctionError.ShouldBeNull();

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        doc.RootElement.GetProperty("myvar").GetString().ShouldBe("hello-env");
        doc.RootElement.GetProperty("region").GetString().ShouldBe("us-west-2");
    }

    [Fact]
    public async Task InvokePythonWarmStart()
    {
        if (!IsPythonAvailable())
        {
            return;
        }

        const string code = """
            import time
            _boot_time = time.time()
            def handler(event, context):
                return {"boot": _boot_time}
            """;

        await CreatePythonFunction("invoke-py-warm", code);

        // First invocation (cold start)
        var result1 = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-py-warm",
            Payload = "{}",
        });

        result1.StatusCode.ShouldBe(200);
        var payload1 = Encoding.UTF8.GetString(result1.Payload.ToArray());
        using var doc1 = JsonDocument.Parse(payload1);
        var bootTime1 = doc1.RootElement.GetProperty("boot").GetDouble();

        // Second invocation (warm start — same process)
        var result2 = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-py-warm",
            Payload = "{}",
        });

        result2.StatusCode.ShouldBe(200);
        var payload2 = Encoding.UTF8.GetString(result2.Payload.ToArray());
        using var doc2 = JsonDocument.Parse(payload2);
        var bootTime2 = doc2.RootElement.GetProperty("boot").GetDouble();

        // Boot time should be the same (module loaded once, process reused)
        bootTime2.ShouldBe(bootTime1);
    }

    [Fact]
    public async Task InvokeNodeJsRequestResponse()
    {
        if (!IsNodeAvailable())
        {
            return;
        }

        const string code = """
            exports.handler = async (event, context) => {
                return { statusCode: 200, body: JSON.stringify({ hello: event.name || 'world' }) };
            };
            """;

        await CreateNodeFunction("invoke-node-func", code);

        var result = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-node-func",
            Payload = """{"name": "Node"}""",
        });

        result.StatusCode.ShouldBe(200);
        result.FunctionError.ShouldBeNull();

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        doc.RootElement.GetProperty("statusCode").GetInt32().ShouldBe(200);

        var bodyStr = doc.RootElement.GetProperty("body").GetString()!;
        using var bodyDoc = JsonDocument.Parse(bodyStr);
        bodyDoc.RootElement.GetProperty("hello").GetString().ShouldBe("Node");
    }

    [Fact]
    public async Task InvokePythonErrorReturnsUnhandled()
    {
        if (!IsPythonAvailable())
        {
            return;
        }

        const string code = """
            def handler(event, context):
                raise ValueError("something went wrong")
            """;

        await CreatePythonFunction("invoke-py-error", code);

        var result = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-py-error",
            Payload = "{}",
        });

        result.StatusCode.ShouldBe(200);
        result.FunctionError.ShouldBe("Unhandled");

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        doc.RootElement.GetProperty("errorMessage").GetString()!.ShouldContain("something went wrong");
    }

    [Fact]
    public async Task UpdateCodeInvalidatesWorker()
    {
        if (!IsPythonAvailable())
        {
            return;
        }

        const string codeV1 = """
            def handler(event, context):
                return {"version": 1}
            """;

        await CreatePythonFunction("invoke-py-update", codeV1);

        // First invocation returns version 1
        var result1 = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-py-update",
            Payload = "{}",
        });

        result1.StatusCode.ShouldBe(200);
        var payload1 = Encoding.UTF8.GetString(result1.Payload.ToArray());
        using var doc1 = JsonDocument.Parse(payload1);
        doc1.RootElement.GetProperty("version").GetInt32().ShouldBe(1);

        // Update the code
        const string codeV2 = """
            def handler(event, context):
                return {"version": 2}
            """;
        using var newZip = MakeZip("index.py", codeV2);
        await _lambda.UpdateFunctionCodeAsync(new UpdateFunctionCodeRequest
        {
            FunctionName = "invoke-py-update",
            ZipFile = newZip,
        });

        // Second invocation should use new code
        var result2 = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-py-update",
            Payload = "{}",
        });

        result2.StatusCode.ShouldBe(200);
        var payload2 = Encoding.UTF8.GetString(result2.Payload.ToArray());
        using var doc2 = JsonDocument.Parse(payload2);
        doc2.RootElement.GetProperty("version").GetInt32().ShouldBe(2);
    }

    [Fact]
    public async Task InvokeEventTypeStillReturns202WithWorkerPool()
    {
        if (!IsPythonAvailable())
        {
            return;
        }

        const string code = """
            def handler(event, context):
                return {"statusCode": 200}
            """;

        await CreatePythonFunction("invoke-py-event", code);

        var result = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-py-event",
            InvocationType = InvocationType.Event,
        });

        result.StatusCode.ShouldBe(202);
    }

    [Fact]
    public async Task ResetTerminatesWorkersAndAllowsNewColdStart()
    {
        if (!IsPythonAvailable())
        {
            return;
        }

        const string code = """
            import time
            _boot_time = time.time()
            def handler(event, context):
                return {"boot": _boot_time}
            """;

        await CreatePythonFunction("invoke-py-reset", code);

        // First invocation — cold start
        var result1 = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-py-reset",
            Payload = "{}",
        });

        result1.StatusCode.ShouldBe(200);
        var payload1 = Encoding.UTF8.GetString(result1.Payload.ToArray());
        using var doc1 = JsonDocument.Parse(payload1);
        var bootTime1 = doc1.RootElement.GetProperty("boot").GetDouble();

        // Reset kills all workers
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);

        // Re-create the function (reset cleared the store)
        await CreatePythonFunction("invoke-py-reset", code);

        // Small delay to ensure different boot time
        await Task.Delay(50);

        // Second invocation — new cold start
        var result2 = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-py-reset",
            Payload = "{}",
        });

        result2.StatusCode.ShouldBe(200);
        var payload2 = Encoding.UTF8.GetString(result2.Payload.ToArray());
        using var doc2 = JsonDocument.Parse(payload2);
        var bootTime2 = doc2.RootElement.GetProperty("boot").GetDouble();

        // Boot time should be different (new worker process)
        bootTime2.ShouldNotBe(bootTime1);
    }

    [Fact]
    public async Task InvokePythonWithContext()
    {
        if (!IsPythonAvailable())
        {
            return;
        }

        const string code = """
            def handler(event, context):
                return {
                    "function_name": context.function_name,
                    "memory_limit": context.memory_limit_in_mb,
                    "has_request_id": len(context.aws_request_id) > 0,
                }
            """;

        await CreatePythonFunction("invoke-py-context", code);

        var result = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-py-context",
            Payload = "{}",
        });

        result.StatusCode.ShouldBe(200);
        result.FunctionError.ShouldBeNull();

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        doc.RootElement.GetProperty("function_name").GetString().ShouldBe("invoke-py-context");
        doc.RootElement.GetProperty("memory_limit").GetInt32().ShouldBe(128);
        doc.RootElement.GetProperty("has_request_id").GetBoolean().ShouldBe(true);
    }

    [Fact]
    public async Task InvokeNodeJsCallbackStyle()
    {
        if (!IsNodeAvailable())
        {
            return;
        }

        const string code = """
            exports.handler = (event, context, callback) => {
                callback(null, { statusCode: 200, body: 'callback-' + (event.name || 'world') });
            };
            """;

        await CreateNodeFunction("invoke-node-cb", code);

        var result = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-node-cb",
            Payload = """{"name": "CB"}""",
        });

        result.StatusCode.ShouldBe(200);
        result.FunctionError.ShouldBeNull();

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        doc.RootElement.GetProperty("statusCode").GetInt32().ShouldBe(200);
        doc.RootElement.GetProperty("body").GetString().ShouldBe("callback-CB");
    }

    // -- ESM Poller Integration Tests ------------------------------------------

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

    [Fact]
    public async Task EsmSqsConsumesMessage()
    {
        if (!IsPythonAvailable())
        {
            return;
        }

        using var sqsClient = CreateSqsClient(_fixture);

        // Create SQS queue
        var createQueue = await sqsClient.CreateQueueAsync(new Amazon.SQS.Model.CreateQueueRequest
        {
            QueueName = "esm-source-queue",
        });
        var queueUrl = createQueue.QueueUrl;

        // Get queue ARN
        var attrs = await sqsClient.GetQueueAttributesAsync(new Amazon.SQS.Model.GetQueueAttributesRequest
        {
            QueueUrl = queueUrl,
            AttributeNames = ["QueueArn"],
        });
        var queueArn = attrs.Attributes["QueueArn"];

        // Create Lambda function that simply returns the number of records consumed
        const string code = "def handler(event, context):\n    return {\"consumed\": len(event.get(\"Records\", []))}\n";
        await CreatePythonFunction("esm-handler", code);

        // Create ESM linking the SQS queue to the Lambda function
        await _lambda.CreateEventSourceMappingAsync(new CreateEventSourceMappingRequest
        {
            FunctionName = "esm-handler",
            EventSourceArn = queueArn,
            BatchSize = 1,
            Enabled = true,
        });

        // Send a message to the source queue
        await sqsClient.SendMessageAsync(new Amazon.SQS.Model.SendMessageRequest
        {
            QueueUrl = queueUrl,
            MessageBody = "{\"test\": true}",
        });

        // Wait for message to be consumed (poll for up to 15 seconds)
        var consumed = false;
        for (var i = 0; i < 15; i++)
        {
            await Task.Delay(1000);
            var receive = await sqsClient.ReceiveMessageAsync(new Amazon.SQS.Model.ReceiveMessageRequest
            {
                QueueUrl = queueUrl,
                MaxNumberOfMessages = 1,
                WaitTimeSeconds = 0,
            });
            if (receive.Messages is null || receive.Messages.Count == 0)
            {
                consumed = true;
                break;
            }
        }

        consumed.ShouldBe(true, "Message should have been consumed by Lambda ESM");
    }

    [Fact]
    public async Task EsmCrudWorksAfterPollerStarts()
    {
        if (!IsPythonAvailable())
        {
            return;
        }

        using var sqsClient = CreateSqsClient(_fixture);

        // Create SQS queue
        var createQueue = await sqsClient.CreateQueueAsync(new Amazon.SQS.Model.CreateQueueRequest
        {
            QueueName = "esm-crud-queue",
        });
        var queueUrl = createQueue.QueueUrl;

        // Get queue ARN
        var attrs = await sqsClient.GetQueueAttributesAsync(new Amazon.SQS.Model.GetQueueAttributesRequest
        {
            QueueUrl = queueUrl,
            AttributeNames = ["QueueArn"],
        });
        var queueArn = attrs.Attributes["QueueArn"];

        // Create Lambda function
        const string code = "def handler(event, context):\n    return {\"ok\": True}\n";
        await CreatePythonFunction("esm-crud-func", code);

        // Create ESM (this starts the poller)
        var created = await _lambda.CreateEventSourceMappingAsync(new CreateEventSourceMappingRequest
        {
            FunctionName = "esm-crud-func",
            EventSourceArn = queueArn,
            BatchSize = 5,
            Enabled = true,
        });

        created.UUID.ShouldNotBeNull();
        created.State.ShouldBe("Enabled");
        created.BatchSize.ShouldBe(5);

        // Get ESM
        var fetched = await _lambda.GetEventSourceMappingAsync(new GetEventSourceMappingRequest
        {
            UUID = created.UUID,
        });

        fetched.UUID.ShouldBe(created.UUID);

        // Update ESM (disable it)
        var updated = await _lambda.UpdateEventSourceMappingAsync(new UpdateEventSourceMappingRequest
        {
            UUID = created.UUID,
            Enabled = false,
        });

        updated.State.ShouldBe("Disabled");

        // List ESMs
        var listed = await _lambda.ListEventSourceMappingsAsync(new ListEventSourceMappingsRequest
        {
            FunctionName = "esm-crud-func",
        });

        listed.EventSourceMappings.ShouldHaveSingleItem();

        // Delete ESM
        var deleted = await _lambda.DeleteEventSourceMappingAsync(new DeleteEventSourceMappingRequest
        {
            UUID = created.UUID,
        });

        deleted.UUID.ShouldNotBeNull();

        // Verify deletion
        var listedAfterDelete = await _lambda.ListEventSourceMappingsAsync(new ListEventSourceMappingsRequest
        {
            FunctionName = "esm-crud-func",
        });

        listedAfterDelete.EventSourceMappings.ShouldBeEmpty();
    }

    [Fact]
    public async Task EsmSqsConsumesMultipleMessages()
    {
        if (!IsPythonAvailable())
        {
            return;
        }

        using var sqsClient = CreateSqsClient(_fixture);

        // Create SQS queue
        var createQueue = await sqsClient.CreateQueueAsync(new Amazon.SQS.Model.CreateQueueRequest
        {
            QueueName = "esm-batch-queue",
        });
        var queueUrl = createQueue.QueueUrl;

        // Get queue ARN
        var attrs = await sqsClient.GetQueueAttributesAsync(new Amazon.SQS.Model.GetQueueAttributesRequest
        {
            QueueUrl = queueUrl,
            AttributeNames = ["QueueArn"],
        });
        var queueArn = attrs.Attributes["QueueArn"];

        // Create Lambda function
        const string code = "def handler(event, context):\n    return {\"consumed\": len(event.get(\"Records\", []))}\n";
        await CreatePythonFunction("esm-batch-handler", code);

        // Create ESM with batch size of 5
        await _lambda.CreateEventSourceMappingAsync(new CreateEventSourceMappingRequest
        {
            FunctionName = "esm-batch-handler",
            EventSourceArn = queueArn,
            BatchSize = 5,
            Enabled = true,
        });

        // Send 3 messages
        for (var i = 0; i < 3; i++)
        {
            await sqsClient.SendMessageAsync(new Amazon.SQS.Model.SendMessageRequest
            {
                QueueUrl = queueUrl,
                MessageBody = $"{{\"index\": {i}}}",
            });
        }

        // Wait for all messages to be consumed
        var consumed = false;
        for (var i = 0; i < 15; i++)
        {
            await Task.Delay(1000);
            var receive = await sqsClient.ReceiveMessageAsync(new Amazon.SQS.Model.ReceiveMessageRequest
            {
                QueueUrl = queueUrl,
                MaxNumberOfMessages = 10,
                WaitTimeSeconds = 0,
            });
            if (receive.Messages is null || receive.Messages.Count == 0)
            {
                consumed = true;
                break;
            }
        }

        consumed.ShouldBe(true, "All messages should have been consumed by Lambda ESM");
    }

    // -- .NET Lambda Invocation Tests -----------------------------------------

    [Fact]
    public async Task InvokeDotnetRequestResponse()
    {
if (!IsDotnetBootstrapAvailable() || FindSimpleHandlerDir() is null)
        {
            TestContext.Current.CancelCurrentTest();
            return;
        }

        await CreateDotnetFunction("invoke-dotnet-func", "GreetHandler");

        var result = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-dotnet-func",
            Payload = """{"name": "DotNet"}""",
        });

        result.StatusCode.ShouldBe(200);
        result.FunctionError.ShouldBeNull();

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        doc.RootElement.GetProperty("greeting").GetString().ShouldBe("hello DotNet");
    }

    [Fact]
    public async Task InvokeDotnetReturnsPayload()
    {
if (!IsDotnetBootstrapAvailable() || FindSimpleHandlerDir() is null)
        {
            TestContext.Current.CancelCurrentTest();
            return;
        }

        await CreateDotnetFunction("invoke-dotnet-payload", "SumHandler");

        var result = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-dotnet-payload",
            Payload = """{"items": [1, 2, 3, 4, 5]}""",
        });

        result.StatusCode.ShouldBe(200);
        result.FunctionError.ShouldBeNull();

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        doc.RootElement.GetProperty("count").GetInt32().ShouldBe(5);
        doc.RootElement.GetProperty("sum").GetInt32().ShouldBe(15);
    }

    [Fact]
    public async Task InvokeDotnetWithEnvironmentVariables()
    {
if (!IsDotnetBootstrapAvailable() || FindSimpleHandlerDir() is null)
        {
            TestContext.Current.CancelCurrentTest();
            return;
        }

        var envVars = new Dictionary<string, string>
        {
            ["MY_VAR"] = "hello-dotnet",
            ["MY_REGION"] = "eu-west-1",
        };

        await CreateDotnetFunction("invoke-dotnet-env", "EnvHandler", envVars);

        var result = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-dotnet-env",
            Payload = "{}",
        });

        result.StatusCode.ShouldBe(200);
        result.FunctionError.ShouldBeNull();

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        doc.RootElement.GetProperty("myvar").GetString().ShouldBe("hello-dotnet");
        doc.RootElement.GetProperty("region").GetString().ShouldBe("eu-west-1");
    }

    [Fact]
    public async Task InvokeDotnetWarmStart()
    {
if (!IsDotnetBootstrapAvailable() || FindSimpleHandlerDir() is null)
        {
            TestContext.Current.CancelCurrentTest();
            return;
        }

        await CreateDotnetFunction("invoke-dotnet-warm", "EchoHandler");

        // First invocation (cold start)
        var result1 = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-dotnet-warm",
            Payload = """{"seq": 1}""",
        });

        result1.StatusCode.ShouldBe(200);
        result1.FunctionError.ShouldBeNull();

        // Second invocation — should be warm (same process)
        var result2 = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-dotnet-warm",
            Payload = """{"seq": 2}""",
        });

        result2.StatusCode.ShouldBe(200);
        result2.FunctionError.ShouldBeNull();

        var payload2 = Encoding.UTF8.GetString(result2.Payload.ToArray());
        using var doc2 = JsonDocument.Parse(payload2);
        doc2.RootElement.GetProperty("seq").GetInt32().ShouldBe(2);
    }

    [Fact]
    public async Task InvokeDotnetErrorReturnsUnhandled()
    {
if (!IsDotnetBootstrapAvailable() || FindSimpleHandlerDir() is null)
        {
            TestContext.Current.CancelCurrentTest();
            return;
        }

        await CreateDotnetFunction("invoke-dotnet-error", "ErrorHandler");

        var result = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-dotnet-error",
            Payload = "{}",
        });

        result.StatusCode.ShouldBe(200);
        result.FunctionError.ShouldBe("Unhandled");

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        doc.RootElement.GetProperty("errorMessage").GetString()!.ShouldContain("something went wrong");
    }

    [Fact]
    public async Task InvokeDotnetWithContext()
    {
if (!IsDotnetBootstrapAvailable() || FindSimpleHandlerDir() is null)
        {
            TestContext.Current.CancelCurrentTest();
            return;
        }

        await CreateDotnetFunction("invoke-dotnet-context", "ContextHandler");

        var result = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-dotnet-context",
            Payload = "{}",
        });

        result.StatusCode.ShouldBe(200);
        result.FunctionError.ShouldBeNull();

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        doc.RootElement.GetProperty("function_name").GetString().ShouldBe("invoke-dotnet-context");
        doc.RootElement.GetProperty("memory_limit").GetInt32().ShouldBe(128);
        doc.RootElement.GetProperty("has_request_id").GetBoolean().ShouldBe(true);
    }

    [Fact]
    public async Task UpdateCodeInvalidatesDotnetWorker()
    {
if (!IsDotnetBootstrapAvailable() || FindSimpleHandlerDir() is null)
        {
            TestContext.Current.CancelCurrentTest();
            return;
        }

        // Create function with EchoHandler
        await CreateDotnetFunction("invoke-dotnet-update", "EchoHandler");

        // First invocation — echo returns input
        var result1 = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-dotnet-update",
            Payload = """{"version": 1}""",
        });

        result1.StatusCode.ShouldBe(200);
        result1.FunctionError.ShouldBeNull();
        var payload1 = Encoding.UTF8.GetString(result1.Payload.ToArray());
        using var doc1 = JsonDocument.Parse(payload1);
        doc1.RootElement.GetProperty("version").GetInt32().ShouldBe(1);

        // Update the function code (same DLL, different handler)
        var handlerDir = FindSimpleHandlerDir()!;
        using var newZip = MakeDotnetZip(handlerDir);
        await _lambda.UpdateFunctionCodeAsync(new UpdateFunctionCodeRequest
        {
            FunctionName = "invoke-dotnet-update",
            ZipFile = newZip,
        });

        // Update config to use GreetHandler
        await _lambda.UpdateFunctionConfigurationAsync(new UpdateFunctionConfigurationRequest
        {
            FunctionName = "invoke-dotnet-update",
            Handler = "SimpleHandler::SimpleHandler.Function::GreetHandler",
        });

        // Second invocation should use new handler (greet)
        var result2 = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-dotnet-update",
            Payload = """{"name": "Updated"}""",
        });

        result2.StatusCode.ShouldBe(200);
        result2.FunctionError.ShouldBeNull();
        var payload2 = Encoding.UTF8.GetString(result2.Payload.ToArray());
        using var doc2 = JsonDocument.Parse(payload2);
        doc2.RootElement.GetProperty("greeting").GetString().ShouldBe("hello Updated");
    }

    // -- .NET Lambda helper methods -------------------------------------------

    /// <summary>
    /// Checks whether the MicroStack.LambdaBootstrap is built and findable via the dev-fallback
    /// path resolution logic (walking up from the test assembly to src/).
    /// </summary>
    private static bool IsDotnetBootstrapAvailable()
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 10; i++)
        {
            if (dir is null) break;

            var bootstrapBinDir = Path.Combine(dir, "src", "MicroStack.LambdaBootstrap", "bin");
            if (Directory.Exists(bootstrapBinDir))
            {
                foreach (var configDir in Directory.GetDirectories(bootstrapBinDir))
                {
                    var candidate = Path.Combine(configDir, "net10.0", "MicroStack.LambdaBootstrap.dll");
                    if (File.Exists(candidate)) return true;
                }
            }

            dir = Path.GetDirectoryName(dir);
        }

        // Also check MICROSTACK_LAMBDA_BOOTSTRAP_PATH env var
        var envPath = System.Environment.GetEnvironmentVariable("MICROSTACK_LAMBDA_BOOTSTRAP_PATH");
        return !string.IsNullOrEmpty(envPath) && File.Exists(envPath);
    }

    /// <summary>
    /// Locates the published SimpleHandler DLL directory (build output, not publish).
    /// Returns null if not found.
    /// </summary>
    private static string? FindSimpleHandlerDir()
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 10; i++)
        {
            if (dir is null) break;

            var handlerBinDir = Path.Combine(dir, "tests", "TestLambdaFunctions", "SimpleHandler", "bin");
            if (Directory.Exists(handlerBinDir))
            {
                foreach (var configDir in Directory.GetDirectories(handlerBinDir))
                {
                    var tfmDir = Path.Combine(configDir, "net10.0");
                    var candidate = Path.Combine(tfmDir, "SimpleHandler.dll");
                    if (File.Exists(candidate)) return tfmDir;
                }
            }

            dir = Path.GetDirectoryName(dir);
        }

        return null;
    }

    /// <summary>
    /// Creates an in-memory ZIP containing all files from the given directory.
    /// </summary>
    private static MemoryStream MakeDotnetZip(string publishDir)
    {
        var ms = new MemoryStream();
        using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, leaveOpen: true))
        {
            foreach (var filePath in Directory.GetFiles(publishDir))
            {
                var entryName = Path.GetFileName(filePath);
                var entry = archive.CreateEntry(entryName);
                using var entryStream = entry.Open();
                using var fileStream = File.OpenRead(filePath);
                fileStream.CopyTo(entryStream);
            }
        }

        ms.Position = 0;
        return ms;
    }

    private async Task<CreateFunctionResponse> CreateDotnetFunction(string name, string handlerMethod, Dictionary<string, string>? envVars = null)
    {
        var handlerDir = FindSimpleHandlerDir()!;
        using var zip = MakeDotnetZip(handlerDir);

        var request = new CreateFunctionRequest
        {
            FunctionName = name,
            Runtime = new Amazon.Lambda.Runtime("dotnet10"),
            Role = LambdaRole,
            Handler = $"SimpleHandler::SimpleHandler.Function::{handlerMethod}",
            Code = new FunctionCode { ZipFile = zip },
        };

        if (envVars is not null)
        {
            request.Environment = new Amazon.Lambda.Model.Environment { Variables = envVars };
        }

        return await _lambda.CreateFunctionAsync(request);
    }
}
