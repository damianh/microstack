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

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _lambda.Dispose();
        return Task.CompletedTask;
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

        Assert.Equal("test-func", result.FunctionName);
        Assert.Contains("test-func", result.FunctionArn);
        Assert.Equal("python3.9", result.Runtime?.Value);
        Assert.Equal("index.handler", result.Handler);
        Assert.True(result.CodeSize > 0);
        Assert.NotEmpty(result.CodeSha256);
        Assert.Equal("$LATEST", result.Version);
        Assert.Equal(State.Active, result.State);
    }

    [Fact]
    public async Task CreateFunctionDuplicateNameFails()
    {
        await CreateTestFunction("dup-func");

        var ex = await Assert.ThrowsAsync<ResourceConflictException>(() =>
            CreateTestFunction("dup-func"));

        Assert.Contains("already exist", ex.Message);
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

        Assert.Equal("get-func", result.Configuration.FunctionName);
        Assert.NotNull(result.Code);
        Assert.NotNull(result.Tags);
    }

    [Fact]
    public async Task GetFunctionNotFound()
    {
        await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
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

        Assert.True(result.Functions.Count >= 2);
        Assert.Contains(result.Functions, f => f.FunctionName == "list-func-a");
        Assert.Contains(result.Functions, f => f.FunctionName == "list-func-b");
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

        await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
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

        Assert.NotEqual(originalCodeSize, result.CodeSize);
        Assert.NotEqual(originalSha, result.CodeSha256);
        Assert.Equal("update-code-func", result.FunctionName);
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

        Assert.Equal("new_handler.handler", result.Handler);
        Assert.Equal("Updated description", result.Description);
        Assert.Equal(30, result.Timeout);
        Assert.Equal(256, result.MemorySize);
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

        Assert.True(tags.Tags.Count >= 2);
        Assert.Equal("prod", tags.Tags["env"]);
        Assert.Equal("platform", tags.Tags["team"]);

        await _lambda.UntagResourceAsync(new UntagResourceRequest
        {
            Resource = create.FunctionArn,
            TagKeys = ["team"],
        });

        var tags2 = await _lambda.ListTagsAsync(new ListTagsRequest
        {
            Resource = create.FunctionArn,
        });

        Assert.DoesNotContain("team", tags2.Tags.Keys);
        Assert.Equal("prod", tags2.Tags["env"]);
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

        Assert.NotNull(result.Statement);
        Assert.Contains("stmt1", result.Statement);
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

        Assert.NotNull(policy.Policy);
        Assert.Contains("stmt-crud", policy.Policy);

        await _lambda.RemovePermissionAsync(new RemovePermissionRequest
        {
            FunctionName = "perm-crud-func",
            StatementId = "stmt-crud",
        });

        // After removal, there should be no statements → ResourceNotFound
        await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
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

        Assert.Single(result.Versions); // Only $LATEST
        Assert.Equal("$LATEST", result.Versions[0].Version);
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

        Assert.Equal("1", published.Version);
        Assert.Contains("publish-func", published.FunctionArn);

        var versions = await _lambda.ListVersionsByFunctionAsync(new ListVersionsByFunctionRequest
        {
            FunctionName = "publish-func",
        });

        Assert.Equal(2, versions.Versions.Count); // $LATEST + version 1
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

        Assert.Equal("1", result.Version);
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

        Assert.Equal(202, result.StatusCode);
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

        Assert.Equal(204, result.StatusCode);
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

        Assert.Equal("prod", created.Name);
        Assert.Equal(published.Version, created.FunctionVersion);

        // Get alias
        var alias = await _lambda.GetAliasAsync(new GetAliasRequest
        {
            FunctionName = "alias-func",
            Name = "prod",
        });

        Assert.Equal("prod", alias.Name);
        Assert.Equal(published.Version, alias.FunctionVersion);

        // Update alias
        var updated = await _lambda.UpdateAliasAsync(new UpdateAliasRequest
        {
            FunctionName = "alias-func",
            Name = "prod",
            Description = "Updated prod alias",
        });

        Assert.Equal("Updated prod alias", updated.Description);

        // List aliases
        var list = await _lambda.ListAliasesAsync(new ListAliasesRequest
        {
            FunctionName = "alias-func",
        });

        Assert.Single(list.Aliases);
        Assert.Equal("prod", list.Aliases[0].Name);

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

        Assert.Empty(listAfter.Aliases);
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

        Assert.Equal(10, put.ReservedConcurrentExecutions);

        // Get concurrency
        var get = await _lambda.GetFunctionConcurrencyAsync(new GetFunctionConcurrencyRequest
        {
            FunctionName = "concurrency-func",
        });

        Assert.Equal(10, get.ReservedConcurrentExecutions);

        // Delete concurrency
        await _lambda.DeleteFunctionConcurrencyAsync(new DeleteFunctionConcurrencyRequest
        {
            FunctionName = "concurrency-func",
        });

        var getAfter = await _lambda.GetFunctionConcurrencyAsync(new GetFunctionConcurrencyRequest
        {
            FunctionName = "concurrency-func",
        });

        Assert.Equal(0, getAfter.ReservedConcurrentExecutions);
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

        Assert.Equal(2, page1.Functions.Count);
        Assert.NotNull(page1.NextMarker);

        var page2 = await _lambda.ListFunctionsAsync(new ListFunctionsRequest
        {
            MaxItems = 2,
            Marker = page1.NextMarker,
        });

        Assert.Equal(2, page2.Functions.Count);

        // Pages should have different functions
        Assert.DoesNotContain(page1.Functions[0].FunctionName, page2.Functions.ConvertAll(f => f.FunctionName));
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

        Assert.Equal(1, result.Version);
        Assert.Contains("test-layer", result.LayerArn);
        Assert.Contains("test-layer", result.LayerVersionArn);
        Assert.Equal("A test layer", result.Description);
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

        Assert.Equal(publish.Version, result.Version);
        Assert.NotNull(result.Content);
        Assert.True(result.Content.CodeSize > 0);
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

        Assert.Equal(2, result.LayerVersions.Count);
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

        Assert.True(result.Layers.Count >= 1);
        Assert.Contains(result.Layers, l => l.LayerName == "listed-layer");
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

        Assert.Empty(versions.LayerVersions);
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

        Assert.NotEmpty(created.FunctionUrl);
        Assert.Equal(FunctionUrlAuthType.NONE, created.AuthType);
        Assert.Contains("url-func", created.FunctionArn);

        // Get
        var got = await _lambda.GetFunctionUrlConfigAsync(new GetFunctionUrlConfigRequest
        {
            FunctionName = "url-func",
        });

        Assert.Equal(created.FunctionUrl, got.FunctionUrl);

        // Update
        var updated = await _lambda.UpdateFunctionUrlConfigAsync(new UpdateFunctionUrlConfigRequest
        {
            FunctionName = "url-func",
            AuthType = FunctionUrlAuthType.AWS_IAM,
        });

        Assert.Equal(FunctionUrlAuthType.AWS_IAM, updated.AuthType);

        // Delete
        await _lambda.DeleteFunctionUrlConfigAsync(new DeleteFunctionUrlConfigRequest
        {
            FunctionName = "url-func",
        });

        await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
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

        Assert.NotEmpty(created.UUID);
        Assert.Equal(5, created.BatchSize);

        // Get
        var got = await _lambda.GetEventSourceMappingAsync(new GetEventSourceMappingRequest
        {
            UUID = created.UUID,
        });

        Assert.Equal(created.UUID, got.UUID);

        // List
        var list = await _lambda.ListEventSourceMappingsAsync(new ListEventSourceMappingsRequest
        {
            FunctionName = "esm-func",
        });

        Assert.True(list.EventSourceMappings.Count >= 1);

        // Update
        var updated = await _lambda.UpdateEventSourceMappingAsync(new UpdateEventSourceMappingRequest
        {
            UUID = created.UUID,
            BatchSize = 20,
        });

        Assert.Equal(20, updated.BatchSize);

        // Delete
        var deleted = await _lambda.DeleteEventSourceMappingAsync(new DeleteEventSourceMappingRequest
        {
            UUID = created.UUID,
        });

        Assert.NotNull(deleted.UUID);
    }

    // -- Unknown Path ----------------------------------------------------------

    [Fact]
    public async Task UnknownPathReturns404()
    {
        // Send a request to an unknown Lambda path that goes through the Lambda handler
        var response = await _fixture.HttpClient.GetAsync("/2015-03-31/functions/nonexistent-func-12345");

        Assert.Equal(System.Net.HttpStatusCode.NotFound, response.StatusCode);
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

        Assert.Equal(1, put.MaximumRetryAttempts);
        Assert.Equal(3600, put.MaximumEventAgeInSeconds);

        // Get
        var get = await _lambda.GetFunctionEventInvokeConfigAsync(new GetFunctionEventInvokeConfigRequest
        {
            FunctionName = "eic-func",
        });

        Assert.Equal(1, get.MaximumRetryAttempts);
        Assert.Equal(3600, get.MaximumEventAgeInSeconds);

        // Delete
        await _lambda.DeleteFunctionEventInvokeConfigAsync(new DeleteFunctionEventInvokeConfigRequest
        {
            FunctionName = "eic-func",
        });

        // After delete, get should throw
        await Assert.ThrowsAsync<Amazon.Lambda.Model.ResourceNotFoundException>(() =>
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

        Assert.Equal(5, put.RequestedProvisionedConcurrentExecutions);

        // Get
        var get = await _lambda.GetProvisionedConcurrencyConfigAsync(new GetProvisionedConcurrencyConfigRequest
        {
            FunctionName = "pc-func",
            Qualifier = published.Version,
        });

        Assert.Equal(5, get.RequestedProvisionedConcurrentExecutions);
        Assert.Equal("READY", get.Status?.Value);

        // Delete
        await _lambda.DeleteProvisionedConcurrencyConfigAsync(new DeleteProvisionedConcurrencyConfigRequest
        {
            FunctionName = "pc-func",
            Qualifier = published.Version,
        });

        // After delete, get should throw
        await Assert.ThrowsAsync<Amazon.Lambda.Model.ProvisionedConcurrencyConfigNotFoundException>(() =>
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

        Assert.Equal("func-with-layer", result.FunctionName);
        Assert.NotNull(result.Layers);
        Assert.Single(result.Layers);
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

        Assert.NotEqual("changed-after-publish", versionConfig.Description);
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

        Assert.Equal(200, result.StatusCode);
        Assert.Null(result.FunctionError);

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        Assert.Equal(200, doc.RootElement.GetProperty("statusCode").GetInt32());
        Assert.Equal("hello Lambda", doc.RootElement.GetProperty("body").GetString());
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

        Assert.Equal(200, result.StatusCode);
        Assert.Null(result.FunctionError);

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        Assert.Equal(5, doc.RootElement.GetProperty("count").GetInt32());
        Assert.Equal(15, doc.RootElement.GetProperty("sum").GetInt32());
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

        Assert.Equal(200, result.StatusCode);
        Assert.Null(result.FunctionError);

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        Assert.Equal("hello-env", doc.RootElement.GetProperty("myvar").GetString());
        Assert.Equal("us-west-2", doc.RootElement.GetProperty("region").GetString());
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

        Assert.Equal(200, result1.StatusCode);
        var payload1 = Encoding.UTF8.GetString(result1.Payload.ToArray());
        using var doc1 = JsonDocument.Parse(payload1);
        var bootTime1 = doc1.RootElement.GetProperty("boot").GetDouble();

        // Second invocation (warm start — same process)
        var result2 = await _lambda.InvokeAsync(new InvokeRequest
        {
            FunctionName = "invoke-py-warm",
            Payload = "{}",
        });

        Assert.Equal(200, result2.StatusCode);
        var payload2 = Encoding.UTF8.GetString(result2.Payload.ToArray());
        using var doc2 = JsonDocument.Parse(payload2);
        var bootTime2 = doc2.RootElement.GetProperty("boot").GetDouble();

        // Boot time should be the same (module loaded once, process reused)
        Assert.Equal(bootTime1, bootTime2);
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

        Assert.Equal(200, result.StatusCode);
        Assert.Null(result.FunctionError);

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        Assert.Equal(200, doc.RootElement.GetProperty("statusCode").GetInt32());

        var bodyStr = doc.RootElement.GetProperty("body").GetString()!;
        using var bodyDoc = JsonDocument.Parse(bodyStr);
        Assert.Equal("Node", bodyDoc.RootElement.GetProperty("hello").GetString());
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

        Assert.Equal(200, result.StatusCode);
        Assert.Equal("Unhandled", result.FunctionError);

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        Assert.Contains("something went wrong", doc.RootElement.GetProperty("errorMessage").GetString());
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

        Assert.Equal(200, result1.StatusCode);
        var payload1 = Encoding.UTF8.GetString(result1.Payload.ToArray());
        using var doc1 = JsonDocument.Parse(payload1);
        Assert.Equal(1, doc1.RootElement.GetProperty("version").GetInt32());

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

        Assert.Equal(200, result2.StatusCode);
        var payload2 = Encoding.UTF8.GetString(result2.Payload.ToArray());
        using var doc2 = JsonDocument.Parse(payload2);
        Assert.Equal(2, doc2.RootElement.GetProperty("version").GetInt32());
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

        Assert.Equal(202, result.StatusCode);
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

        Assert.Equal(200, result1.StatusCode);
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

        Assert.Equal(200, result2.StatusCode);
        var payload2 = Encoding.UTF8.GetString(result2.Payload.ToArray());
        using var doc2 = JsonDocument.Parse(payload2);
        var bootTime2 = doc2.RootElement.GetProperty("boot").GetDouble();

        // Boot time should be different (new worker process)
        Assert.NotEqual(bootTime1, bootTime2);
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

        Assert.Equal(200, result.StatusCode);
        Assert.Null(result.FunctionError);

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        Assert.Equal("invoke-py-context", doc.RootElement.GetProperty("function_name").GetString());
        Assert.Equal(128, doc.RootElement.GetProperty("memory_limit").GetInt32());
        Assert.True(doc.RootElement.GetProperty("has_request_id").GetBoolean());
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

        Assert.Equal(200, result.StatusCode);
        Assert.Null(result.FunctionError);

        var payload = Encoding.UTF8.GetString(result.Payload.ToArray());
        using var doc = JsonDocument.Parse(payload);
        Assert.Equal(200, doc.RootElement.GetProperty("statusCode").GetInt32());
        Assert.Equal("callback-CB", doc.RootElement.GetProperty("body").GetString());
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

        Assert.True(consumed, "Message should have been consumed by Lambda ESM");
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

        Assert.NotNull(created.UUID);
        Assert.Equal("Enabled", created.State);
        Assert.Equal(5, created.BatchSize);

        // Get ESM
        var fetched = await _lambda.GetEventSourceMappingAsync(new GetEventSourceMappingRequest
        {
            UUID = created.UUID,
        });

        Assert.Equal(created.UUID, fetched.UUID);

        // Update ESM (disable it)
        var updated = await _lambda.UpdateEventSourceMappingAsync(new UpdateEventSourceMappingRequest
        {
            UUID = created.UUID,
            Enabled = false,
        });

        Assert.Equal("Disabled", updated.State);

        // List ESMs
        var listed = await _lambda.ListEventSourceMappingsAsync(new ListEventSourceMappingsRequest
        {
            FunctionName = "esm-crud-func",
        });

        Assert.Single(listed.EventSourceMappings);

        // Delete ESM
        var deleted = await _lambda.DeleteEventSourceMappingAsync(new DeleteEventSourceMappingRequest
        {
            UUID = created.UUID,
        });

        Assert.NotNull(deleted.UUID);

        // Verify deletion
        var listedAfterDelete = await _lambda.ListEventSourceMappingsAsync(new ListEventSourceMappingsRequest
        {
            FunctionName = "esm-crud-func",
        });

        Assert.Empty(listedAfterDelete.EventSourceMappings);
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

        Assert.True(consumed, "All messages should have been consumed by Lambda ESM");
    }
}
