using System.IO.Compression;
using System.Text;
using Amazon;
using Amazon.ApiGatewayV2;
using Amazon.ApiGatewayV2.Model;
using Amazon.Lambda;
using Amazon.Lambda.Model;
using Amazon.Runtime;
using Microsoft.AspNetCore.Mvc.Testing;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the API Gateway v2 HTTP API service handler.
/// Uses AWSSDK.ApiGatewayV2 and AWSSDK.Lambda pointed at the in-process MicroStack server.
///
/// Ports coverage from ministack/tests/test_apigw.py (~45 tests).
/// </summary>
public sealed class ApiGatewayV2Tests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonApiGatewayV2Client _apigw;
    private readonly AmazonLambdaClient _lambda;

    private const string LambdaRole = "arn:aws:iam::000000000000:role/lambda-role";

    public ApiGatewayV2Tests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _apigw = CreateApigwClient(fixture);
        _lambda = CreateLambdaClient(fixture);
    }

    private static AmazonApiGatewayV2Client CreateApigwClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonApiGatewayV2Config
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonApiGatewayV2Client(
            new BasicAWSCredentials("test", "test"), config);
    }

    private static AmazonLambdaClient CreateLambdaClient(MicroStackFixture fixture)
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
        _apigw.Dispose();
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

    private async Task<string> CreatePythonFunction(string name, string code)
    {
        using var zip = MakeZip("index.py", code);
        await _lambda.CreateFunctionAsync(new CreateFunctionRequest
        {
            FunctionName = name,
            Runtime = "python3.9",
            Role = LambdaRole,
            Handler = "index.handler",
            Code = new FunctionCode { ZipFile = zip },
        });
        return $"arn:aws:lambda:us-east-1:000000000000:function:{name}";
    }

    /// <summary>
    /// Creates a full API Gateway setup: API + integration + route + stage.
    /// Returns (apiId, integrationId, routeId).
    /// </summary>
    private async Task<(string ApiId, string IntegrationId, string RouteId)> CreateApiWithLambdaRoute(
        string funcArn, string routeKey)
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = $"test-api-{Guid.NewGuid():N}",
            ProtocolType = ProtocolType.HTTP,
        });
        var apiId = api.ApiId;

        var integ = await _apigw.CreateIntegrationAsync(new CreateIntegrationRequest
        {
            ApiId = apiId,
            IntegrationType = IntegrationType.AWS_PROXY,
            IntegrationUri = funcArn,
            PayloadFormatVersion = "2.0",
        });

        var route = await _apigw.CreateRouteAsync(new CreateRouteRequest
        {
            ApiId = apiId,
            RouteKey = routeKey,
            Target = $"integrations/{integ.IntegrationId}",
        });

        await _apigw.CreateStageAsync(new CreateStageRequest
        {
            ApiId = apiId,
            StageName = "$default",
        });

        return (apiId, integ.IntegrationId, route.RouteId);
    }

    /// <summary>
    /// Sends a data plane request via the test server's HttpClient with the appropriate Host header.
    /// </summary>
    private async Task<HttpResponseMessage> ExecuteApiRequest(
        string apiId, HttpMethod method, string path)
    {
        var request = new HttpRequestMessage(method, path);
        request.Headers.Host = $"{apiId}.execute-api.localhost";
        return await _fixture.HttpClient.SendAsync(request);
    }

    private async Task<HttpResponseMessage> ExecuteApiRequest(
        string apiId, HttpMethod method, string path,
        Dictionary<string, string> headers)
    {
        var request = new HttpRequestMessage(method, path);
        request.Headers.Host = $"{apiId}.execute-api.localhost";
        foreach (var (key, value) in headers)
        {
            request.Headers.TryAddWithoutValidation(key, value);
        }
        return await _fixture.HttpClient.SendAsync(request);
    }

    // ── Control Plane: API CRUD ─────────────────────────────────────────────────

    [Fact]
    public async Task CreateApi()
    {
        var resp = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "test-api",
            ProtocolType = ProtocolType.HTTP,
        });
        Assert.NotEmpty(resp.ApiId);
        Assert.Equal("test-api", resp.Name);
        Assert.Equal(ProtocolType.HTTP, resp.ProtocolType);
    }

    [Fact]
    public async Task GetApi()
    {
        var created = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "get-api-test",
            ProtocolType = ProtocolType.HTTP,
        });
        var resp = await _apigw.GetApiAsync(new GetApiRequest { ApiId = created.ApiId });
        Assert.Equal(created.ApiId, resp.ApiId);
        Assert.Equal("get-api-test", resp.Name);
    }

    [Fact]
    public async Task GetApis()
    {
        await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "list-api-a",
            ProtocolType = ProtocolType.HTTP,
        });
        await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "list-api-b",
            ProtocolType = ProtocolType.HTTP,
        });
        var resp = await _apigw.GetApisAsync(new GetApisRequest());
        var names = resp.Items.Select(a => a.Name).ToList();
        Assert.Contains("list-api-a", names);
        Assert.Contains("list-api-b", names);
    }

    [Fact]
    public async Task UpdateApi()
    {
        var created = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "update-api-before",
            ProtocolType = ProtocolType.HTTP,
        });
        await _apigw.UpdateApiAsync(new UpdateApiRequest
        {
            ApiId = created.ApiId,
            Name = "update-api-after",
        });
        var resp = await _apigw.GetApiAsync(new GetApiRequest { ApiId = created.ApiId });
        Assert.Equal("update-api-after", resp.Name);
    }

    [Fact]
    public async Task DeleteApi()
    {
        var created = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "delete-api-test",
            ProtocolType = ProtocolType.HTTP,
        });
        await _apigw.DeleteApiAsync(new DeleteApiRequest { ApiId = created.ApiId });
        var ex = await Assert.ThrowsAsync<Amazon.ApiGatewayV2.Model.NotFoundException>(
            () => _apigw.GetApiAsync(new GetApiRequest { ApiId = created.ApiId }));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    // ── Control Plane: Routes ────────────────────────────────────────────────────

    [Fact]
    public async Task CreateRoute()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "route-api",
            ProtocolType = ProtocolType.HTTP,
        });
        var resp = await _apigw.CreateRouteAsync(new CreateRouteRequest
        {
            ApiId = api.ApiId,
            RouteKey = "GET /items",
        });
        Assert.NotEmpty(resp.RouteId);
        Assert.Equal("GET /items", resp.RouteKey);
    }

    [Fact]
    public async Task GetRoutes()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "routes-list-api",
            ProtocolType = ProtocolType.HTTP,
        });
        await _apigw.CreateRouteAsync(new CreateRouteRequest
        {
            ApiId = api.ApiId,
            RouteKey = "GET /a",
        });
        await _apigw.CreateRouteAsync(new CreateRouteRequest
        {
            ApiId = api.ApiId,
            RouteKey = "POST /b",
        });
        var resp = await _apigw.GetRoutesAsync(new GetRoutesRequest { ApiId = api.ApiId });
        var keys = resp.Items.Select(r => r.RouteKey).ToList();
        Assert.Contains("GET /a", keys);
        Assert.Contains("POST /b", keys);
    }

    [Fact]
    public async Task GetRoute()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "get-route-api",
            ProtocolType = ProtocolType.HTTP,
        });
        var created = await _apigw.CreateRouteAsync(new CreateRouteRequest
        {
            ApiId = api.ApiId,
            RouteKey = "DELETE /things",
        });
        var resp = await _apigw.GetRouteAsync(new GetRouteRequest
        {
            ApiId = api.ApiId,
            RouteId = created.RouteId,
        });
        Assert.Equal(created.RouteId, resp.RouteId);
        Assert.Equal("DELETE /things", resp.RouteKey);
    }

    [Fact]
    public async Task UpdateRoute()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "update-route-api",
            ProtocolType = ProtocolType.HTTP,
        });
        var created = await _apigw.CreateRouteAsync(new CreateRouteRequest
        {
            ApiId = api.ApiId,
            RouteKey = "GET /old",
        });
        await _apigw.UpdateRouteAsync(new UpdateRouteRequest
        {
            ApiId = api.ApiId,
            RouteId = created.RouteId,
            RouteKey = "GET /new",
        });
        var resp = await _apigw.GetRouteAsync(new GetRouteRequest
        {
            ApiId = api.ApiId,
            RouteId = created.RouteId,
        });
        Assert.Equal("GET /new", resp.RouteKey);
    }

    [Fact]
    public async Task DeleteRoute()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "del-route-api",
            ProtocolType = ProtocolType.HTTP,
        });
        var created = await _apigw.CreateRouteAsync(new CreateRouteRequest
        {
            ApiId = api.ApiId,
            RouteKey = "GET /gone",
        });
        await _apigw.DeleteRouteAsync(new DeleteRouteRequest
        {
            ApiId = api.ApiId,
            RouteId = created.RouteId,
        });
        var resp = await _apigw.GetRoutesAsync(new GetRoutesRequest { ApiId = api.ApiId });
        Assert.DoesNotContain(resp.Items, r => r.RouteId == created.RouteId);
    }

    // ── Control Plane: Integrations ──────────────────────────────────────────────

    [Fact]
    public async Task CreateIntegration()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "integ-api",
            ProtocolType = ProtocolType.HTTP,
        });
        var resp = await _apigw.CreateIntegrationAsync(new CreateIntegrationRequest
        {
            ApiId = api.ApiId,
            IntegrationType = IntegrationType.AWS_PROXY,
            IntegrationUri = "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
            PayloadFormatVersion = "2.0",
        });
        Assert.NotEmpty(resp.IntegrationId);
        Assert.Equal(IntegrationType.AWS_PROXY, resp.IntegrationType);
        Assert.Equal("2.0", resp.PayloadFormatVersion);
    }

    [Fact]
    public async Task GetIntegrations()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "integ-list-api",
            ProtocolType = ProtocolType.HTTP,
        });
        await _apigw.CreateIntegrationAsync(new CreateIntegrationRequest
        {
            ApiId = api.ApiId,
            IntegrationType = IntegrationType.AWS_PROXY,
            IntegrationUri = "arn:aws:lambda:us-east-1:000000000000:function:fn1",
        });
        var resp = await _apigw.GetIntegrationsAsync(new GetIntegrationsRequest { ApiId = api.ApiId });
        Assert.True(resp.Items.Count >= 1);
    }

    [Fact]
    public async Task GetIntegration()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "get-integ-api",
            ProtocolType = ProtocolType.HTTP,
        });
        var created = await _apigw.CreateIntegrationAsync(new CreateIntegrationRequest
        {
            ApiId = api.ApiId,
            IntegrationType = IntegrationType.HTTP_PROXY,
            IntegrationUri = "https://example.com",
            IntegrationMethod = "GET",
        });
        var resp = await _apigw.GetIntegrationAsync(new GetIntegrationRequest
        {
            ApiId = api.ApiId,
            IntegrationId = created.IntegrationId,
        });
        Assert.Equal(created.IntegrationId, resp.IntegrationId);
        Assert.Equal(IntegrationType.HTTP_PROXY, resp.IntegrationType);
    }

    [Fact]
    public async Task DeleteIntegration()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "del-integ-api",
            ProtocolType = ProtocolType.HTTP,
        });
        var created = await _apigw.CreateIntegrationAsync(new CreateIntegrationRequest
        {
            ApiId = api.ApiId,
            IntegrationType = IntegrationType.AWS_PROXY,
            IntegrationUri = "arn:aws:lambda:us-east-1:000000000000:function:fn2",
        });
        await _apigw.DeleteIntegrationAsync(new DeleteIntegrationRequest
        {
            ApiId = api.ApiId,
            IntegrationId = created.IntegrationId,
        });
        var resp = await _apigw.GetIntegrationsAsync(new GetIntegrationsRequest { ApiId = api.ApiId });
        Assert.DoesNotContain(resp.Items, i => i.IntegrationId == created.IntegrationId);
    }

    // ── Control Plane: Stages ────────────────────────────────────────────────────

    [Fact]
    public async Task CreateStage()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "stage-api",
            ProtocolType = ProtocolType.HTTP,
        });
        var resp = await _apigw.CreateStageAsync(new CreateStageRequest
        {
            ApiId = api.ApiId,
            StageName = "prod",
        });
        Assert.Equal("prod", resp.StageName);
    }

    [Fact]
    public async Task GetStages()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "stages-list-api",
            ProtocolType = ProtocolType.HTTP,
        });
        await _apigw.CreateStageAsync(new CreateStageRequest
        {
            ApiId = api.ApiId,
            StageName = "v1",
        });
        await _apigw.CreateStageAsync(new CreateStageRequest
        {
            ApiId = api.ApiId,
            StageName = "v2",
        });
        var resp = await _apigw.GetStagesAsync(new GetStagesRequest { ApiId = api.ApiId });
        var names = resp.Items.Select(s => s.StageName).ToList();
        Assert.Contains("v1", names);
        Assert.Contains("v2", names);
    }

    [Fact]
    public async Task GetStage()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "get-stage-api",
            ProtocolType = ProtocolType.HTTP,
        });
        await _apigw.CreateStageAsync(new CreateStageRequest
        {
            ApiId = api.ApiId,
            StageName = "dev",
        });
        var resp = await _apigw.GetStageAsync(new GetStageRequest
        {
            ApiId = api.ApiId,
            StageName = "dev",
        });
        Assert.Equal("dev", resp.StageName);
    }

    [Fact]
    public async Task UpdateStage()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "update-stage-api",
            ProtocolType = ProtocolType.HTTP,
        });
        await _apigw.CreateStageAsync(new CreateStageRequest
        {
            ApiId = api.ApiId,
            StageName = "staging",
        });
        await _apigw.UpdateStageAsync(new UpdateStageRequest
        {
            ApiId = api.ApiId,
            StageName = "staging",
            Description = "updated",
        });
        var resp = await _apigw.GetStageAsync(new GetStageRequest
        {
            ApiId = api.ApiId,
            StageName = "staging",
        });
        Assert.Equal("updated", resp.Description);
    }

    [Fact]
    public async Task DeleteStage()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "del-stage-api",
            ProtocolType = ProtocolType.HTTP,
        });
        await _apigw.CreateStageAsync(new CreateStageRequest
        {
            ApiId = api.ApiId,
            StageName = "temp",
        });
        await _apigw.DeleteStageAsync(new DeleteStageRequest
        {
            ApiId = api.ApiId,
            StageName = "temp",
        });
        var resp = await _apigw.GetStagesAsync(new GetStagesRequest { ApiId = api.ApiId });
        Assert.DoesNotContain(resp.Items, s => s.StageName == "temp");
    }

    // ── Control Plane: Deployments ───────────────────────────────────────────────

    [Fact]
    public async Task CreateDeployment()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "deploy-api",
            ProtocolType = ProtocolType.HTTP,
        });
        var resp = await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest
        {
            ApiId = api.ApiId,
        });
        Assert.NotEmpty(resp.DeploymentId);
        Assert.Equal(DeploymentStatus.DEPLOYED, resp.DeploymentStatus);
    }

    [Fact]
    public async Task GetDeployments()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "deployments-list-api",
            ProtocolType = ProtocolType.HTTP,
        });
        await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest
        {
            ApiId = api.ApiId,
            Description = "first",
        });
        await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest
        {
            ApiId = api.ApiId,
            Description = "second",
        });
        var resp = await _apigw.GetDeploymentsAsync(new GetDeploymentsRequest { ApiId = api.ApiId });
        Assert.True(resp.Items.Count >= 2);
    }

    [Fact]
    public async Task GetDeployment()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "get-deploy-api",
            ProtocolType = ProtocolType.HTTP,
        });
        var created = await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest
        {
            ApiId = api.ApiId,
            Description = "single",
        });
        var resp = await _apigw.GetDeploymentAsync(new GetDeploymentRequest
        {
            ApiId = api.ApiId,
            DeploymentId = created.DeploymentId,
        });
        Assert.Equal(created.DeploymentId, resp.DeploymentId);
    }

    [Fact]
    public async Task DeleteDeployment()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "del-deploy-api",
            ProtocolType = ProtocolType.HTTP,
        });
        var created = await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest
        {
            ApiId = api.ApiId,
        });
        await _apigw.DeleteDeploymentAsync(new DeleteDeploymentRequest
        {
            ApiId = api.ApiId,
            DeploymentId = created.DeploymentId,
        });
        var resp = await _apigw.GetDeploymentsAsync(new GetDeploymentsRequest { ApiId = api.ApiId });
        Assert.DoesNotContain(resp.Items, d => d.DeploymentId == created.DeploymentId);
    }

    // ── Control Plane: Tags ─────────────────────────────────────────────────────

    [Fact]
    public async Task TagResource()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "tag-api",
            ProtocolType = ProtocolType.HTTP,
        });
        var resourceArn = $"arn:aws:apigateway:us-east-1::/apis/{api.ApiId}";
        await _apigw.TagResourceAsync(new Amazon.ApiGatewayV2.Model.TagResourceRequest
        {
            ResourceArn = resourceArn,
            Tags = new Dictionary<string, string> { ["env"] = "test", ["owner"] = "team-a" },
        });
        var resp = await _apigw.GetTagsAsync(new GetTagsRequest { ResourceArn = resourceArn });
        Assert.Equal("test", resp.Tags["env"]);
        Assert.Equal("team-a", resp.Tags["owner"]);
    }

    [Fact]
    public async Task UntagResource()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "untag-api",
            ProtocolType = ProtocolType.HTTP,
        });
        var resourceArn = $"arn:aws:apigateway:us-east-1::/apis/{api.ApiId}";
        await _apigw.TagResourceAsync(new Amazon.ApiGatewayV2.Model.TagResourceRequest
        {
            ResourceArn = resourceArn,
            Tags = new Dictionary<string, string> { ["remove-me"] = "yes", ["keep-me"] = "yes" },
        });
        await _apigw.UntagResourceAsync(new Amazon.ApiGatewayV2.Model.UntagResourceRequest
        {
            ResourceArn = resourceArn,
            TagKeys = ["remove-me"],
        });
        var resp = await _apigw.GetTagsAsync(new GetTagsRequest { ResourceArn = resourceArn });
        Assert.DoesNotContain("remove-me", resp.Tags.Keys);
        Assert.Equal("yes", resp.Tags["keep-me"]);
    }

    // ── Control Plane: Error cases ──────────────────────────────────────────────

    [Fact]
    public async Task ApiNotFound()
    {
        var ex = await Assert.ThrowsAsync<Amazon.ApiGatewayV2.Model.NotFoundException>(
            () => _apigw.GetApiAsync(new GetApiRequest { ApiId = "00000000" }));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [Fact]
    public async Task RouteOnDeletedApi()
    {
        var ex = await Assert.ThrowsAsync<Amazon.ApiGatewayV2.Model.NotFoundException>(
            () => _apigw.CreateRouteAsync(new CreateRouteRequest
            {
                ApiId = "00000000",
                RouteKey = "GET /x",
            }));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [Fact]
    public async Task HttpProtocolType()
    {
        var resp = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "http-proto-api",
            ProtocolType = ProtocolType.HTTP,
        });
        Assert.Equal(ProtocolType.HTTP, resp.ProtocolType);
        var fetched = await _apigw.GetApiAsync(new GetApiRequest { ApiId = resp.ApiId });
        Assert.Equal(ProtocolType.HTTP, fetched.ProtocolType);
    }

    // ── Control Plane: Authorizer CRUD ──────────────────────────────────────────

    [Fact]
    public async Task AuthorizerCrud()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = $"auth-test-{Guid.NewGuid():N}",
            ProtocolType = ProtocolType.HTTP,
        });

        // Create JWT authorizer
        var created = await _apigw.CreateAuthorizerAsync(new CreateAuthorizerRequest
        {
            ApiId = api.ApiId,
            AuthorizerType = AuthorizerType.JWT,
            Name = "my-jwt-auth",
            IdentitySource = new List<string> { "$request.header.Authorization" },
            JwtConfiguration = new JWTConfiguration
            {
                Audience = ["https://example.com"],
                Issuer = "https://idp.example.com",
            },
        });
        Assert.Equal(AuthorizerType.JWT, created.AuthorizerType);
        Assert.Equal("my-jwt-auth", created.Name);
        var authId = created.AuthorizerId;

        // Get single
        var got = await _apigw.GetAuthorizerAsync(new GetAuthorizerRequest
        {
            ApiId = api.ApiId,
            AuthorizerId = authId,
        });
        Assert.Equal(authId, got.AuthorizerId);
        Assert.Equal("https://idp.example.com", got.JwtConfiguration.Issuer);

        // List
        var listed = await _apigw.GetAuthorizersAsync(new GetAuthorizersRequest { ApiId = api.ApiId });
        Assert.Contains(listed.Items, a => a.AuthorizerId == authId);

        // Update
        var updated = await _apigw.UpdateAuthorizerAsync(new UpdateAuthorizerRequest
        {
            ApiId = api.ApiId,
            AuthorizerId = authId,
            Name = "renamed-auth",
        });
        Assert.Equal("renamed-auth", updated.Name);

        // Delete
        await _apigw.DeleteAuthorizerAsync(new DeleteAuthorizerRequest
        {
            ApiId = api.ApiId,
            AuthorizerId = authId,
        });
        var listed2 = await _apigw.GetAuthorizersAsync(new GetAuthorizersRequest { ApiId = api.ApiId });
        Assert.DoesNotContain(listed2.Items, a => a.AuthorizerId == authId);
    }

    // ── Control Plane: UpdateIntegration ─────────────────────────────────────────

    [Fact]
    public async Task UpdateIntegration()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "qa-apigw-update-integ",
            ProtocolType = ProtocolType.HTTP,
        });
        var created = await _apigw.CreateIntegrationAsync(new CreateIntegrationRequest
        {
            ApiId = api.ApiId,
            IntegrationType = IntegrationType.AWS_PROXY,
            IntegrationUri = "arn:aws:lambda:us-east-1:000000000000:function:old-fn",
        });
        await _apigw.UpdateIntegrationAsync(new UpdateIntegrationRequest
        {
            ApiId = api.ApiId,
            IntegrationId = created.IntegrationId,
            IntegrationUri = "arn:aws:lambda:us-east-1:000000000000:function:new-fn",
        });
        var resp = await _apigw.GetIntegrationAsync(new GetIntegrationRequest
        {
            ApiId = api.ApiId,
            IntegrationId = created.IntegrationId,
        });
        Assert.Contains("new-fn", resp.IntegrationUri);
    }

    // ── Control Plane: DeleteRoute V2 ────────────────────────────────────────────

    [Fact]
    public async Task DeleteRouteV2()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "qa-apigw-del-route",
            ProtocolType = ProtocolType.HTTP,
        });
        var route = await _apigw.CreateRouteAsync(new CreateRouteRequest
        {
            ApiId = api.ApiId,
            RouteKey = "GET /qa",
        });
        await _apigw.DeleteRouteAsync(new DeleteRouteRequest
        {
            ApiId = api.ApiId,
            RouteId = route.RouteId,
        });
        var routes = await _apigw.GetRoutesAsync(new GetRoutesRequest { ApiId = api.ApiId });
        Assert.DoesNotContain(routes.Items, r => r.RouteId == route.RouteId);
    }

    // ── Control Plane: Stage Variables ───────────────────────────────────────────

    [Fact]
    public async Task StageVariables()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "qa-apigw-stage-vars",
            ProtocolType = ProtocolType.HTTP,
        });
        await _apigw.CreateStageAsync(new CreateStageRequest
        {
            ApiId = api.ApiId,
            StageName = "dev",
            StageVariables = new Dictionary<string, string>
            {
                ["env"] = "development",
                ["version"] = "1",
            },
        });
        var stage = await _apigw.GetStageAsync(new GetStageRequest
        {
            ApiId = api.ApiId,
            StageName = "dev",
        });
        Assert.Equal("development", stage.StageVariables["env"]);
        Assert.Equal("1", stage.StageVariables["version"]);
    }

    // ── Control Plane: Stage Timestamps ──────────────────────────────────────────

    [Fact]
    public async Task V2StageTimestamps()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "ts-stage-v44",
            ProtocolType = ProtocolType.HTTP,
        });
        var stage = await _apigw.CreateStageAsync(new CreateStageRequest
        {
            ApiId = api.ApiId,
            StageName = "test-stage",
        });
        // SDK parses ISO 8601 timestamps into DateTime
        Assert.True(stage.CreatedDate > DateTime.MinValue,
            $"CreatedDate should be a valid datetime, got {stage.CreatedDate}");
        Assert.True(stage.LastUpdatedDate > DateTime.MinValue,
            $"LastUpdatedDate should be a valid datetime, got {stage.LastUpdatedDate}");
    }

    // ── Data Plane: Execute No Route ────────────────────────────────────────────

    [Fact]
    public async Task ExecuteNoRoute()
    {
        var api = await _apigw.CreateApiAsync(new CreateApiRequest
        {
            Name = "no-route-api",
            ProtocolType = ProtocolType.HTTP,
        });
        await _apigw.CreateStageAsync(new CreateStageRequest
        {
            ApiId = api.ApiId,
            StageName = "$default",
        });

        var resp = await ExecuteApiRequest(api.ApiId, HttpMethod.Get, "/$default/nonexistent");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    // ── Data Plane: Lambda Proxy Tests ──────────────────────────────────────────

    [Fact]
    public async Task ExecuteLambdaProxy()
    {
        var fname = $"intg-apigw-fn-{Guid.NewGuid():N}"[..30];
        const string code =
            "import json\n" +
            "def handler(event, context):\n" +
            "    return {\n" +
            "        'statusCode': 200,\n" +
            "        'headers': {'Content-Type': 'application/json'},\n" +
            "        'body': json.dumps({'path': event.get('rawPath', '/')}),\n" +
            "    }\n";

        var funcArn = await CreatePythonFunction(fname, code);
        var (apiId, _, _) = await CreateApiWithLambdaRoute(funcArn, "GET /hello");

        var resp = await ExecuteApiRequest(apiId, HttpMethod.Get, "/$default/hello");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var body = JsonSerializer.Deserialize<JsonElement>(await resp.Content.ReadAsStringAsync());
        Assert.Equal("/hello", body.GetProperty("path").GetString());
    }

    [Fact]
    public async Task ExecuteDefaultRoute()
    {
        var fname = $"intg-default-fn-{Guid.NewGuid():N}"[..30];
        const string code =
            "def handler(event, context):\n" +
            "    return {'statusCode': 200, 'body': 'ok'}\n";

        var funcArn = await CreatePythonFunction(fname, code);
        var (apiId, _, _) = await CreateApiWithLambdaRoute(funcArn, "$default");

        var resp = await ExecuteApiRequest(apiId, HttpMethod.Post, "/$default/any/path/here");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
    }

    [Fact]
    public async Task PathParamRoute()
    {
        var fname = $"intg-param-fn-{Guid.NewGuid():N}"[..30];
        const string code =
            "import json\n" +
            "def handler(event, context):\n" +
            "    return {'statusCode': 200, 'body': json.dumps({'rawPath': event.get('rawPath')})}\n";

        var funcArn = await CreatePythonFunction(fname, code);
        var (apiId, _, _) = await CreateApiWithLambdaRoute(funcArn, "GET /items/{id}");

        var resp = await ExecuteApiRequest(apiId, HttpMethod.Get, "/$default/items/abc123");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var body = JsonSerializer.Deserialize<JsonElement>(await resp.Content.ReadAsStringAsync());
        Assert.Equal("/items/abc123", body.GetProperty("rawPath").GetString());
    }

    [Fact]
    public async Task PathParametersInEvent()
    {
        var fname = $"intg-pathparam-{Guid.NewGuid():N}"[..30];
        const string code =
            "import json\n" +
            "def handler(event, context):\n" +
            "    return {'statusCode': 200, 'body': json.dumps(event.get('pathParameters'))}\n";

        var funcArn = await CreatePythonFunction(fname, code);
        var (apiId, _, _) = await CreateApiWithLambdaRoute(funcArn, "GET /items/{itemId}");

        var resp = await ExecuteApiRequest(apiId, HttpMethod.Get, "/$default/items/my-item-42");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var body = JsonSerializer.Deserialize<JsonElement>(await resp.Content.ReadAsStringAsync());
        Assert.Equal("my-item-42", body.GetProperty("itemId").GetString());
    }

    [Fact]
    public async Task GreedyPathParametersInEvent()
    {
        var fname = $"intg-greedy-pp-{Guid.NewGuid():N}"[..30];
        const string code =
            "import json\n" +
            "def handler(event, context):\n" +
            "    return {'statusCode': 200, 'body': json.dumps(event.get('pathParameters'))}\n";

        var funcArn = await CreatePythonFunction(fname, code);
        var (apiId, _, _) = await CreateApiWithLambdaRoute(funcArn, "GET /files/{proxy+}");

        var resp = await ExecuteApiRequest(apiId, HttpMethod.Get, "/$default/files/a/b/c.txt");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var body = JsonSerializer.Deserialize<JsonElement>(await resp.Content.ReadAsStringAsync());
        Assert.Equal("a/b/c.txt", body.GetProperty("proxy").GetString());
    }

    [Fact]
    public async Task QueryParamsAndHeadersInEvent()
    {
        var fname = $"intg-qp-{Guid.NewGuid():N}"[..30];
        const string code =
            "import json\n" +
            "def handler(event, context):\n" +
            "    return {'statusCode': 200, 'body': json.dumps({\n" +
            "        'qs': event.get('queryStringParameters'),\n" +
            "        'rawQs': event.get('rawQueryString'),\n" +
            "        'customHeader': event.get('headers', {}).get('x-custom-header'),\n" +
            "    })}\n";

        var funcArn = await CreatePythonFunction(fname, code);
        var (apiId, _, _) = await CreateApiWithLambdaRoute(funcArn, "GET /search");

        var resp = await ExecuteApiRequest(apiId, HttpMethod.Get,
            "/$default/search?q=hello&tag=a&tag=b",
            new Dictionary<string, string> { ["X-Custom-Header"] = "test-value" });
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var body = JsonSerializer.Deserialize<JsonElement>(await resp.Content.ReadAsStringAsync());
        Assert.Equal("hello", body.GetProperty("qs").GetProperty("q").GetString());
        var tagVal = body.GetProperty("qs").GetProperty("tag").GetString();
        Assert.Equal("a,b", tagVal);
        var rawQs = body.GetProperty("rawQs").GetString()!;
        Assert.Contains("q=hello", rawQs);
        Assert.Contains("tag=a", rawQs);
        Assert.Contains("tag=b", rawQs);
        Assert.Equal("test-value", body.GetProperty("customHeader").GetString());
    }

    [Fact]
    public async Task MultiplePathParameters()
    {
        var fname = $"intg-multi-pp-{Guid.NewGuid():N}"[..30];
        const string code =
            "import json\n" +
            "def handler(event, context):\n" +
            "    return {'statusCode': 200, 'body': json.dumps(event.get('pathParameters'))}\n";

        var funcArn = await CreatePythonFunction(fname, code);
        var (apiId, _, _) = await CreateApiWithLambdaRoute(funcArn,
            "GET /projects/{projectKey}/items/{itemId}");

        var resp = await ExecuteApiRequest(apiId, HttpMethod.Get,
            "/$default/projects/bunya/items/prod-42");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var body = JsonSerializer.Deserialize<JsonElement>(await resp.Content.ReadAsStringAsync());
        Assert.Equal("bunya", body.GetProperty("projectKey").GetString());
        Assert.Equal("prod-42", body.GetProperty("itemId").GetString());
    }

    [Fact]
    public async Task NoPathParametersReturnsNull()
    {
        var fname = $"intg-no-pp-{Guid.NewGuid():N}"[..30];
        const string code =
            "import json\n" +
            "def handler(event, context):\n" +
            "    return {'statusCode': 200, 'body': json.dumps({'pp': event.get('pathParameters')})}\n";

        var funcArn = await CreatePythonFunction(fname, code);
        var (apiId, _, _) = await CreateApiWithLambdaRoute(funcArn, "GET /products");

        var resp = await ExecuteApiRequest(apiId, HttpMethod.Get, "/$default/products");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var body = JsonSerializer.Deserialize<JsonElement>(await resp.Content.ReadAsStringAsync());
        Assert.Equal(JsonValueKind.Null, body.GetProperty("pp").ValueKind);
    }

    [Fact]
    public async Task UrlEncodedPathParameter()
    {
        var fname = $"intg-enc-pp-{Guid.NewGuid():N}"[..30];
        const string code =
            "import json\n" +
            "def handler(event, context):\n" +
            "    return {'statusCode': 200, 'body': json.dumps(event.get('pathParameters'))}\n";

        var funcArn = await CreatePythonFunction(fname, code);
        var (apiId, _, _) = await CreateApiWithLambdaRoute(funcArn, "GET /items/{itemId}");

        var resp = await ExecuteApiRequest(apiId, HttpMethod.Get, "/$default/items/hello%20world");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var body = JsonSerializer.Deserialize<JsonElement>(await resp.Content.ReadAsStringAsync());
        Assert.Equal("hello world", body.GetProperty("itemId").GetString());
    }

    [Fact]
    public async Task GreedyPathParam()
    {
        var fname = $"intg-greedy-{Guid.NewGuid():N}"[..30];
        const string code =
            "def handler(event, context):\n" +
            "    return {'statusCode': 200, 'body': event['rawPath']}\n";

        var funcArn = await CreatePythonFunction(fname, code);
        var (apiId, _, _) = await CreateApiWithLambdaRoute(funcArn, "GET /files/{proxy+}");

        var resp = await ExecuteApiRequest(apiId, HttpMethod.Get, "/$default/files/a/b/c");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var body = await resp.Content.ReadAsStringAsync();
        Assert.Equal("/files/a/b/c", body);
    }

    [Fact]
    public async Task RouteKeyInLambdaEvent()
    {
        var fname = $"intg-rk-{Guid.NewGuid():N}"[..30];
        const string code =
            "def handler(event, context):\n" +
            "    return {'statusCode': 200, 'body': event['routeKey']}\n";

        var funcArn = await CreatePythonFunction(fname, code);
        var (apiId, _, _) = await CreateApiWithLambdaRoute(funcArn, "GET /ping");

        var resp = await ExecuteApiRequest(apiId, HttpMethod.Get, "/$default/ping");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var body = await resp.Content.ReadAsStringAsync();
        Assert.Equal("GET /ping", body);
    }
}
