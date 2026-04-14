using System.IO.Compression;
using System.Net;
using System.Text;
using Amazon;
using Amazon.APIGateway;
using Amazon.APIGateway.Model;
using Amazon.Lambda;
using Amazon.Lambda.Model;
using Amazon.Runtime;
using Microsoft.AspNetCore.Mvc.Testing;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the API Gateway v1 REST API service handler.
/// Uses AWSSDK.APIGateway and AWSSDK.Lambda pointed at the in-process MicroStack server.
///
/// Ports coverage from ministack/tests/test_apigwv1.py (~30 tests).
/// </summary>
public sealed class ApiGatewayV1Tests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonAPIGatewayClient _apigw;
    private readonly AmazonLambdaClient _lambda;
    private readonly HttpClient _rawHttp;

    private const string LambdaRole = "arn:aws:iam::000000000000:role/lambda-role";

    public ApiGatewayV1Tests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _apigw = CreateApigwClient(fixture);
        _lambda = CreateLambdaClient(fixture);

        // Raw HTTP client for data plane (execute-api) requests
        var innerHandler = fixture.Factory.Server.CreateHandler();
        _rawHttp = new HttpClient(innerHandler) { BaseAddress = new Uri("http://localhost/") };
    }

    private static AmazonAPIGatewayClient CreateApigwClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonAPIGatewayConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonAPIGatewayClient(
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
        _rawHttp.Dispose();
        return Task.CompletedTask;
    }

    // -- Helper: Create a Lambda zip with simple Python handler ----------------

    private static byte[] CreatePythonZip(string code)
    {
        using var ms = new MemoryStream();
        using (var zip = new ZipArchive(ms, ZipArchiveMode.Create, leaveOpen: true))
        {
            var entry = zip.CreateEntry("index.py");
            using var writer = new StreamWriter(entry.Open());
            writer.Write(code);
        }
        return ms.ToArray();
    }

    // -- Helper: Get root resource ID for a REST API ---------------------------

    private async Task<string> GetRootResourceIdAsync(string restApiId)
    {
        var resources = await _apigw.GetResourcesAsync(new GetResourcesRequest
        {
            RestApiId = restApiId,
        });
        return resources.Items.First(r => r.Path == "/").Id;
    }

    // -- Helper: Send execute-api request via raw HTTP -------------------------

    private async Task<HttpResponseMessage> ExecuteApiAsync(string apiId, string stageName, string path, HttpMethod method)
    {
        var url = $"/{stageName}{path}";
        var request = new HttpRequestMessage(method, url);
        request.Headers.Host = $"{apiId}.execute-api.localhost";
        return await _rawHttp.SendAsync(request);
    }

    // ---- REST API CRUD -------------------------------------------------------

    [Fact]
    public async Task CreateRestApi()
    {
        var resp = await _apigw.CreateRestApiAsync(new CreateRestApiRequest
        {
            Name = "v1-create-test",
        });
        resp.Id.ShouldNotBeNull();
        resp.Name.ShouldBe("v1-create-test");
        (resp.CreatedDate > DateTime.MinValue).ShouldBe(true, "createdDate should be set");
    }

    [Fact]
    public async Task GetRestApi()
    {
        var created = await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-get-test" });
        var resp = await _apigw.GetRestApiAsync(new GetRestApiRequest { RestApiId = created.Id });
        resp.Id.ShouldBe(created.Id);
        resp.Name.ShouldBe("v1-get-test");
    }

    [Fact]
    public async Task GetRestApis()
    {
        var id1 = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-list-a" })).Id;
        var id2 = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-list-b" })).Id;
        var resp = await _apigw.GetRestApisAsync(new GetRestApisRequest());
        var ids = resp.Items.Select(a => a.Id).ToList();
        ids.ShouldContain(id1);
        ids.ShouldContain(id2);
    }

    [Fact]
    public async Task UpdateRestApi()
    {
        var created = await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-update-before" });
        await _apigw.UpdateRestApiAsync(new UpdateRestApiRequest
        {
            RestApiId = created.Id,
            PatchOperations = [new PatchOperation { Op = Op.Replace, Path = "/name", Value = "v1-update-after" }],
        });
        var resp = await _apigw.GetRestApiAsync(new GetRestApiRequest { RestApiId = created.Id });
        resp.Name.ShouldBe("v1-update-after");
    }

    [Fact]
    public async Task DeleteRestApi()
    {
        var created = await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-delete-test" });
        await _apigw.DeleteRestApiAsync(new DeleteRestApiRequest { RestApiId = created.Id });
        var ex = await Should.ThrowAsync<NotFoundException>(() =>
            _apigw.GetRestApiAsync(new GetRestApiRequest { RestApiId = created.Id }));
        ex.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    // ---- Resources -----------------------------------------------------------

    [Fact]
    public async Task CreateResource()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-resource-create" })).Id;
        var rootId = await GetRootResourceIdAsync(apiId);
        var resp = await _apigw.CreateResourceAsync(new CreateResourceRequest
        {
            RestApiId = apiId,
            ParentId = rootId,
            PathPart = "users",
        });
        resp.PathPart.ShouldBe("users");
        resp.Path.ShouldBe("/users");
        resp.Id.ShouldNotBeNull();
    }

    [Fact]
    public async Task GetResources()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-get-resources" })).Id;
        var rootId = await GetRootResourceIdAsync(apiId);
        await _apigw.CreateResourceAsync(new CreateResourceRequest
        {
            RestApiId = apiId,
            ParentId = rootId,
            PathPart = "items",
        });
        var resources = await _apigw.GetResourcesAsync(new GetResourcesRequest { RestApiId = apiId });
        var paths = resources.Items.Select(r => r.Path).ToList();
        paths.ShouldContain("/");
        paths.ShouldContain("/items");
    }

    [Fact]
    public async Task DeleteResource()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-del-resource" })).Id;
        var rootId = await GetRootResourceIdAsync(apiId);
        var childId = (await _apigw.CreateResourceAsync(new CreateResourceRequest
        {
            RestApiId = apiId,
            ParentId = rootId,
            PathPart = "todel",
        })).Id;
        await _apigw.DeleteResourceAsync(new DeleteResourceRequest
        {
            RestApiId = apiId,
            ResourceId = childId,
        });
        var ex = await Should.ThrowAsync<NotFoundException>(() =>
            _apigw.GetResourceAsync(new GetResourceRequest { RestApiId = apiId, ResourceId = childId }));
        ex.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    // ---- Methods --------------------------------------------------------------

    [Fact]
    public async Task PutGetMethod()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-method-test" })).Id;
        var rootId = await GetRootResourceIdAsync(apiId);
        var resourceId = (await _apigw.CreateResourceAsync(new CreateResourceRequest
        {
            RestApiId = apiId,
            ParentId = rootId,
            PathPart = "ping",
        })).Id;
        await _apigw.PutMethodAsync(new PutMethodRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "GET",
            AuthorizationType = "NONE",
        });
        var resp = await _apigw.GetMethodAsync(new GetMethodRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "GET",
        });
        resp.HttpMethod.ShouldBe("GET");
        resp.AuthorizationType.ShouldBe("NONE");
    }

    [Fact]
    public async Task DeleteMethod()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-del-method" })).Id;
        var rootId = await GetRootResourceIdAsync(apiId);
        await _apigw.PutMethodAsync(new PutMethodRequest
        {
            RestApiId = apiId,
            ResourceId = rootId,
            HttpMethod = "GET",
            AuthorizationType = "NONE",
        });
        await _apigw.DeleteMethodAsync(new DeleteMethodRequest
        {
            RestApiId = apiId,
            ResourceId = rootId,
            HttpMethod = "GET",
        });
        var ex = await Should.ThrowAsync<NotFoundException>(() =>
            _apigw.GetMethodAsync(new GetMethodRequest
            {
                RestApiId = apiId,
                ResourceId = rootId,
                HttpMethod = "GET",
            }));
        ex.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    // ---- Integration ----------------------------------------------------------

    [Fact]
    public async Task PutIntegration()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-integration-test" })).Id;
        var rootId = await GetRootResourceIdAsync(apiId);
        var resourceId = (await _apigw.CreateResourceAsync(new CreateResourceRequest
        {
            RestApiId = apiId,
            ParentId = rootId,
            PathPart = "ping",
        })).Id;
        await _apigw.PutMethodAsync(new PutMethodRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "GET",
            AuthorizationType = "NONE",
        });
        var resp = await _apigw.PutIntegrationAsync(new PutIntegrationRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "GET",
            Type = IntegrationType.AWS_PROXY,
            IntegrationHttpMethod = "POST",
            Uri = "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:myFunc/invocations",
        });
        resp.Type.ShouldBe(IntegrationType.AWS_PROXY);
    }

    [Fact]
    public async Task DeleteIntegration()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-del-integration" })).Id;
        var rootId = await GetRootResourceIdAsync(apiId);
        await _apigw.PutMethodAsync(new PutMethodRequest
        {
            RestApiId = apiId,
            ResourceId = rootId,
            HttpMethod = "GET",
            AuthorizationType = "NONE",
        });
        await _apigw.PutIntegrationAsync(new PutIntegrationRequest
        {
            RestApiId = apiId,
            ResourceId = rootId,
            HttpMethod = "GET",
            Type = IntegrationType.MOCK,
        });
        await _apigw.DeleteIntegrationAsync(new DeleteIntegrationRequest
        {
            RestApiId = apiId,
            ResourceId = rootId,
            HttpMethod = "GET",
        });
        var ex = await Should.ThrowAsync<NotFoundException>(() =>
            _apigw.GetIntegrationAsync(new GetIntegrationRequest
            {
                RestApiId = apiId,
                ResourceId = rootId,
                HttpMethod = "GET",
            }));
        ex.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    // ---- Method Responses -----------------------------------------------------

    [Fact]
    public async Task PutMethodResponse()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-method-response-test" })).Id;
        var rootId = await GetRootResourceIdAsync(apiId);
        var resourceId = (await _apigw.CreateResourceAsync(new CreateResourceRequest
        {
            RestApiId = apiId,
            ParentId = rootId,
            PathPart = "things",
        })).Id;
        await _apigw.PutMethodAsync(new PutMethodRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "GET",
            AuthorizationType = "NONE",
        });
        var resp = await _apigw.PutMethodResponseAsync(new PutMethodResponseRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "GET",
            StatusCode = "200",
        });
        resp.StatusCode.ShouldBe("200");
    }

    [Fact]
    public async Task DeleteMethodResponse()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-del-mresp" })).Id;
        var rootId = await GetRootResourceIdAsync(apiId);
        await _apigw.PutMethodAsync(new PutMethodRequest
        {
            RestApiId = apiId,
            ResourceId = rootId,
            HttpMethod = "GET",
            AuthorizationType = "NONE",
        });
        await _apigw.PutMethodResponseAsync(new PutMethodResponseRequest
        {
            RestApiId = apiId,
            ResourceId = rootId,
            HttpMethod = "GET",
            StatusCode = "200",
        });
        await _apigw.DeleteMethodResponseAsync(new DeleteMethodResponseRequest
        {
            RestApiId = apiId,
            ResourceId = rootId,
            HttpMethod = "GET",
            StatusCode = "200",
        });
        var ex = await Should.ThrowAsync<NotFoundException>(() =>
            _apigw.GetMethodResponseAsync(new GetMethodResponseRequest
            {
                RestApiId = apiId,
                ResourceId = rootId,
                HttpMethod = "GET",
                StatusCode = "200",
            }));
        ex.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    // ---- Integration Responses ------------------------------------------------

    [Fact]
    public async Task PutIntegrationResponse()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-int-response-test" })).Id;
        var rootId = await GetRootResourceIdAsync(apiId);
        var resourceId = (await _apigw.CreateResourceAsync(new CreateResourceRequest
        {
            RestApiId = apiId,
            ParentId = rootId,
            PathPart = "things",
        })).Id;
        await _apigw.PutMethodAsync(new PutMethodRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "GET",
            AuthorizationType = "NONE",
        });
        await _apigw.PutIntegrationAsync(new PutIntegrationRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "GET",
            Type = IntegrationType.MOCK,
            IntegrationHttpMethod = "POST",
            Uri = "",
        });
        var resp = await _apigw.PutIntegrationResponseAsync(new PutIntegrationResponseRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "GET",
            StatusCode = "200",
            SelectionPattern = "",
        });
        resp.StatusCode.ShouldBe("200");
    }

    [Fact]
    public async Task DeleteIntegrationResponse()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-del-iresp" })).Id;
        var rootId = await GetRootResourceIdAsync(apiId);
        await _apigw.PutMethodAsync(new PutMethodRequest
        {
            RestApiId = apiId,
            ResourceId = rootId,
            HttpMethod = "GET",
            AuthorizationType = "NONE",
        });
        await _apigw.PutIntegrationAsync(new PutIntegrationRequest
        {
            RestApiId = apiId,
            ResourceId = rootId,
            HttpMethod = "GET",
            Type = IntegrationType.MOCK,
        });
        await _apigw.PutIntegrationResponseAsync(new PutIntegrationResponseRequest
        {
            RestApiId = apiId,
            ResourceId = rootId,
            HttpMethod = "GET",
            StatusCode = "200",
            SelectionPattern = "",
        });
        await _apigw.DeleteIntegrationResponseAsync(new DeleteIntegrationResponseRequest
        {
            RestApiId = apiId,
            ResourceId = rootId,
            HttpMethod = "GET",
            StatusCode = "200",
        });
        var ex = await Should.ThrowAsync<NotFoundException>(() =>
            _apigw.GetIntegrationResponseAsync(new GetIntegrationResponseRequest
            {
                RestApiId = apiId,
                ResourceId = rootId,
                HttpMethod = "GET",
                StatusCode = "200",
            }));
        ex.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    // ---- Deployments ----------------------------------------------------------

    [Fact]
    public async Task CreateDeployment()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-deployment-test" })).Id;
        var resp = await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest
        {
            RestApiId = apiId,
            Description = "initial deployment",
        });
        resp.Id.ShouldNotBeNull();
        (resp.CreatedDate > DateTime.MinValue).ShouldBe(true);
    }

    [Fact]
    public async Task DeleteDeployment()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-del-deploy" })).Id;
        var depId = (await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest { RestApiId = apiId })).Id;
        await _apigw.DeleteDeploymentAsync(new DeleteDeploymentRequest
        {
            RestApiId = apiId,
            DeploymentId = depId,
        });
        var ex = await Should.ThrowAsync<NotFoundException>(() =>
            _apigw.GetDeploymentAsync(new GetDeploymentRequest { RestApiId = apiId, DeploymentId = depId }));
        ex.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    // ---- Stages ---------------------------------------------------------------

    [Fact]
    public async Task CreateStage()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-stage-test" })).Id;
        var depId = (await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest { RestApiId = apiId })).Id;
        var resp = await _apigw.CreateStageAsync(new CreateStageRequest
        {
            RestApiId = apiId,
            StageName = "prod",
            DeploymentId = depId,
        });
        resp.StageName.ShouldBe("prod");
        resp.DeploymentId.ShouldBe(depId);
    }

    [Fact]
    public async Task UpdateStage()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-stage-update" })).Id;
        var depId = (await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest { RestApiId = apiId })).Id;
        await _apigw.CreateStageAsync(new CreateStageRequest
        {
            RestApiId = apiId,
            StageName = "dev",
            DeploymentId = depId,
        });
        await _apigw.UpdateStageAsync(new UpdateStageRequest
        {
            RestApiId = apiId,
            StageName = "dev",
            PatchOperations = [new PatchOperation { Op = Op.Replace, Path = "/variables/myVar", Value = "myVal" }],
        });
        var resp = await _apigw.GetStageAsync(new GetStageRequest
        {
            RestApiId = apiId,
            StageName = "dev",
        });
        resp.Variables["myVar"].ShouldBe("myVal");
    }

    [Fact]
    public async Task DeleteStage()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-del-stage" })).Id;
        var depId = (await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest { RestApiId = apiId })).Id;
        await _apigw.CreateStageAsync(new CreateStageRequest
        {
            RestApiId = apiId,
            StageName = "todel",
            DeploymentId = depId,
        });
        await _apigw.DeleteStageAsync(new DeleteStageRequest
        {
            RestApiId = apiId,
            StageName = "todel",
        });
        var ex = await Should.ThrowAsync<NotFoundException>(() =>
            _apigw.GetStageAsync(new GetStageRequest { RestApiId = apiId, StageName = "todel" }));
        ex.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    // ---- Authorizers ----------------------------------------------------------

    [Fact]
    public async Task AuthorizerCrud()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-auth-crud" })).Id;
        var auth = await _apigw.CreateAuthorizerAsync(new CreateAuthorizerRequest
        {
            RestApiId = apiId,
            Name = "my-auth",
            Type = AuthorizerType.TOKEN,
            AuthorizerUri = "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:auth/invocations",
            IdentitySource = "method.request.header.Authorization",
        });
        auth.Name.ShouldBe("my-auth");

        var got = await _apigw.GetAuthorizerAsync(new GetAuthorizerRequest
        {
            RestApiId = apiId,
            AuthorizerId = auth.Id,
        });
        got.Id.ShouldBe(auth.Id);

        await _apigw.UpdateAuthorizerAsync(new UpdateAuthorizerRequest
        {
            RestApiId = apiId,
            AuthorizerId = auth.Id,
            PatchOperations = [new PatchOperation { Op = Op.Replace, Path = "/name", Value = "renamed-auth" }],
        });
        var got2 = await _apigw.GetAuthorizerAsync(new GetAuthorizerRequest
        {
            RestApiId = apiId,
            AuthorizerId = auth.Id,
        });
        got2.Name.ShouldBe("renamed-auth");

        var listed = await _apigw.GetAuthorizersAsync(new GetAuthorizersRequest { RestApiId = apiId });
        listed.Items.ShouldContain(a => a.Id == auth.Id);

        await _apigw.DeleteAuthorizerAsync(new DeleteAuthorizerRequest
        {
            RestApiId = apiId,
            AuthorizerId = auth.Id,
        });
        var ex = await Should.ThrowAsync<NotFoundException>(() =>
            _apigw.GetAuthorizerAsync(new GetAuthorizerRequest { RestApiId = apiId, AuthorizerId = auth.Id }));
        ex.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    // ---- Models ---------------------------------------------------------------

    [Fact]
    public async Task ModelCrud()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-model-crud" })).Id;
        var resp = await _apigw.CreateModelAsync(new CreateModelRequest
        {
            RestApiId = apiId,
            Name = "MyModel",
            ContentType = "application/json",
            Schema = "{\"type\": \"object\"}",
        });
        resp.Name.ShouldBe("MyModel");

        var got = await _apigw.GetModelAsync(new GetModelRequest { RestApiId = apiId, ModelName = "MyModel" });
        got.Name.ShouldBe("MyModel");

        var listed = await _apigw.GetModelsAsync(new GetModelsRequest { RestApiId = apiId });
        listed.Items.ShouldContain(m => m.Name == "MyModel");

        await _apigw.DeleteModelAsync(new DeleteModelRequest { RestApiId = apiId, ModelName = "MyModel" });
        var ex = await Should.ThrowAsync<NotFoundException>(() =>
            _apigw.GetModelAsync(new GetModelRequest { RestApiId = apiId, ModelName = "MyModel" }));
        ex.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    // ---- API Keys -------------------------------------------------------------

    [Fact]
    public async Task ApiKeyCrud()
    {
        var resp = await _apigw.CreateApiKeyAsync(new CreateApiKeyRequest
        {
            Name = "v1-test-key",
            Enabled = true,
        });
        var keyId = resp.Id;
        resp.Name.ShouldBe("v1-test-key");
        resp.Value.ShouldNotBeNull();

        var got = await _apigw.GetApiKeyAsync(new GetApiKeyRequest { ApiKey = keyId, IncludeValue = true });
        got.Id.ShouldBe(keyId);

        var listed = await _apigw.GetApiKeysAsync(new GetApiKeysRequest());
        listed.Items.ShouldContain(k => k.Id == keyId);

        await _apigw.DeleteApiKeyAsync(new DeleteApiKeyRequest { ApiKey = keyId });
        var ex = await Should.ThrowAsync<NotFoundException>(() =>
            _apigw.GetApiKeyAsync(new GetApiKeyRequest { ApiKey = keyId }));
        ex.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task UpdateApiKey()
    {
        var keyId = (await _apigw.CreateApiKeyAsync(new CreateApiKeyRequest { Name = "v1-key-update-before" })).Id;
        var resp = await _apigw.UpdateApiKeyAsync(new UpdateApiKeyRequest
        {
            ApiKey = keyId,
            PatchOperations = [new PatchOperation { Op = Op.Replace, Path = "/name", Value = "v1-key-update-after" }],
        });
        resp.Name.ShouldBe("v1-key-update-after");
        (resp.LastUpdatedDate > DateTime.MinValue).ShouldBe(true);
    }

    // ---- Usage Plans ----------------------------------------------------------

    [Fact]
    public async Task UsagePlanCrud()
    {
        var resp = await _apigw.CreateUsagePlanAsync(new CreateUsagePlanRequest
        {
            Name = "v1-plan",
            Throttle = new ThrottleSettings { RateLimit = 100, BurstLimit = 200 },
            Quota = new QuotaSettings { Limit = 10000, Period = QuotaPeriodType.MONTH },
        });
        var planId = resp.Id;
        resp.Name.ShouldBe("v1-plan");

        var got = await _apigw.GetUsagePlanAsync(new GetUsagePlanRequest { UsagePlanId = planId });
        got.Id.ShouldBe(planId);

        var listed = await _apigw.GetUsagePlansAsync(new GetUsagePlansRequest());
        listed.Items.ShouldContain(p => p.Id == planId);

        await _apigw.DeleteUsagePlanAsync(new DeleteUsagePlanRequest { UsagePlanId = planId });
        var ex = await Should.ThrowAsync<NotFoundException>(() =>
            _apigw.GetUsagePlanAsync(new GetUsagePlanRequest { UsagePlanId = planId }));
        ex.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task UpdateUsagePlan()
    {
        var planId = (await _apigw.CreateUsagePlanAsync(new CreateUsagePlanRequest { Name = "v1-plan-update-before" })).Id;
        var resp = await _apigw.UpdateUsagePlanAsync(new UpdateUsagePlanRequest
        {
            UsagePlanId = planId,
            PatchOperations = [new PatchOperation { Op = Op.Replace, Path = "/name", Value = "v1-plan-update-after" }],
        });
        resp.Name.ShouldBe("v1-plan-update-after");
    }

    [Fact]
    public async Task UsagePlanKeyCrud()
    {
        var apiKey = await _apigw.CreateApiKeyAsync(new CreateApiKeyRequest { Name = "qa-v1-key", Enabled = true });
        var keyId = apiKey.Id;
        var plan = await _apigw.CreateUsagePlanAsync(new CreateUsagePlanRequest
        {
            Name = "qa-v1-plan",
            Throttle = new ThrottleSettings { RateLimit = 100, BurstLimit = 200 },
        });
        var planId = plan.Id;
        await _apigw.CreateUsagePlanKeyAsync(new CreateUsagePlanKeyRequest
        {
            UsagePlanId = planId,
            KeyId = keyId,
            KeyType = "API_KEY",
        });
        var keys = await _apigw.GetUsagePlanKeysAsync(new GetUsagePlanKeysRequest { UsagePlanId = planId });
        keys.Items.ShouldContain(k => k.Id == keyId);
        await _apigw.DeleteUsagePlanKeyAsync(new DeleteUsagePlanKeyRequest
        {
            UsagePlanId = planId,
            KeyId = keyId,
        });
        var keys2 = await _apigw.GetUsagePlanKeysAsync(new GetUsagePlanKeysRequest { UsagePlanId = planId });
        keys2.Items.ShouldNotContain(k => k.Id == keyId);
    }

    // ---- Domain Names ---------------------------------------------------------

    [Fact]
    public async Task DomainNameCrud()
    {
        var resp = await _apigw.CreateDomainNameAsync(new CreateDomainNameRequest
        {
            DomainName = "api.example.com",
            EndpointConfiguration = new EndpointConfiguration { Types = ["REGIONAL"] },
        });
        resp.Name.ShouldBe("api.example.com");

        var got = await _apigw.GetDomainNameAsync(new GetDomainNameRequest { DomainName = "api.example.com" });
        got.Name.ShouldBe("api.example.com");

        var listed = await _apigw.GetDomainNamesAsync(new GetDomainNamesRequest());
        listed.Items.ShouldContain(d => d.Name == "api.example.com");

        await _apigw.DeleteDomainNameAsync(new DeleteDomainNameRequest { DomainName = "api.example.com" });
        var ex = await Should.ThrowAsync<NotFoundException>(() =>
            _apigw.GetDomainNameAsync(new GetDomainNameRequest { DomainName = "api.example.com" }));
        ex.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    // ---- Base Path Mappings ---------------------------------------------------

    [Fact]
    public async Task BasePathMappingCrud()
    {
        await _apigw.CreateDomainNameAsync(new CreateDomainNameRequest { DomainName = "bpm.example.com" });
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-bpm-api" })).Id;
        var depId = (await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest { RestApiId = apiId })).Id;
        await _apigw.CreateStageAsync(new CreateStageRequest
        {
            RestApiId = apiId,
            StageName = "prod",
            DeploymentId = depId,
        });

        var mapping = await _apigw.CreateBasePathMappingAsync(new CreateBasePathMappingRequest
        {
            DomainName = "bpm.example.com",
            BasePath = "v1",
            RestApiId = apiId,
            Stage = "prod",
        });
        mapping.BasePath.ShouldBe("v1");
        mapping.RestApiId.ShouldBe(apiId);

        var got = await _apigw.GetBasePathMappingAsync(new GetBasePathMappingRequest
        {
            DomainName = "bpm.example.com",
            BasePath = "v1",
        });
        got.BasePath.ShouldBe("v1");

        var listed = await _apigw.GetBasePathMappingsAsync(new GetBasePathMappingsRequest
        {
            DomainName = "bpm.example.com",
        });
        listed.Items.ShouldContain(m => m.BasePath == "v1");

        await _apigw.DeleteBasePathMappingAsync(new DeleteBasePathMappingRequest
        {
            DomainName = "bpm.example.com",
            BasePath = "v1",
        });
    }

    // ---- Tags -----------------------------------------------------------------

    [Fact]
    public async Task Tags()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-tags-test" })).Id;
        var arn = $"arn:aws:apigateway:us-east-1::/restapis/{apiId}";

        await _apigw.TagResourceAsync(new Amazon.APIGateway.Model.TagResourceRequest
        {
            ResourceArn = arn,
            Tags = new Dictionary<string, string> { ["env"] = "test", ["team"] = "platform" },
        });
        var resp = await _apigw.GetTagsAsync(new GetTagsRequest { ResourceArn = arn });
        resp.Tags["env"].ShouldBe("test");
        resp.Tags["team"].ShouldBe("platform");

        await _apigw.UntagResourceAsync(new Amazon.APIGateway.Model.UntagResourceRequest
        {
            ResourceArn = arn,
            TagKeys = ["env"],
        });
        var resp2 = await _apigw.GetTagsAsync(new GetTagsRequest { ResourceArn = arn });
        resp2.Tags.Keys.ShouldNotContain("env");
        resp2.Tags["team"].ShouldBe("platform");
    }

    // ---- Data plane: MOCK integration -----------------------------------------

    [Fact]
    public async Task ExecuteMockIntegration()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-mock-test" })).Id;
        var rootId = await GetRootResourceIdAsync(apiId);
        var resourceId = (await _apigw.CreateResourceAsync(new CreateResourceRequest
        {
            RestApiId = apiId,
            ParentId = rootId,
            PathPart = "mock",
        })).Id;
        await _apigw.PutMethodAsync(new PutMethodRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "GET",
            AuthorizationType = "NONE",
        });
        await _apigw.PutIntegrationAsync(new PutIntegrationRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "GET",
            Type = IntegrationType.MOCK,
            IntegrationHttpMethod = "GET",
            Uri = "",
            RequestTemplates = new Dictionary<string, string> { ["application/json"] = "{\"statusCode\": 200}" },
        });
        await _apigw.PutMethodResponseAsync(new PutMethodResponseRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "GET",
            StatusCode = "200",
        });
        await _apigw.PutIntegrationResponseAsync(new PutIntegrationResponseRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "GET",
            StatusCode = "200",
            SelectionPattern = "",
            ResponseTemplates = new Dictionary<string, string> { ["application/json"] = "{\"mocked\": true}" },
        });
        var depId = (await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest { RestApiId = apiId })).Id;
        await _apigw.CreateStageAsync(new CreateStageRequest
        {
            RestApiId = apiId,
            StageName = "test",
            DeploymentId = depId,
        });

        var resp = await ExecuteApiAsync(apiId, "test", "/mock", HttpMethod.Get);
        resp.StatusCode.ShouldBe(HttpStatusCode.OK);
        var body = await resp.Content.ReadAsStringAsync();
        body.ShouldContain("\"mocked\"");
    }

    // ---- Data plane: 404 for missing resource ---------------------------------

    [Fact]
    public async Task ExecuteMissingResource404()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-missing-resource" })).Id;
        var depId = (await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest { RestApiId = apiId })).Id;
        await _apigw.CreateStageAsync(new CreateStageRequest
        {
            RestApiId = apiId,
            StageName = "test",
            DeploymentId = depId,
        });

        var resp = await ExecuteApiAsync(apiId, "test", "/nonexistent", HttpMethod.Get);
        resp.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    // ---- Data plane: 404 for missing stage ------------------------------------

    [Fact]
    public async Task ExecuteMissingStage404()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-no-stage" })).Id;
        await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest { RestApiId = apiId });

        var resp = await ExecuteApiAsync(apiId, "nonexistent", "/", HttpMethod.Get);
        resp.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    // ---- Data plane: 405 for missing method -----------------------------------

    [Fact]
    public async Task ExecuteMissingMethod405()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-no-method" })).Id;
        var rootId = await GetRootResourceIdAsync(apiId);
        var resourceId = (await _apigw.CreateResourceAsync(new CreateResourceRequest
        {
            RestApiId = apiId,
            ParentId = rootId,
            PathPart = "noop",
        })).Id;
        // PUT method for POST only — GET not configured
        await _apigw.PutMethodAsync(new PutMethodRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "POST",
            AuthorizationType = "NONE",
        });
        await _apigw.PutIntegrationAsync(new PutIntegrationRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "POST",
            Type = IntegrationType.MOCK,
        });
        var depId = (await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest { RestApiId = apiId })).Id;
        await _apigw.CreateStageAsync(new CreateStageRequest
        {
            RestApiId = apiId,
            StageName = "test",
            DeploymentId = depId,
        });

        var resp = await ExecuteApiAsync(apiId, "test", "/noop", HttpMethod.Get);
        resp.StatusCode.ShouldBe(HttpStatusCode.MethodNotAllowed);
    }

    // ---- Data plane: Lambda proxy integration ---------------------------------

    [Fact]
    public async Task ExecuteLambdaProxy()
    {
        var fname = $"intg-v1-proxy-{Guid.NewGuid():N}"[..24];
        var code = CreatePythonZip("import json\ndef handler(event, context):\n    return {'statusCode': 200, 'body': 'pong'}\n");
        await _lambda.CreateFunctionAsync(new CreateFunctionRequest
        {
            FunctionName = fname,
            Runtime = Runtime.Python39,
            Role = LambdaRole,
            Handler = "index.handler",
            Code = new FunctionCode { ZipFile = new MemoryStream(code) },
        });

        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = $"v1-exec-{fname}" })).Id;
        var rootId = await GetRootResourceIdAsync(apiId);
        var resourceId = (await _apigw.CreateResourceAsync(new CreateResourceRequest
        {
            RestApiId = apiId,
            ParentId = rootId,
            PathPart = "ping",
        })).Id;
        await _apigw.PutMethodAsync(new PutMethodRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "GET",
            AuthorizationType = "NONE",
        });
        await _apigw.PutIntegrationAsync(new PutIntegrationRequest
        {
            RestApiId = apiId,
            ResourceId = resourceId,
            HttpMethod = "GET",
            Type = IntegrationType.AWS_PROXY,
            IntegrationHttpMethod = "POST",
            Uri = $"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
        });
        var depId = (await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest { RestApiId = apiId })).Id;
        await _apigw.CreateStageAsync(new CreateStageRequest
        {
            RestApiId = apiId,
            StageName = "test",
            DeploymentId = depId,
        });

        var resp = await ExecuteApiAsync(apiId, "test", "/ping", HttpMethod.Get);
        resp.StatusCode.ShouldBe(HttpStatusCode.OK);
        var body = await resp.Content.ReadAsStringAsync();
        body.ShouldBe("pong");
    }

    // ---- Data plane: Lambda with path parameters ------------------------------

    [Fact]
    public async Task ExecuteLambdaPathParams()
    {
        var fname = $"intg-v1-params-{Guid.NewGuid():N}"[..24];
        var code = CreatePythonZip(
            "import json\n" +
            "def handler(event, context):\n" +
            "    uid = (event.get('pathParameters') or {}).get('userId', 'missing')\n" +
            "    return {'statusCode': 200, 'body': uid}\n");
        await _lambda.CreateFunctionAsync(new CreateFunctionRequest
        {
            FunctionName = fname,
            Runtime = Runtime.Python39,
            Role = LambdaRole,
            Handler = "index.handler",
            Code = new FunctionCode { ZipFile = new MemoryStream(code) },
        });

        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = $"v1-params-{fname}" })).Id;
        var rootId = await GetRootResourceIdAsync(apiId);
        var usersId = (await _apigw.CreateResourceAsync(new CreateResourceRequest
        {
            RestApiId = apiId,
            ParentId = rootId,
            PathPart = "users",
        })).Id;
        var userIdRes = (await _apigw.CreateResourceAsync(new CreateResourceRequest
        {
            RestApiId = apiId,
            ParentId = usersId,
            PathPart = "{userId}",
        })).Id;
        await _apigw.PutMethodAsync(new PutMethodRequest
        {
            RestApiId = apiId,
            ResourceId = userIdRes,
            HttpMethod = "GET",
            AuthorizationType = "NONE",
        });
        await _apigw.PutIntegrationAsync(new PutIntegrationRequest
        {
            RestApiId = apiId,
            ResourceId = userIdRes,
            HttpMethod = "GET",
            Type = IntegrationType.AWS_PROXY,
            IntegrationHttpMethod = "POST",
            Uri = $"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
        });
        var depId = (await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest { RestApiId = apiId })).Id;
        await _apigw.CreateStageAsync(new CreateStageRequest
        {
            RestApiId = apiId,
            StageName = "v1",
            DeploymentId = depId,
        });

        var resp = await ExecuteApiAsync(apiId, "v1", "/users/alice123", HttpMethod.Get);
        resp.StatusCode.ShouldBe(HttpStatusCode.OK);
        var body = await resp.Content.ReadAsStringAsync();
        body.ShouldBe("alice123");
    }

    // ---- CreatedDate is Unix timestamp (datetime) -----------------------------

    [Fact]
    public async Task CreatedDateIsUnixTimestamp()
    {
        var resp = await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "tf-date-test" });
        (resp.CreatedDate > new DateTime(2024, 1, 1)).ShouldBe(true, "createdDate should be a recent date");
    }

    // ---- Deployment apiSummary ------------------------------------------------

    [Fact]
    public async Task DeploymentApiSummary()
    {
        var apiId = (await _apigw.CreateRestApiAsync(new CreateRestApiRequest { Name = "v1-api-summary" })).Id;
        var rootId = await GetRootResourceIdAsync(apiId);
        await _apigw.PutMethodAsync(new PutMethodRequest
        {
            RestApiId = apiId,
            ResourceId = rootId,
            HttpMethod = "GET",
            AuthorizationType = "NONE",
        });
        await _apigw.PutIntegrationAsync(new PutIntegrationRequest
        {
            RestApiId = apiId,
            ResourceId = rootId,
            HttpMethod = "GET",
            Type = IntegrationType.MOCK,
        });
        var dep = await _apigw.CreateDeploymentAsync(new CreateDeploymentRequest { RestApiId = apiId });
        dep.ApiSummary.ShouldNotBeNull();
        dep.ApiSummary.ContainsKey("/").ShouldBe(true, "apiSummary must include root resource path");
        dep.ApiSummary["/"].ContainsKey("GET").ShouldBe(true, "apiSummary must include configured HTTP method");
    }
}
