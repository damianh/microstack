using System.Net.Http.Json;
using System.Text.Json;
using Amazon;
using Amazon.APIGateway;
using Amazon.APIGateway.Model;
using Amazon.ElasticLoadBalancingV2;
using Amazon.ElasticLoadBalancingV2.Model;
using Amazon.RDS;
using Amazon.RDS.Model;
using Amazon.Runtime;
using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Internal.Admin;
using Microsoft.Extensions.DependencyInjection;

namespace MicroStack.Tests;

public sealed class AdminRootKindMetadataTests(MicroStackFixture fixture)
    : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private static readonly Dictionary<string, string[]> ExpectedRoots = new(StringComparer.Ordinal)
    {
        ["ses"] = ["identities", "templates", "configuration-sets", "emails"],
        ["stepfunctions"] = ["state-machines", "activities"],
        ["s3files"] = ["file-systems"],
        ["efs"] = ["file-systems"],
        ["rds"] =
        [
            "db-instances", "db-clusters", "db-subnet-groups", "db-parameter-groups",
            "db-cluster-parameter-groups", "db-snapshots", "db-cluster-snapshots",
            "event-subscriptions", "db-proxies", "option-groups", "global-clusters",
        ],
        ["rdsdata"] = [],
        ["elasticache"] =
        [
            "cache-clusters", "replication-groups", "cache-subnet-groups", "cache-parameter-groups",
            "snapshots", "users", "user-groups",
        ],
        ["lambda"] = ["functions", "layers", "event-source-mappings"],
        ["ec2"] =
        [
            "instances", "security-groups", "key-pairs", "vpcs", "subnets", "internet-gateways",
            "elastic-ips", "route-tables", "network-interfaces", "vpc-endpoints", "volumes",
            "snapshots", "nat-gateways", "network-acls", "flow-logs", "vpc-peerings",
            "dhcp-options", "egress-internet-gateways", "prefix-lists", "vpn-gateways",
            "customer-gateways", "launch-templates",
        ],
        ["ecs"] = ["clusters", "task-definitions"],
        ["ecr"] = ["repositories"],
        ["apigateway"] = ["rest-apis", "api-keys", "usage-plans", "domain-names"],
        ["apigatewayv2"] = ["apis"],
        ["alb"] = ["load-balancers", "target-groups"],
        ["appsync"] = ["graphql-apis"],
        ["cloudfront"] = ["distributions"],
        ["route53"] = ["hosted-zones"],
        ["servicediscovery"] = ["namespaces"],
        ["acm"] = ["certificate"],
        ["cognitoidp"] = ["user-pool"],
        ["cognitoidentity"] = ["identity-pool"],
        ["iam"] = ["user", "role", "policy", "group", "instance-profile", "oidc-provider"],
        ["kms"] = ["key", "alias"],
        ["secretsmanager"] = ["secret"],
        ["sts"] = [],
        ["waf"] = ["web-acl", "ip-set", "rule-group"],
        ["athena"] = ["query-execution", "workgroup", "named-query", "data-catalog", "prepared-statement"],
        ["emr"] = ["cluster"],
        ["firehose"] = ["delivery-stream"],
        ["glue"] = ["database", "crawler", "job", "registry"],
        ["kinesis"] = ["stream"],
        ["cloudformation"] = ["stack"],
        ["cloudwatch"] = ["metric", "alarm", "dashboard"],
        ["logs"] = ["log-group", "destination"],
        ["ssm"] = ["parameter"],
    };

    public async ValueTask InitializeAsync()
    {
        using var response = await fixture.HttpClient.PostAsync("/_microstack/reset", null);
        response.EnsureSuccessStatusCode();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    [Fact]
    public async Task EveryOtherCatalogServiceDeclaresItsActualRootKindsWithoutInferringFromData()
    {
        var services = await fixture.HttpClient.GetFromJsonAsync(
            "/_microstack/admin/v1/services", AdminJsonContext.Default.AdminServiceArray);
        services.ShouldNotBeNull();
        var covered = services.Where(service =>
            service.Id is not ("sqs" or "sns" or "events" or "s3" or "dynamodb")).ToArray();
        covered.Select(service => service.Id).Order().ShouldBe(ExpectedRoots.Keys.Order());

        foreach (var service in covered)
        {
            service.Kinds.Select(kind => kind.Id).ShouldBeUnique();
            service.Kinds.ShouldAllBe(kind => !string.IsNullOrWhiteSpace(kind.Label));
            service.Kinds.Where(kind => kind.IsRoot).Select(kind => kind.Id).Order()
                .ShouldBe(ExpectedRoots[service.Id].Order(), service.Id);

            var roots = await Resources(service.Id);
            roots.Items.ShouldAllBe(resource => ExpectedRoots[service.Id].Contains(resource.Key.Kind));
            foreach (var root in roots.Items)
            {
                var detail = await Detail(service.Id, root.Key);
                if (detail.HasChildren)
                    detail.ChildKinds.ShouldNotBeEmpty(service.Id + ":" + root.Key.Kind);
            }
        }
    }

    [Fact]
    public async Task AlbCatalogIdRetainsBothRootsAndStableEmptyListenerRuleAndTargetCollections()
    {
        using var client = Client<AmazonElasticLoadBalancingV2Client, AmazonElasticLoadBalancingV2Config>(
            config => new(new BasicAWSCredentials("test", "test"), config));
        var lb = await client.CreateLoadBalancerAsync(new CreateLoadBalancerRequest { Name = "metadata-lb" });
        var tg = await client.CreateTargetGroupAsync(new CreateTargetGroupRequest
        {
            Name = "metadata-tg", Port = 80, Protocol = ProtocolEnum.HTTP,
        });
        var lbArn = lb.LoadBalancers.Single().LoadBalancerArn;
        var tgArn = tg.TargetGroups.Single().TargetGroupArn;
        var lbKey = new AdminKey("load-balancers", lbArn);
        var tgKey = new AdminKey("target-groups", tgArn);

        (await Resources("alb")).Items.Select(item => item.Key.Kind).Order()
            .ShouldBe(new[] { "load-balancers", "target-groups" });
        AssertChildKinds(await Detail("alb", lbKey), "listeners");
        (await Children("alb", lbKey)).Items.ShouldBeEmpty();
        AssertChildKinds(await Detail("alb", tgKey), "targets");
        (await Children("alb", tgKey)).Items.ShouldBeEmpty();

        await client.CreateListenerAsync(new CreateListenerRequest
        {
            LoadBalancerArn = lbArn, Port = 80, Protocol = ProtocolEnum.HTTP,
            DefaultActions = [new() { Type = ActionTypeEnum.Forward, TargetGroupArn = tgArn }],
        });
        var listener = (await Children("alb", lbKey)).Items.Single();
        AssertChildKinds(await Detail("alb", lbKey, listener.Key), "rules");
        await client.RegisterTargetsAsync(new RegisterTargetsRequest
        {
            TargetGroupArn = tgArn, Targets = [new() { Id = "i-metadata" }],
        });
        (await Children("alb", tgKey)).Items.Single().Key.Kind.ShouldBe("targets");
        AssertChildKinds(await Detail("alb", tgKey), "targets");
    }

    [Fact]
    public async Task RestApiKeepsAdditionalRootsAndEmptyNestedMethodCapabilities()
    {
        using var client = Client<AmazonAPIGatewayClient, AmazonAPIGatewayConfig>(
            config => new(new BasicAWSCredentials("test", "test"), config));
        var api = await client.CreateRestApiAsync(new CreateRestApiRequest { Name = "metadata-api" });
        await client.CreateApiKeyAsync(new CreateApiKeyRequest { Name = "metadata-key" });
        var plan = await client.CreateUsagePlanAsync(new CreateUsagePlanRequest { Name = "metadata-plan" });
        var apiKey = new AdminKey("rest-apis", api.Id);
        var planKey = new AdminKey("usage-plans", plan.Id);
        (await Resources("apigateway")).Items.Select(item => item.Key.Kind).Order()
            .ShouldBe(new[] { "api-keys", "rest-apis", "usage-plans" });
        AssertChildKinds(await Detail("apigateway", apiKey),
            "resources", "stages", "deployments", "authorizers", "models");
        AssertChildKinds(await Detail("apigateway", planKey), "usage-plan-keys");
        (await Children("apigateway", planKey)).Items.ShouldBeEmpty();

        var resources = await client.GetResourcesAsync(new GetResourcesRequest { RestApiId = api.Id });
        var rootId = resources.Items.Single(item => item.Path == "/").Id;
        var resource = await client.CreateResourceAsync(new CreateResourceRequest
        {
            RestApiId = api.Id, ParentId = rootId, PathPart = "items",
        });
        var resourceKey = new AdminKey("resources", resource.Id);
        AssertChildKinds(await Detail("apigateway", apiKey, resourceKey), "methods");
        (await Children("apigateway", apiKey, resourceKey)).Items.ShouldBeEmpty();

        await client.PutMethodAsync(new PutMethodRequest
        {
            RestApiId = api.Id, ResourceId = resource.Id, HttpMethod = "GET", AuthorizationType = "NONE",
        });
        var methodKey = new AdminKey("methods", "GET");
        AssertChildKinds(await Detail("apigateway", apiKey, resourceKey, methodKey),
            "method-responses", "integrations");
        (await Children("apigateway", apiKey, resourceKey, methodKey)).Items.ShouldBeEmpty();
        await client.PutIntegrationAsync(new PutIntegrationRequest
        {
            RestApiId = api.Id, ResourceId = resource.Id, HttpMethod = "GET", Type = IntegrationType.MOCK,
        });
        var integration = (await Children("apigateway", apiKey, resourceKey, methodKey)).Items.Single();
        AssertChildKinds(await Detail("apigateway", apiKey, resourceKey, methodKey, integration.Key),
            "integration-responses");
    }

    [Fact]
    public async Task RdsRetainsIndependentInfrastructureRootsAndStableTagMetadata()
    {
        using var client = Client<AmazonRDSClient, AmazonRDSConfig>(
            config => new(new BasicAWSCredentials("test", "test"), config));
        var instance = await client.CreateDBInstanceAsync(new CreateDBInstanceRequest
        {
            DBInstanceIdentifier = "metadata-instance", DBInstanceClass = "db.t3.micro",
            Engine = "postgres", MasterUsername = "admin", MasterUserPassword = "test-password",
            AllocatedStorage = 20,
            MonitoringRoleArn = "arn:aws:iam::000000000000:role/metadata-monitoring",
        });
        await client.CreateDBClusterAsync(new CreateDBClusterRequest
        {
            DBClusterIdentifier = "metadata-cluster", Engine = "aurora-postgresql",
            MasterUsername = "admin", MasterUserPassword = "test-password",
        });
        await client.CreateDBSubnetGroupAsync(new CreateDBSubnetGroupRequest
        {
            DBSubnetGroupName = "metadata-subnets", DBSubnetGroupDescription = "Metadata test",
            SubnetIds = ["subnet-a", "subnet-b"],
        });

        (await Resources("rds")).Items.Select(item => item.Key.Kind).Order()
            .ShouldBe(new[] { "db-clusters", "db-instances", "db-subnet-groups" });
        var key = new AdminKey("db-instances", "metadata-instance");
        var detail = await Detail("rds", key);
        detail.Resource.Arn.ShouldBe(instance.DBInstance.DBInstanceArn);
        AssertChildKinds(detail, "tags");
        (await Children("rds", key)).Items.ShouldBeEmpty();
        await client.AddTagsToResourceAsync(new AddTagsToResourceRequest
        {
            ResourceName = instance.DBInstance.DBInstanceArn,
            Tags = [new Amazon.RDS.Model.Tag { Key = "owner", Value = "metadata" }],
        });
        var retainedTags = await client.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceName = instance.DBInstance.DBInstanceArn,
        });
        retainedTags.TagList.ShouldContain(tag => tag.Key == "owner" && tag.Value == "metadata");
        AssertChildKinds(await Detail("rds", key), "tags");
        (await Children("rds", key)).Items.Single().Key.ShouldBe(new AdminKey("tags", "owner"));
    }

    [Fact]
    public async Task WorkflowAndFunctionRelationshipSnapshotsRetainOnlyAccountScopedDestinationIdentity()
    {
        using var lambda = Client<Amazon.Lambda.AmazonLambdaClient, Amazon.Lambda.AmazonLambdaConfig>(
            config => new(new BasicAWSCredentials("test", "test"), config));
        using var states = Client<Amazon.StepFunctions.AmazonStepFunctionsClient, Amazon.StepFunctions.AmazonStepFunctionsConfig>(
            config => new(new BasicAWSCredentials("test", "test"), config));
        using var code = new MemoryStream();
        var function = await lambda.CreateFunctionAsync(new Amazon.Lambda.Model.CreateFunctionRequest
        {
            FunctionName = "metadata-function", Runtime = Amazon.Lambda.Runtime.Dotnet8,
            Role = "arn:aws:iam::000000000000:role/metadata", Handler = "Example::Handler",
            Code = new() { ZipFile = code },
        });
        var machine = await states.CreateStateMachineAsync(new Amazon.StepFunctions.Model.CreateStateMachineRequest
        {
            Name = "metadata-machine", RoleArn = "arn:aws:iam::000000000000:role/metadata",
            Definition = """{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}""",
        });
        var activity = await states.CreateActivityAsync(new Amazon.StepFunctions.Model.CreateActivityRequest
        {
            Name = "metadata-activity",
        });
        var context = await fixture.HttpClient.GetFromJsonAsync(
            "/_microstack/admin/v1/context", AdminJsonContext.Default.AdminContext);
        context.ShouldNotBeNull();
        var registry = fixture.Factory.Services.GetRequiredService<ServiceRegistry>();
        var lambdaSource = (IAdminRelationshipSource)registry.Resolve("lambda")!;
        var statesSource = (IAdminRelationshipSource)registry.Resolve("states")!;
        using (AccountContext.BeginScope(context.DefaultAccount))
        {
            var functionSnapshot = lambdaSource.GetAdminRelationshipSnapshot();
            functionSnapshot.Relationships.ShouldBeEmpty();
            var functionResource = functionSnapshot.Resources.Single();
            functionResource.Path.ShouldBe([new AdminKey("functions", "metadata-function")]);
            functionResource.Name.ShouldBe("metadata-function");
            functionResource.Arn.ShouldBe(function.FunctionArn);

            var workflowSnapshot = statesSource.GetAdminRelationshipSnapshot();
            workflowSnapshot.Relationships.ShouldBeEmpty();
            workflowSnapshot.Resources.Count.ShouldBe(2);
            workflowSnapshot.Resources.Single(item => item.Name == "metadata-machine").Path
                .ShouldBe([new AdminKey("state-machines", machine.StateMachineArn)]);
            workflowSnapshot.Resources.Single(item => item.Name == "metadata-activity").Path
                .ShouldBe([new AdminKey("activities", activity.ActivityArn)]);
        }
        var otherAccount = context.DefaultAccount == "111111111111" ? "222222222222" : "111111111111";
        using (AccountContext.BeginScope(otherAccount))
        {
            lambdaSource.GetAdminRelationshipSnapshot().Resources.ShouldBeEmpty();
            statesSource.GetAdminRelationshipSnapshot().Resources.ShouldBeEmpty();
        }
    }

    private static void AssertChildKinds(AdminResourceDetail detail, params string[] expected)
    {
        detail.HasChildren.ShouldBeTrue();
        detail.ChildKinds.Select(kind => kind.Id).ShouldBe(expected);
        detail.ChildKinds.ShouldAllBe(kind => !kind.IsRoot && !string.IsNullOrWhiteSpace(kind.Label));
    }

    private async Task<AdminPage<AdminResourceSummary>> Resources(string service) =>
        (await fixture.HttpClient.GetFromJsonAsync(
            $"/_microstack/admin/v1/services/{service}/resources?pageSize=200",
            AdminJsonContext.Default.AdminPageAdminResourceSummary))!;

    private async Task<AdminResourceDetail> Detail(string service, params AdminKey[] path) =>
        (await fixture.HttpClient.GetFromJsonAsync(Url(service, "resource", path),
            AdminJsonContext.Default.AdminResourceDetail))!;

    private async Task<AdminPage<AdminResourceSummary>> Children(string service, params AdminKey[] path) =>
        (await fixture.HttpClient.GetFromJsonAsync(Url(service, "children", path),
            AdminJsonContext.Default.AdminPageAdminResourceSummary))!;

    private static string Url(string service, string operation, AdminKey[] path) =>
        $"/_microstack/admin/v1/services/{service}/{operation}?path=" +
        Uri.EscapeDataString(JsonSerializer.Serialize(path, AdminJsonContext.Default.AdminKeyArray));

    private TClient Client<TClient, TConfig>(Func<TConfig, TClient> create)
        where TConfig : ClientConfig, new()
    {
        var http = new HttpClient(new CanonicalizeUriHandler(fixture.CreateHandler()))
        {
            BaseAddress = new Uri("http://localhost/"),
        };
        return create(new TConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(http),
        });
    }
}
