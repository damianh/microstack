using System.Text;
using System.Text.Json;
using System.Xml.Linq;
using MicroStack.Internal;
using MicroStack.Internal.Admin;
using MicroStack.Services.Alb;
using MicroStack.Services.ApiGateway;
using MicroStack.Services.AppSync;
using MicroStack.Services.CloudFront;
using MicroStack.Services.Lambda;
using MicroStack.Services.Route53;
using MicroStack.Services.ServiceDiscovery;

namespace MicroStack.Tests;

public sealed class AdminNetworkingTests
{
    [Fact]
    public async Task ApiGatewayProviderExposesV1AndV2FromSameLiveHandler()
    {
        var handler = new ApiGatewayV2ServiceHandler(new LambdaServiceHandler());
        var source = (IAdminResourceSource)handler;

        var v1 = await handler.HandleAsync(Json("POST", "/restapis", """{"name":"rest"}"""));
        var v1Id = JsonString(v1, "id");
        await handler.HandleAsync(Json("POST", $"/restapis/{v1Id}/resources",
            """{"pathPart":"items"}"""));

        var v2 = await handler.HandleAsync(Json("POST", "/v2/apis",
            """{"name":"http","protocolType":"HTTP"}"""));
        var v2Id = JsonString(v2, "apiId");
        await handler.HandleAsync(Json("POST", $"/v2/apis/{v2Id}/routes",
            """{"routeKey":"GET /items"}"""));
        await handler.HandleAsync(Json("POST", $"/v2/apis/{v2Id}/integrations",
            """{"integrationType":"MOCK"}"""));
        await handler.HandleAsync(Json("POST", $"/v2/apis/{v2Id}/stages",
            """{"stageName":"dev"}"""));

        source.GetAdminResourceKinds("apigateway").Select(x => x.Id)
            .ShouldContain("rest-apis");
        source.GetAdminResourceKinds("apigatewayv2").Select(x => x.Id)
            .ShouldContain("routes");

        var rest = source.GetAdminResources("apigateway").Single(x => x.Resource.Key.Id == v1Id);
        rest.ReadChildren!().ShouldContain(x => x.Resource.Key.Kind == "resources");
        var http = source.GetAdminResources("apigatewayv2").Single(x => x.Resource.Key.Id == v2Id);
        var children = http.ReadChildren!().ToArray();
        children.ShouldContain(x => x.Resource.Key.Kind == "routes");
        children.ShouldContain(x => x.Resource.Key.Kind == "integrations");
        children.ShouldContain(x => x.Resource.Key.Kind == "stages");
    }

    [Fact]
    public async Task AlbProviderBuildsLinkedLoadBalancerGraph()
    {
        var handler = new AlbServiceHandler();
        var lb = await handler.HandleAsync(Query("CreateLoadBalancer", ("Name", "admin-lb")));
        var lbArn = XmlValue(lb, "LoadBalancerArn");
        var tg = await handler.HandleAsync(Query("CreateTargetGroup",
            ("Name", "admin-tg"), ("Port", "80"), ("Protocol", "HTTP")));
        var tgArn = XmlValue(tg, "TargetGroupArn");
        await handler.HandleAsync(Query("RegisterTargets",
            ("TargetGroupArn", tgArn), ("Targets.member.1.Id", "i-123")));
        await handler.HandleAsync(Query("CreateListener",
            ("LoadBalancerArn", lbArn), ("Port", "80"), ("Protocol", "HTTP"),
            ("DefaultActions.member.1.Type", "forward"),
            ("DefaultActions.member.1.TargetGroupArn", tgArn)));

        var roots = ((IAdminResourceSource)handler)
            .GetAdminResources("elasticloadbalancing").ToArray();
        var loadBalancer = roots.Single(x => x.Resource.Key.Id == lbArn);
        var listener = loadBalancer.ReadChildren!().Single();
        listener.ReadConnections!().ShouldContain(x =>
            x.TargetPath != null && x.TargetPath.Single().Id == tgArn && x.Relation == "forwards-to");
        var targetGroup = roots.Single(x => x.Resource.Key.Id == tgArn);
        targetGroup.ReadChildren!().Single().Resource.Name.ShouldBe("i-123");
        targetGroup.ReadConnections!().Single().TargetPath!.Single().Id.ShouldBe(lbArn);
    }

    [Fact]
    public async Task AppSyncProviderExposesConfigurationSchemaAndMasksKeys()
    {
        var handler = new AppSyncServiceHandler();
        var created = await handler.HandleAsync(Json("POST", "/v1/apis",
            """{"name":"admin-api","authenticationType":"API_KEY"}"""));
        var apiId = JsonNestedString(created, "graphqlApi", "apiId");
        var key = await handler.HandleAsync(Json("POST", $"/v1/apis/{apiId}/apikeys", "{}"));
        var secret = JsonNestedString(key, "apiKey", "id");
        await handler.HandleAsync(Json("POST", $"/v1/apis/{apiId}/types",
            """{"definition":"type Query { ping: String }","format":"SDL"}"""));
        await handler.HandleAsync(Json("POST", $"/v1/apis/{apiId}/datasources",
            """{"name":"none","type":"NONE"}"""));
        await handler.HandleAsync(Json("POST", $"/v1/apis/{apiId}/types/Query/resolvers",
            """{"fieldName":"ping","dataSourceName":"none"}"""));

        var api = ((IAdminResourceSource)handler).GetAdminResources("appsync").Single();
        var children = api.ReadChildren!().ToArray();
        var apiKey = children.Single(x => x.Resource.Key.Kind == "api-keys");
        apiKey.Resource.Key.Id.ShouldNotContain(secret);
        apiKey.ReadFields!().Single(x => x.Name == "id").Value.ShouldBe(AdminData.MaskedValue);
        children.Single(x => x.Resource.Key.Kind == "types")
            .ReadContent!().Text!.ShouldContain("type Query");
        children.Single(x => x.Resource.Key.Kind == "resolvers")
            .ReadConnections!().Single().TargetPath!.Single().Id.ShouldBe("none");
    }

    [Fact]
    public async Task CloudFrontProviderExposesConfigAndInvalidations()
    {
        var handler = new CloudFrontServiceHandler();
        var create = await handler.HandleAsync(Xml("POST", "/2020-05-31/distribution",
            "<DistributionConfig><CallerReference>one</CallerReference><Origins/>" +
            "<DefaultCacheBehavior/><Enabled>true</Enabled></DistributionConfig>"));
        var id = XmlValue(create, "Id");
        await handler.HandleAsync(Xml("POST", $"/2020-05-31/distribution/{id}/invalidation",
            "<InvalidationBatch><Paths><Quantity>1</Quantity><Items><Path>/*</Path></Items></Paths>" +
            "<CallerReference>inv-one</CallerReference></InvalidationBatch>"));

        var distribution = ((IAdminResourceSource)handler)
            .GetAdminResources("cloudfront").Single();
        distribution.ReadContent!().ContentType.ShouldBe("application/xml");
        distribution.ReadContent().Text!.ShouldContain("DistributionConfig");
        distribution.ReadChildren!().Single().Resource.Key.Kind.ShouldBe("invalidations");
    }

    [Fact]
    public async Task Route53AndCloudMapProvidersExposeRecordsAndActualLinks()
    {
        var route53 = new Route53ServiceHandler();
        await route53.HandleAsync(Xml("POST", "/2013-04-01/hostedzone",
            "<CreateHostedZoneRequest><Name>example.test</Name>" +
            "<CallerReference>route-one</CallerReference></CreateHostedZoneRequest>"));
        var zone = ((IAdminResourceSource)route53).GetAdminResources("route53").Single();
        zone.ReadChildren!().Select(x => x.Resource.Name)
            .ShouldContain(x => x.EndsWith(" SOA", StringComparison.Ordinal));

        var cloudMap = new ServiceDiscoveryServiceHandler(route53);
        await cloudMap.HandleAsync(Target("CreatePrivateDnsNamespace",
            """{"Name":"internal.test","Vpc":"vpc-1"}"""));
        var ns = ((IAdminResourceSource)cloudMap).GetAdminResources("servicediscovery").Single();
        var zoneLink = ns.ReadConnections!().Single();
        ((IAdminResourceSource)route53).GetAdminResources("route53")
            .ShouldContain(x => x.Resource.Key.Id == zoneLink.TargetPath!.Single().Id);

        await cloudMap.HandleAsync(Target("CreateService",
            $"{{\"Name\":\"orders\",\"NamespaceId\":\"{ns.Resource.Key.Id}\"}}"));
        var service = ns.ReadChildren!().Single();
        await cloudMap.HandleAsync(Target("RegisterInstance",
            $"{{\"ServiceId\":\"{service.Resource.Key.Id}\",\"InstanceId\":\"node-1\"," +
            "\"Attributes\":{\"AWS_INSTANCE_IPV4\":\"10.0.0.1\"}}"));
        var instance = service.ReadChildren!().Single();
        service.ReadConnections!().Single().TargetPath!.Single().Id.ShouldBe(ns.Resource.Key.Id);
        instance.ReadConnections!().Single().TargetPath!.Single().Id.ShouldBe(service.Resource.Key.Id);
    }

    [Fact]
    public async Task ProvidersRemainAccountScoped()
    {
        var handler = new Route53ServiceHandler();
        using (AccountContext.BeginScope("111111111111"))
            await handler.HandleAsync(Xml("POST", "/2013-04-01/hostedzone",
                "<CreateHostedZoneRequest><Name>one.test</Name>" +
                "<CallerReference>one</CallerReference></CreateHostedZoneRequest>"));

        using (AccountContext.BeginScope("222222222222"))
            ((IAdminResourceSource)handler).GetAdminResources("route53").ShouldBeEmpty();
    }

    private static ServiceRequest Json(string method, string path, string body) =>
        new(method, path, new Dictionary<string, string>(),
            Encoding.UTF8.GetBytes(body), new Dictionary<string, string[]>());

    private static ServiceRequest Xml(string method, string path, string body) =>
        new(method, path, new Dictionary<string, string>(),
            Encoding.UTF8.GetBytes(body), new Dictionary<string, string[]>());

    private static ServiceRequest Target(string action, string body) =>
        new("POST", "/", new Dictionary<string, string>
        {
            ["x-amz-target"] = $"Route53AutoNaming_v20170314.{action}",
        }, Encoding.UTF8.GetBytes(body), new Dictionary<string, string[]>());

    private static ServiceRequest Query(string action, params (string Key, string Value)[] values)
    {
        var parameters = new[] { ("Action", action) }.Concat(values);
        var body = string.Join("&", parameters.Select(x =>
            $"{Uri.EscapeDataString(x.Item1)}={Uri.EscapeDataString(x.Item2)}"));
        return new("POST", "/", new Dictionary<string, string>(),
            Encoding.UTF8.GetBytes(body), new Dictionary<string, string[]>());
    }

    private static string JsonString(ServiceResponse response, string property)
    {
        using var doc = JsonDocument.Parse(response.Body);
        return doc.RootElement.GetProperty(property).GetString()!;
    }

    private static string JsonNestedString(ServiceResponse response, string parent, string property)
    {
        using var doc = JsonDocument.Parse(response.Body);
        return doc.RootElement.GetProperty(parent).GetProperty(property).GetString()!;
    }

    private static string XmlValue(ServiceResponse response, string localName) =>
        XDocument.Parse(Encoding.UTF8.GetString(response.Body))
            .Descendants().First(x => x.Name.LocalName == localName).Value;
}
