using System.Text;
using MicroStack.Internal;
using MicroStack.Internal.Admin;
using MicroStack.Services.Ec2;
using MicroStack.Services.Ecr;
using MicroStack.Services.Ecs;
using MicroStack.Services.Lambda;
using Shouldly;

namespace MicroStack.Tests;

public sealed class AdminComputeTests
{
    [Fact]
    public void Compute_sources_publish_all_supported_kinds()
    {
        Kinds(new LambdaServiceHandler(), "lambda").Order().ShouldBe(
            new[] { "aliases", "event-source-mappings", "functions", "layer-versions", "layers", "versions" });
        Kinds(new EcsServiceHandler(), "ecs").Order().ShouldBe(
            new[] { "clusters", "services", "task-definitions", "tasks" });
        Kinds(new EcrServiceHandler(), "ecr").Order().ShouldBe(
            new[] { "images", "lifecycle-policies", "repositories", "repository-policies" });
        Kinds(new Ec2ServiceHandler(), "ec2").Order().ShouldBe(new[]
        {
            "customer-gateways", "dhcp-options", "egress-internet-gateways", "elastic-ips",
            "flow-logs", "instances", "internet-gateways", "key-pairs", "launch-template-versions",
            "launch-templates", "nat-gateways", "network-acls", "network-interfaces", "prefix-lists",
            "route-tables", "security-groups", "snapshots", "subnets", "volumes", "vpc-endpoints",
            "vpc-peerings", "vpcs", "vpn-gateways",
        });
        Kinds(new LambdaServiceHandler(), "not-lambda").ShouldBeEmpty();
    }

    [Fact]
    public async Task Lambda_admin_projection_redacts_environment_without_mutating_service_state()
    {
        var handler = new LambdaServiceHandler();
        var body = """
            {"FunctionName":"redacted","Runtime":"dotnet8","Role":"arn:aws:iam::000000000000:role/test",
             "Handler":"Example::Handler","Code":{"ZipFile":""},
             "Environment":{"Variables":{"AWS_SECRET_ACCESS_KEY":"keep-me","NORMAL":"visible"}}}
            """;
        (await handler.HandleAsync(Rest("POST", "/2015-03-31/functions", body))).StatusCode.ShouldBe(201);

        var source = (IAdminResourceSource)handler;
        var function = source.GetAdminResources("lambda").Single();
        var projected = function.ReadContent!().Text!;
        using (var document = JsonDocument.Parse(projected))
        {
            document.RootElement.GetProperty("Environment").GetProperty("Variables")
                .GetProperty("AWS_SECRET_ACCESS_KEY").GetString().ShouldBe(AdminData.MaskedValue);
            document.RootElement.GetProperty("Environment").GetProperty("Variables")
                .GetProperty("NORMAL").GetString().ShouldBe("visible");
        }
        projected.ShouldNotContain("keep-me");

        var response = await handler.HandleAsync(Rest("GET", "/2015-03-31/functions/redacted/configuration"));
        Encoding.UTF8.GetString(response.Body).ShouldContain("keep-me");
    }

    [Fact]
    public async Task Ecr_admin_resources_are_account_scoped()
    {
        var handler = new EcrServiceHandler();
        using (AccountContext.BeginScope("111111111111"))
        {
            var request = JsonTarget("AmazonEC2ContainerRegistry_V20150921.CreateRepository",
                """{"repositoryName":"one"}""");
            (await handler.HandleAsync(request)).StatusCode.ShouldBe(200);
            ((IAdminResourceSource)handler).GetAdminResources("ecr").Single().Resource.Key.Id.ShouldBe("one");
        }
        using (AccountContext.BeginScope("222222222222"))
            ((IAdminResourceSource)handler).GetAdminResources("ecr").ShouldBeEmpty();
    }

    [Fact]
    public async Task Ec2_key_pair_projection_keeps_metadata_but_never_private_material()
    {
        var handler = new Ec2ServiceHandler();
        var request = Rest("POST", "/", "Action=CreateKeyPair&KeyName=admin-test");
        (await handler.HandleAsync(request)).StatusCode.ShouldBe(200);

        var keyPair = ((IAdminResourceSource)handler).GetAdminResources("ec2")
            .Single(node => node.Resource.Key.Kind == "key-pairs");
        var content = keyPair.ReadContent!().Text!;
        content.ShouldContain("KeyFingerprint");
        content.ShouldNotContain("KeyMaterial");
        content.ShouldNotContain("PRIVATE KEY");
    }

    private static string[] Kinds(IAdminResourceSource source, string service) =>
        source.GetAdminResourceKinds(service).Select(kind => kind.Id).ToArray();

    private static ServiceRequest Rest(string method, string path, string body = "") =>
        new(method, path, new Dictionary<string, string>(),
            Encoding.UTF8.GetBytes(body), new Dictionary<string, string[]>());

    private static ServiceRequest JsonTarget(string target, string body) =>
        new("POST", "/", new Dictionary<string, string> { ["x-amz-target"] = target },
            Encoding.UTF8.GetBytes(body), new Dictionary<string, string[]>());
}
