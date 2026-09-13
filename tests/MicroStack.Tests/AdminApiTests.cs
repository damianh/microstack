using System.Net.Http.Json;
using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Internal.Admin;
using Microsoft.Extensions.DependencyInjection;

namespace MicroStack.Tests;

public sealed class AdminApiTests(MicroStackFixture fixture) : IClassFixture<MicroStackFixture>
{
    [Fact]
    public async Task ContextAndCatalogUseVersionedCamelCaseContract()
    {
        using var contextResponse = await fixture.HttpClient.GetAsync("/_microstack/admin/v1/context");
        contextResponse.EnsureSuccessStatusCode();
        var json = await contextResponse.Content.ReadAsStringAsync();
        json.ShouldContain("\"defaultAccount\"");
        json.Contains("\"DefaultAccount\":", StringComparison.Ordinal).ShouldBeFalse();

        var context = await contextResponse.Content.ReadFromJsonAsync(AdminJsonContext.Default.AdminContext);
        context.ShouldNotBeNull();
        context.DefaultAccount.Length.ShouldBe(12);
        context.PreviewMaxBytes.ShouldBe(1_048_576);

        var services = await fixture.HttpClient.GetFromJsonAsync(
            "/_microstack/admin/v1/services", AdminJsonContext.Default.AdminServiceArray);
        services.ShouldNotBeNull();
        services.Length.ShouldBe(40);
        services.Select(service => service.Id).Distinct(StringComparer.Ordinal).Count().ShouldBe(40);
        services.Single(service => service.Id == "apigateway").CanonicalHandler.ShouldBe("apigateway");
        services.Single(service => service.Id == "apigatewayv2").CanonicalHandler.ShouldBe("apigateway");
    }

    [Fact]
    public void CatalogIdentityAndScopeMatchRuntimeHandlers()
    {
        var expected = new Dictionary<string, (string Canonical, string Category, string Scope)>
        {
            ["sqs"]=("sqs","Messaging & workflows","account"), ["sns"]=("sns","Messaging & workflows","account"),
            ["events"]=("events","Messaging & workflows","account"), ["ses"]=("ses","Messaging & workflows","account"),
            ["stepfunctions"]=("states","Messaging & workflows","account"),
            ["s3"]=("s3","Storage & databases","account"), ["s3files"]=("s3files","Storage & databases","account"),
            ["efs"]=("elasticfilesystem","Storage & databases","account"), ["dynamodb"]=("dynamodb","Storage & databases","account"),
            ["rds"]=("rds","Storage & databases","account"), ["rdsdata"]=("rds-data","Storage & databases","account"),
            ["elasticache"]=("elasticache","Storage & databases","account"),
            ["lambda"]=("lambda","Compute & containers","account"), ["ec2"]=("ec2","Compute & containers","account"),
            ["ecs"]=("ecs","Compute & containers","account"), ["ecr"]=("ecr","Compute & containers","account"),
            ["apigateway"]=("apigateway","Networking & delivery","account"),
            ["apigatewayv2"]=("apigateway","Networking & delivery","account"),
            ["alb"]=("elasticloadbalancing","Networking & delivery","account"), ["appsync"]=("appsync","Networking & delivery","account"),
            ["cloudfront"]=("cloudfront","Networking & delivery","account"), ["route53"]=("route53","Networking & delivery","account"),
            ["servicediscovery"]=("servicediscovery","Networking & delivery","account"),
            ["acm"]=("acm","Security & identity","account"), ["cognitoidp"]=("cognito-idp","Security & identity","account"),
            ["cognitoidentity"]=("cognito-identity","Security & identity","account"), ["iam"]=("iam","Security & identity","account"),
            ["kms"]=("kms","Security & identity","account"), ["secretsmanager"]=("secretsmanager","Security & identity","account"),
            ["sts"]=("sts","Security & identity","account"), ["waf"]=("wafv2","Security & identity","account"),
            ["athena"]=("athena","Analytics & streaming","account"), ["emr"]=("elasticmapreduce","Analytics & streaming","account"),
            ["firehose"]=("firehose","Analytics & streaming","global"), ["glue"]=("glue","Analytics & streaming","account"),
            ["kinesis"]=("kinesis","Analytics & streaming","account"),
            ["cloudformation"]=("cloudformation","Management & observability","account"),
            ["cloudwatch"]=("monitoring","Management & observability","account"), ["logs"]=("logs","Management & observability","account"),
            ["ssm"]=("ssm","Management & observability","account")
        };

        AdminCatalog.Entries.Count.ShouldBe(40);
        AdminCatalog.Entries.Select(entry => entry.Id).ShouldBeUnique();
        AdminCatalog.Entries.Select(entry => entry.Icon).ShouldBeUnique();
        foreach (var entry in AdminCatalog.Entries)
        {
            expected.ShouldContainKey(entry.Id);
            entry.Icon.ShouldBe(entry.Id);
            (entry.CanonicalHandler, entry.Category, entry.Scope).ShouldBe(expected[entry.Id]);
        }

        var registry = fixture.Factory.Services.GetRequiredService<ServiceRegistry>();
        var apiGateway = (IAdminResourceSource)registry.Resolve("apigateway")!;
        apiGateway.GetAdminResourceKinds("__wrong_variant__").ShouldBeEmpty();
        apiGateway.GetAdminResourceKinds("apigateway").Select(kind => kind.Id).ShouldContain("rest-apis");
        apiGateway.GetAdminResourceKinds("apigateway").Select(kind => kind.Id).ShouldNotContain("apis");
        apiGateway.GetAdminResourceKinds("apigatewayv2").Select(kind => kind.Id).ShouldContain("apis");
        apiGateway.GetAdminResourceKinds("apigatewayv2").Select(kind => kind.Id).ShouldNotContain("rest-apis");
    }

    [Theory]
    [InlineData("")]
    [InlineData("123")]
    [InlineData("12345678901x")]
    [InlineData("1234567890123")]
    public async Task InvalidAccountsReturnStructuredBadRequest(string account)
    {
        using var response = await fixture.HttpClient.GetAsync(
            $"/_microstack/admin/v1/services?accountId={account}");

        response.StatusCode.ShouldBe(HttpStatusCode.BadRequest);
        var error = await response.Content.ReadFromJsonAsync(AdminJsonContext.Default.AdminError);
        error.ShouldNotBeNull();
        error.Code.ShouldBe("invalid_account");
    }

    [Fact]
    public async Task UnknownServiceAndMalformedPagingAreStructured()
    {
        using var missing = await fixture.HttpClient.GetAsync(
            "/_microstack/admin/v1/services/not-a-service/resources");
        missing.StatusCode.ShouldBe(HttpStatusCode.NotFound);
        (await missing.Content.ReadFromJsonAsync(AdminJsonContext.Default.AdminError))!.Code
            .ShouldBe("service_not_found");

        using var invalidLimit = await fixture.HttpClient.GetAsync(
            "/_microstack/admin/v1/services/sqs/resources?pageSize=201");
        invalidLimit.StatusCode.ShouldBe(HttpStatusCode.BadRequest);
        (await invalidLimit.Content.ReadFromJsonAsync(AdminJsonContext.Default.AdminError))!.Code
            .ShouldBe("invalid_limit");

        using var invalidCursor = await fixture.HttpClient.GetAsync(
            "/_microstack/admin/v1/services/sqs/resources?cursor=not-a-cursor");
        invalidCursor.StatusCode.ShouldBe(HttpStatusCode.BadRequest);
        (await invalidCursor.Content.ReadFromJsonAsync(AdminJsonContext.Default.AdminError))!.Code
            .ShouldBe("invalid_cursor");
    }
}
