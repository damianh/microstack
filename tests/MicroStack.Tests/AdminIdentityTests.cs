using System.Text;
using System.Text.Json;
using MicroStack.Internal;
using MicroStack.Internal.Admin;
using MicroStack.Services.Acm;
using MicroStack.Services.Cognito;
using MicroStack.Services.Iam;
using MicroStack.Services.Kms;
using MicroStack.Services.SecretsManager;
using MicroStack.Services.Sts;
using MicroStack.Services.Waf;

namespace MicroStack.Tests;

public sealed class AdminIdentityTests : IDisposable
{
    public void Dispose() => AccountContext.Reset();

    [Fact]
    public void IdentityHandlersDeclareOnlyRetainedRootKinds()
    {
        var idp = new CognitoIdpServiceHandler();

        Kinds(new AcmServiceHandler()).ShouldBe(["certificate"]);
        Kinds(idp).ShouldBe(["group", "user", "user-pool", "user-pool-client"], ignoreOrder: true);
        Kinds(new CognitoIdentityServiceHandler(idp)).ShouldBe(["identity", "identity-pool"], ignoreOrder: true);
        Kinds(new IamServiceHandler()).ShouldBe(
            ["access-key", "group", "instance-profile", "oidc-provider", "policy", "policy-version",
             "role", "role-inline-policy", "user", "user-inline-policy"], ignoreOrder: true);
        Kinds(new KmsServiceHandler()).ShouldBe(["alias", "key"], ignoreOrder: true);
        Kinds(new SecretsManagerServiceHandler()).ShouldBe(["secret", "secret-version"]);
        Kinds(new WafServiceHandler()).ShouldBe(["ip-set", "rule-group", "web-acl"], ignoreOrder: true);

        var sts = (IAdminResourceSource)new StsServiceHandler(new IamServiceHandler());
        sts.GetAdminResourceKinds("ignored").ShouldBeEmpty();
        sts.GetAdminResources("ignored").ShouldBeEmpty();
        sts.GetAdminNotice("sts")!.ShouldContain("not retained");
    }

    [Fact]
    public async Task SecretsAreMaskedAndOnlyExplicitlyRevealExistingValues()
    {
        const string secretValue = "do-not-leak";
        var handler = new SecretsManagerServiceHandler();
        await Json(handler, "secretsmanager", "AWSSecretsManager.CreateSecret",
            $$"""{"Name":"admin-secret","SecretString":"{{secretValue}}"}""");

        var secret = ((IAdminResourceSource)handler).GetAdminResources("ignored").Single();
        var version = secret.ReadChildren!().Single();
        var fields = version.ReadFields!();
        fields.Single(x => x.Name == "SecretString").Value.ShouldBe(AdminData.MaskedValue);
        JsonSerializer.Serialize(fields).ShouldNotContain(secretValue);
        version.RevealableFields.ShouldBe(["SecretString"]);
        version.RevealField!("SecretString").Text.ShouldBe(secretValue);
        version.RevealField!("SecretBinary").Text.ShouldBeNull();
        version.RevealField!("missing").Text.ShouldBeNull();
    }

    [Fact]
    public async Task CognitoUsesSharedStateAndNeverRevealsPasswords()
    {
        var idp = new CognitoIdpServiceHandler();
        var identity = new CognitoIdentityServiceHandler(idp);
        var poolResponse = await Json(idp, "cognito-idp",
            "AWSCognitoIdentityProviderService.CreateUserPool", """{"PoolName":"shared"}""");
        using var poolJson = JsonDocument.Parse(poolResponse.Body);
        var poolId = poolJson.RootElement.GetProperty("UserPool").GetProperty("Id").GetString()!;

        var clientResponse = await Json(idp, "cognito-idp",
            "AWSCognitoIdentityProviderService.CreateUserPoolClient",
            $$"""{"UserPoolId":"{{poolId}}","ClientName":"app","GenerateSecret":true}""");
        using var clientJson = JsonDocument.Parse(clientResponse.Body);
        var expectedSecret = clientJson.RootElement.GetProperty("UserPoolClient")
            .GetProperty("ClientSecret").GetString()!;
        await Json(idp, "cognito-idp", "AWSCognitoIdentityProviderService.AdminCreateUser",
            $$"""{"UserPoolId":"{{poolId}}","Username":"alice","TemporaryPassword":"hidden-password"}""");
        await Json(identity, "cognito-identity", "AWSCognitoIdentityService.CreateIdentityPool",
            "{\"IdentityPoolName\":\"federated\",\"AllowUnauthenticatedIdentities\":false," +
            "\"CognitoIdentityProviders\":[{\"ProviderName\":\"cognito-idp.us-east-1.amazonaws.com/" +
            poolId + "\"}]}");

        var pool = ((IAdminResourceSource)idp).GetAdminResources("ignored").Single();
        var children = pool.ReadChildren!().ToArray();
        var client = children.Single(x => x.Resource.Key.Kind == "user-pool-client");
        client.ReadFields!().Single(x => x.Name == "Client secret").Value.ShouldBe(AdminData.MaskedValue);
        client.RevealField!("ClientSecret").Text.ShouldBe(expectedSecret);
        var user = children.Single(x => x.Resource.Key.Kind == "user");
        JsonSerializer.Serialize(user.ReadFields!()).ShouldNotContain("hidden-password");
        user.RevealField.ShouldBeNull();

        ((IAdminResourceSource)identity).GetAdminResources("ignored").Single()
            .ReadConnections!().Single().TargetPath![0].Id.ShouldBe(poolId);
    }

    [Fact]
    public async Task KmsProjectionNeverExposesOrGeneratesKeyMaterial()
    {
        var handler = new KmsServiceHandler();
        await Json(handler, "kms", "TrentService.CreateKey",
            """{"Description":"admin key","KeySpec":"SYMMETRIC_DEFAULT"}""");

        var key = ((IAdminResourceSource)handler).GetAdminResources("ignored")
            .Single(x => x.Resource.Key.Kind == "key");
        key.RevealableFields.ShouldBeEmpty();
        key.RevealField.ShouldBeNull();
        var projection = JsonSerializer.Serialize(key.ReadFields!());
        projection.ShouldNotContain("SymmetricKey");
        projection.ShouldNotContain("Private");
    }

    [Fact]
    public async Task ResourcesAreAccountScopedAndInspectionDoesNotMutate()
    {
        var handler = new AcmServiceHandler();
        using (AccountContext.BeginScope("111111111111"))
        {
            await Json(handler, "acm", "CertificateManager.RequestCertificate",
                """{"DomainName":"example.test"}""");
            var node = ((IAdminResourceSource)handler).GetAdminResources("ignored").Single();
            var before = JsonSerializer.Serialize(node.ReadFields!());
            _ = node.ReadFields!();
            JsonSerializer.Serialize(node.ReadFields!()).ShouldBe(before);
        }

        using (AccountContext.BeginScope("222222222222"))
            ((IAdminResourceSource)handler).GetAdminResources("ignored").ShouldBeEmpty();
    }

    private static string[] Kinds(IAdminResourceSource source) =>
        source.GetAdminResourceKinds("ignored").Select(x => x.Id).ToArray();

    private static Task<ServiceResponse> Json(
        IServiceHandler handler, string service, string target, string body) =>
        handler.HandleAsync(new ServiceRequest("POST", "/",
            new Dictionary<string, string>
            {
                ["x-amz-target"] = target,
                ["content-type"] = "application/x-amz-json-1.1",
                ["host"] = $"{service}.us-east-1.amazonaws.com"
            },
            Encoding.UTF8.GetBytes(body), new Dictionary<string, string[]>()));
}
