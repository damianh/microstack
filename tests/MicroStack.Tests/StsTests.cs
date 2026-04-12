using Amazon;
using Amazon.IdentityManagement;
using Amazon.IdentityManagement.Model;
using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the STS service handler.
/// Uses the AWS SDK for .NET v4 pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_sts.py.
/// </summary>
public sealed class StsTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonSecurityTokenServiceClient _sts;
    private readonly AmazonIdentityManagementServiceClient _iam;

    public StsTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _sts = CreateStsClient(fixture);
        _iam = CreateIamClient(fixture);
    }

    private static AmazonSecurityTokenServiceClient CreateStsClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonSecurityTokenServiceConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonSecurityTokenServiceClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    private static AmazonIdentityManagementServiceClient CreateIamClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonIdentityManagementServiceConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonIdentityManagementServiceClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _sts.Dispose();
        _iam.Dispose();
        return Task.CompletedTask;
    }

    [Fact]
    public async Task GetCallerIdentity()
    {
        var resp = await _sts.GetCallerIdentityAsync(new GetCallerIdentityRequest());
        Assert.Equal("000000000000", resp.Account);
    }

    [Fact]
    public async Task AssumeRoleReturnsCredentials()
    {
        var resp = await _sts.AssumeRoleAsync(new AssumeRoleRequest
        {
            RoleArn = "arn:aws:iam::000000000000:role/test-role",
            RoleSessionName = "intg-session",
        });

        Assert.NotEmpty(resp.Credentials.AccessKeyId);
        Assert.NotEmpty(resp.Credentials.SecretAccessKey);
        Assert.NotEmpty(resp.Credentials.SessionToken);
        Assert.NotEqual(default, resp.Credentials.Expiration);
        Assert.NotEmpty(resp.AssumedRoleUser.Arn);
    }

    [Fact]
    public async Task GetAccessKeyInfo()
    {
        var resp = await _sts.GetAccessKeyInfoAsync(new GetAccessKeyInfoRequest
        {
            AccessKeyId = "AKIAIOSFODNN7EXAMPLE",
        });

        Assert.Equal("000000000000", resp.Account);
    }

    [Fact]
    public async Task GetCallerIdentityFull()
    {
        var resp = await _sts.GetCallerIdentityAsync(new GetCallerIdentityRequest());
        Assert.Equal("000000000000", resp.Account);
        Assert.NotEmpty(resp.Arn);
        Assert.NotEmpty(resp.UserId);
    }

    [Fact]
    public async Task AssumeRole()
    {
        var resp = await _sts.AssumeRoleAsync(new AssumeRoleRequest
        {
            RoleArn = "arn:aws:iam::000000000000:role/iam-test-role",
            RoleSessionName = "test-session",
            DurationSeconds = 900,
        });

        Assert.StartsWith("ASIA", resp.Credentials.AccessKeyId);
        Assert.NotEmpty(resp.Credentials.SecretAccessKey);
        Assert.NotEmpty(resp.Credentials.SessionToken);
        Assert.NotEqual(default, resp.Credentials.Expiration);
        Assert.Contains("test-session", resp.AssumedRoleUser.Arn);
        Assert.NotEmpty(resp.AssumedRoleUser.AssumedRoleId);
    }

    [Fact]
    public async Task GetSessionToken()
    {
        var resp = await _sts.GetSessionTokenAsync(new GetSessionTokenRequest
        {
            DurationSeconds = 900,
        });

        Assert.NotEmpty(resp.Credentials.AccessKeyId);
        Assert.NotEmpty(resp.Credentials.SecretAccessKey);
        Assert.NotEmpty(resp.Credentials.SessionToken);
        Assert.NotEqual(default, resp.Credentials.Expiration);
    }

    [Fact]
    public async Task AssumeRoleWithWebIdentity()
    {
        await _iam.CreateRoleAsync(new CreateRoleRequest
        {
            RoleName = "test-oidc-role",
            AssumeRolePolicyDocument = """{"Version":"2012-10-17","Statement":[]}""",
        });

        var roleArn = "arn:aws:iam::000000000000:role/test-oidc-role";

        var resp = await _sts.AssumeRoleWithWebIdentityAsync(new AssumeRoleWithWebIdentityRequest
        {
            RoleArn = roleArn,
            RoleSessionName = "ci-session",
            WebIdentityToken = "fake-oidc-token-value",
        });

        Assert.NotEmpty(resp.Credentials.AccessKeyId);
        Assert.NotEmpty(resp.Credentials.SecretAccessKey);
        Assert.NotEmpty(resp.Credentials.SessionToken);
        Assert.NotEqual(default, resp.Credentials.Expiration);
    }
}
