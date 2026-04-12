using Amazon;
using Amazon.Runtime;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the SSM Parameter Store service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_ssm.py.
/// </summary>
public sealed class SsmTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonSimpleSystemsManagementClient _ssm;

    public SsmTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _ssm = CreateSsmClient(fixture);
    }

    private static AmazonSimpleSystemsManagementClient CreateSsmClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonSimpleSystemsManagementConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonSimpleSystemsManagementClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _ssm.Dispose();
        return Task.CompletedTask;
    }

    // -- PutParameter / GetParameter ------------------------------------------

    [Fact]
    public async Task PutGet()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/app/db/host",
            Value = "localhost",
            Type = ParameterType.String,
        });

        var resp = await _ssm.GetParameterAsync(new GetParameterRequest
        {
            Name = "/app/db/host",
        });

        Assert.Equal("localhost", resp.Parameter.Value);
    }

    // -- GetParametersByPath --------------------------------------------------

    [Fact]
    public async Task GetByPath()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/app/config/key1",
            Value = "val1",
            Type = ParameterType.String,
        });

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/app/config/key2",
            Value = "val2",
            Type = ParameterType.String,
        });

        var resp = await _ssm.GetParametersByPathAsync(new GetParametersByPathRequest
        {
            Path = "/app/config",
            Recursive = true,
        });

        Assert.True(resp.Parameters.Count >= 2);
    }

    // -- Overwrite ------------------------------------------------------------

    [Fact]
    public async Task Overwrite()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/app/overwrite",
            Value = "v1",
            Type = ParameterType.String,
        });

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/app/overwrite",
            Value = "v2",
            Type = ParameterType.String,
            Overwrite = true,
        });

        var resp = await _ssm.GetParameterAsync(new GetParameterRequest
        {
            Name = "/app/overwrite",
        });

        Assert.Equal("v2", resp.Parameter.Value);
    }

    // -- PutGet with SecureString ---------------------------------------------

    [Fact]
    public async Task PutGetV2()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/pg/host",
            Value = "db.local",
            Type = ParameterType.String,
        });

        var resp = await _ssm.GetParameterAsync(new GetParameterRequest
        {
            Name = "/ssm2/pg/host",
        });

        Assert.Equal("db.local", resp.Parameter.Value);
        Assert.Equal(ParameterType.String, resp.Parameter.Type);
        Assert.Equal(1, resp.Parameter.Version);

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/pg/pass",
            Value = "secret123",
            Type = ParameterType.SecureString,
        });

        var respEnc = await _ssm.GetParameterAsync(new GetParameterRequest
        {
            Name = "/ssm2/pg/pass",
            WithDecryption = true,
        });

        Assert.Equal("secret123", respEnc.Parameter.Value);
    }

    // -- Overwrite + Version tracking -----------------------------------------

    [Fact]
    public async Task OverwriteVersionV2()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/ov/p",
            Value = "v1",
            Type = ParameterType.String,
        });

        var r1 = await _ssm.GetParameterAsync(new GetParameterRequest
        {
            Name = "/ssm2/ov/p",
        });

        Assert.Equal(1, r1.Parameter.Version);

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/ov/p",
            Value = "v2",
            Type = ParameterType.String,
            Overwrite = true,
        });

        var r2 = await _ssm.GetParameterAsync(new GetParameterRequest
        {
            Name = "/ssm2/ov/p",
        });

        Assert.Equal("v2", r2.Parameter.Value);
        Assert.Equal(2, r2.Parameter.Version);

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/ov/p",
            Value = "v3",
            Type = ParameterType.String,
            Overwrite = true,
        });

        var r3 = await _ssm.GetParameterAsync(new GetParameterRequest
        {
            Name = "/ssm2/ov/p",
        });

        Assert.Equal(3, r3.Parameter.Version);
    }

    // -- GetByPath recursive vs non-recursive ---------------------------------

    [Fact]
    public async Task GetByPathV2()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/path/x",
            Value = "vx",
            Type = ParameterType.String,
        });

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/path/y",
            Value = "vy",
            Type = ParameterType.String,
        });

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/path/sub/z",
            Value = "vz",
            Type = ParameterType.String,
        });

        var resp = await _ssm.GetParametersByPathAsync(new GetParametersByPathRequest
        {
            Path = "/ssm2/path",
            Recursive = true,
        });

        var names = resp.Parameters.ConvertAll(p => p.Name);
        Assert.Contains("/ssm2/path/x", names);
        Assert.Contains("/ssm2/path/y", names);
        Assert.Contains("/ssm2/path/sub/z", names);

        var respShallow = await _ssm.GetParametersByPathAsync(new GetParametersByPathRequest
        {
            Path = "/ssm2/path",
            Recursive = false,
        });

        var namesShallow = respShallow.Parameters.ConvertAll(p => p.Name);
        Assert.Contains("/ssm2/path/x", namesShallow);
        Assert.DoesNotContain("/ssm2/path/sub/z", namesShallow);
    }

    // -- GetParameters (batch) ------------------------------------------------

    [Fact]
    public async Task GetParametersMultipleV2()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/multi/a",
            Value = "va",
            Type = ParameterType.String,
        });

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/multi/b",
            Value = "vb",
            Type = ParameterType.String,
        });

        var resp = await _ssm.GetParametersAsync(new GetParametersRequest
        {
            Names = ["/ssm2/multi/a", "/ssm2/multi/b", "/ssm2/multi/nope"],
        });

        Assert.Equal(2, resp.Parameters.Count);
        Assert.Contains(resp.Parameters, p => p.Name == "/ssm2/multi/a");
        Assert.Contains(resp.Parameters, p => p.Name == "/ssm2/multi/b");
        Assert.Contains("/ssm2/multi/nope", resp.InvalidParameters ?? []);
    }

    // -- DeleteParameter / DeleteParameters -----------------------------------

    [Fact]
    public async Task DeleteV2()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/del/tmp",
            Value = "bye",
            Type = ParameterType.String,
        });

        await _ssm.DeleteParameterAsync(new DeleteParameterRequest
        {
            Name = "/ssm2/del/tmp",
        });

        var ex = await Assert.ThrowsAsync<ParameterNotFoundException>(() =>
            _ssm.GetParameterAsync(new GetParameterRequest
            {
                Name = "/ssm2/del/tmp",
            }));

        Assert.Equal("ParameterNotFound", ex.ErrorCode);

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/del/b1",
            Value = "v1",
            Type = ParameterType.String,
        });

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/del/b2",
            Value = "v2",
            Type = ParameterType.String,
        });

        var resp = await _ssm.DeleteParametersAsync(new DeleteParametersRequest
        {
            Names = ["/ssm2/del/b1", "/ssm2/del/b2", "/ssm2/del/ghost"],
        });

        Assert.Equal(2, (resp.DeletedParameters ?? []).Count);
        Assert.Contains("/ssm2/del/ghost", resp.InvalidParameters ?? []);
    }

    // -- DescribeParameters with filter ---------------------------------------

    [Fact]
    public async Task DescribeV2()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/desc/alpha",
            Value = "va",
            Type = ParameterType.String,
            Description = "alpha param",
        });

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/desc/beta",
            Value = "vb",
            Type = ParameterType.SecureString,
        });

        var resp = await _ssm.DescribeParametersAsync(new DescribeParametersRequest
        {
            ParameterFilters =
            [
                new ParameterStringFilter
                {
                    Key = "Name",
                    Option = "BeginsWith",
                    Values = ["/ssm2/desc/"],
                },
            ],
        });

        var names = (resp.Parameters ?? []).ConvertAll(p => p.Name);
        Assert.Contains("/ssm2/desc/alpha", names);
        Assert.Contains("/ssm2/desc/beta", names);
    }

    // -- GetParameterHistory --------------------------------------------------

    [Fact]
    public async Task ParameterHistoryV2()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/hist/h",
            Value = "h1",
            Type = ParameterType.String,
            Description = "d1",
        });

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/hist/h",
            Value = "h2",
            Type = ParameterType.String,
            Overwrite = true,
            Description = "d2",
        });

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/hist/h",
            Value = "h3",
            Type = ParameterType.String,
            Overwrite = true,
            Description = "d3",
        });

        var resp = await _ssm.GetParameterHistoryAsync(new GetParameterHistoryRequest
        {
            Name = "/ssm2/hist/h",
        });

        var history = resp.Parameters ?? [];
        Assert.Equal(3, history.Count);
        Assert.Equal("h1", history[0].Value);
        Assert.Equal(1, history[0].Version);
        Assert.Equal("h3", history[2].Value);
        Assert.Equal(3, history[2].Version);
    }

    // -- Tags -----------------------------------------------------------------

    [Fact]
    public async Task TagsV2()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/ssm2/tag/t1",
            Value = "v",
            Type = ParameterType.String,
        });

        await _ssm.AddTagsToResourceAsync(new AddTagsToResourceRequest
        {
            ResourceType = ResourceTypeForTagging.Parameter,
            ResourceId = "/ssm2/tag/t1",
            Tags =
            [
                new Tag { Key = "team", Value = "platform" },
                new Tag { Key = "env", Value = "staging" },
            ],
        });

        var resp = await _ssm.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceType = ResourceTypeForTagging.Parameter,
            ResourceId = "/ssm2/tag/t1",
        });

        var tagMap = resp.TagList.ToDictionary(t => t.Key, t => t.Value);
        Assert.Equal("platform", tagMap["team"]);
        Assert.Equal("staging", tagMap["env"]);

        await _ssm.RemoveTagsFromResourceAsync(new RemoveTagsFromResourceRequest
        {
            ResourceType = ResourceTypeForTagging.Parameter,
            ResourceId = "/ssm2/tag/t1",
            TagKeys = ["team"],
        });

        var resp2 = await _ssm.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceType = ResourceTypeForTagging.Parameter,
            ResourceId = "/ssm2/tag/t1",
        });

        var tagMap2 = resp2.TagList.ToDictionary(t => t.Key, t => t.Value);
        Assert.DoesNotContain("team", tagMap2.Keys);
        Assert.Equal("staging", tagMap2["env"]);
    }

    // -- LabelParameterVersion ------------------------------------------------

    [Fact]
    public async Task LabelParameterVersion()
    {
        var pname = $"/intg/label/{Guid.NewGuid():N}"[..24];

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = pname,
            Value = "v1",
            Type = ParameterType.String,
        });

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = pname,
            Value = "v2",
            Type = ParameterType.String,
            Overwrite = true,
        });

        var resp = await _ssm.LabelParameterVersionAsync(new LabelParameterVersionRequest
        {
            Name = pname,
            ParameterVersion = 1,
            Labels = ["stable"],
        });

        Assert.Equal(1, resp.ParameterVersion);
        Assert.Empty(resp.InvalidLabels);
    }

    // -- AddRemoveTags --------------------------------------------------------

    [Fact]
    public async Task AddRemoveTags()
    {
        var pname = $"/intg/tagged/{Guid.NewGuid():N}"[..24];

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = pname,
            Value = "hello",
            Type = ParameterType.String,
        });

        await _ssm.AddTagsToResourceAsync(new AddTagsToResourceRequest
        {
            ResourceType = ResourceTypeForTagging.Parameter,
            ResourceId = pname,
            Tags =
            [
                new Tag { Key = "env", Value = "prod" },
                new Tag { Key = "team", Value = "backend" },
            ],
        });

        var tags = await _ssm.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceType = ResourceTypeForTagging.Parameter,
            ResourceId = pname,
        });

        var tagMap = tags.TagList.ToDictionary(t => t.Key, t => t.Value);
        Assert.Equal("prod", tagMap["env"]);
        Assert.Equal("backend", tagMap["team"]);

        await _ssm.RemoveTagsFromResourceAsync(new RemoveTagsFromResourceRequest
        {
            ResourceType = ResourceTypeForTagging.Parameter,
            ResourceId = pname,
            TagKeys = ["team"],
        });

        var tags2 = await _ssm.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceType = ResourceTypeForTagging.Parameter,
            ResourceId = pname,
        });

        var tagMap2 = tags2.TagList.ToDictionary(t => t.Key, t => t.Value);
        Assert.DoesNotContain("team", tagMap2.Keys);
        Assert.Equal("prod", tagMap2["env"]);
    }

    // -- GetParameterHistory (alternate test) ----------------------------------

    [Fact]
    public async Task GetParameterHistory()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/qa/ssm/hist",
            Value = "v1",
            Type = ParameterType.String,
        });

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/qa/ssm/hist",
            Value = "v2",
            Type = ParameterType.String,
            Overwrite = true,
        });

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/qa/ssm/hist",
            Value = "v3",
            Type = ParameterType.String,
            Overwrite = true,
        });

        var resp = await _ssm.GetParameterHistoryAsync(new GetParameterHistoryRequest
        {
            Name = "/qa/ssm/hist",
        });

        var history = resp.Parameters ?? [];
        Assert.Equal(3, history.Count);

        var values = history.ConvertAll(h => h.Value);
        Assert.Contains("v1", values);
        Assert.Contains("v2", values);
        Assert.Contains("v3", values);
    }

    // -- DescribeParameters with Path filter -----------------------------------

    [Fact]
    public async Task DescribeParametersFilter()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/qa/ssm/filter/a",
            Value = "1",
            Type = ParameterType.String,
        });

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/qa/ssm/filter/b",
            Value = "2",
            Type = ParameterType.String,
        });

        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/qa/ssm/other/c",
            Value = "3",
            Type = ParameterType.String,
        });

        var resp = await _ssm.DescribeParametersAsync(new DescribeParametersRequest
        {
            ParameterFilters =
            [
                new ParameterStringFilter
                {
                    Key = "Path",
                    Values = ["/qa/ssm/filter"],
                },
            ],
        });

        var names = (resp.Parameters ?? []).ConvertAll(p => p.Name);
        Assert.Contains("/qa/ssm/filter/a", names);
        Assert.Contains("/qa/ssm/filter/b", names);
        Assert.DoesNotContain("/qa/ssm/other/c", names);
    }

    // -- SecureString not decrypted by default --------------------------------

    [Fact]
    public async Task SecureStringNotDecryptedByDefault()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/qa/ssm/secure",
            Value = "mysecret",
            Type = ParameterType.SecureString,
        });

        var resp = await _ssm.GetParameterAsync(new GetParameterRequest
        {
            Name = "/qa/ssm/secure",
            WithDecryption = false,
        });

        Assert.NotEqual("mysecret", resp.Parameter.Value);

        var resp2 = await _ssm.GetParameterAsync(new GetParameterRequest
        {
            Name = "/qa/ssm/secure",
            WithDecryption = true,
        });

        Assert.Equal("mysecret", resp2.Parameter.Value);
    }
}
