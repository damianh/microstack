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

        resp.Parameter.Value.ShouldBe("localhost");
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

        (resp.Parameters.Count >= 2).ShouldBe(true);
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

        resp.Parameter.Value.ShouldBe("v2");
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

        resp.Parameter.Value.ShouldBe("db.local");
        resp.Parameter.Type.ShouldBe(ParameterType.String);
        resp.Parameter.Version.ShouldBe(1);

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

        respEnc.Parameter.Value.ShouldBe("secret123");
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

        r1.Parameter.Version.ShouldBe(1);

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

        r2.Parameter.Value.ShouldBe("v2");
        r2.Parameter.Version.ShouldBe(2);

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

        r3.Parameter.Version.ShouldBe(3);
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
        names.ShouldContain("/ssm2/path/x");
        names.ShouldContain("/ssm2/path/y");
        names.ShouldContain("/ssm2/path/sub/z");

        var respShallow = await _ssm.GetParametersByPathAsync(new GetParametersByPathRequest
        {
            Path = "/ssm2/path",
            Recursive = false,
        });

        var namesShallow = respShallow.Parameters.ConvertAll(p => p.Name);
        namesShallow.ShouldContain("/ssm2/path/x");
        namesShallow.ShouldNotContain("/ssm2/path/sub/z");
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

        resp.Parameters.Count.ShouldBe(2);
        resp.Parameters.ShouldContain(p => p.Name == "/ssm2/multi/a");
        resp.Parameters.ShouldContain(p => p.Name == "/ssm2/multi/b");
        resp.InvalidParameters.ShouldContain("/ssm2/multi/nope");
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

        var ex = await Should.ThrowAsync<ParameterNotFoundException>(() =>
            _ssm.GetParameterAsync(new GetParameterRequest
            {
                Name = "/ssm2/del/tmp",
            }));

        ex.ErrorCode.ShouldBe("ParameterNotFound");

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

        ((resp.DeletedParameters ?? []).Count).ShouldBe(2);
        resp.InvalidParameters.ShouldContain("/ssm2/del/ghost");
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
        names.ShouldContain("/ssm2/desc/alpha");
        names.ShouldContain("/ssm2/desc/beta");
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
        history.Count.ShouldBe(3);
        history[0].Value.ShouldBe("h1");
        history[0].Version.ShouldBe(1);
        history[2].Value.ShouldBe("h3");
        history[2].Version.ShouldBe(3);
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
        tagMap["team"].ShouldBe("platform");
        tagMap["env"].ShouldBe("staging");

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
        tagMap2.Keys.ShouldNotContain("team");
        tagMap2["env"].ShouldBe("staging");
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

        resp.ParameterVersion.ShouldBe(1);
        resp.InvalidLabels.ShouldBeEmpty();
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
        tagMap["env"].ShouldBe("prod");
        tagMap["team"].ShouldBe("backend");

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
        tagMap2.Keys.ShouldNotContain("team");
        tagMap2["env"].ShouldBe("prod");
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
        history.Count.ShouldBe(3);

        var values = history.ConvertAll(h => h.Value);
        values.ShouldContain("v1");
        values.ShouldContain("v2");
        values.ShouldContain("v3");
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
        names.ShouldContain("/qa/ssm/filter/a");
        names.ShouldContain("/qa/ssm/filter/b");
        names.ShouldNotContain("/qa/ssm/other/c");
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

        resp.Parameter.Value.ShouldNotBe("mysecret");

        var resp2 = await _ssm.GetParameterAsync(new GetParameterRequest
        {
            Name = "/qa/ssm/secure",
            WithDecryption = true,
        });

        resp2.Parameter.Value.ShouldBe("mysecret");
    }
}
