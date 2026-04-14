using Amazon;
using Amazon.CertificateManager;
using Amazon.CertificateManager.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the ACM (Certificate Manager) service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_acm.py.
/// </summary>
public sealed class AcmTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonCertificateManagerClient _acm;

    public AcmTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _acm = CreateAcmClient(fixture);
    }

    private static AmazonCertificateManagerClient CreateAcmClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonCertificateManagerConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonCertificateManagerClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _acm.Dispose();
        return Task.CompletedTask;
    }

    // -- RequestCertificate ---------------------------------------------------

    [Fact]
    public async Task RequestCertificate()
    {
        var resp = await _acm.RequestCertificateAsync(new RequestCertificateRequest
        {
            DomainName = "example.com",
            ValidationMethod = ValidationMethod.DNS,
            SubjectAlternativeNames = ["www.example.com"],
        });

        resp.CertificateArn.ShouldStartWith("arn:aws:acm:us-east-1:000000000000:certificate/");
    }

    // -- DescribeCertificate --------------------------------------------------

    [Fact]
    public async Task DescribeCertificate()
    {
        var reqResp = await _acm.RequestCertificateAsync(new RequestCertificateRequest
        {
            DomainName = "describe.example.com",
        });

        var resp = await _acm.DescribeCertificateAsync(new DescribeCertificateRequest
        {
            CertificateArn = reqResp.CertificateArn,
        });

        resp.Certificate.DomainName.ShouldBe("describe.example.com");
        resp.Certificate.Status.ShouldBe(CertificateStatus.ISSUED);
        (resp.Certificate.DomainValidationOptions.Count >= 1).ShouldBe(true);
        resp.Certificate.DomainValidationOptions[0].ResourceRecord.ShouldNotBeNull();
    }

    // -- ListCertificates -----------------------------------------------------

    [Fact]
    public async Task ListCertificates()
    {
        var reqResp = await _acm.RequestCertificateAsync(new RequestCertificateRequest
        {
            DomainName = "list.example.com",
        });

        var resp = await _acm.ListCertificatesAsync(new ListCertificatesRequest());
        var arns = resp.CertificateSummaryList.ConvertAll(c => c.CertificateArn);
        arns.ShouldContain(reqResp.CertificateArn);
    }

    // -- Tags (Add, List, Remove) ---------------------------------------------

    [Fact]
    public async Task Tags()
    {
        var reqResp = await _acm.RequestCertificateAsync(new RequestCertificateRequest
        {
            DomainName = "tags.example.com",
        });

        var arn = reqResp.CertificateArn;

        await _acm.AddTagsToCertificateAsync(new AddTagsToCertificateRequest
        {
            CertificateArn = arn,
            Tags =
            [
                new Tag { Key = "env", Value = "test" },
                new Tag { Key = "team", Value = "platform" },
            ],
        });

        var tagsResp = await _acm.ListTagsForCertificateAsync(new ListTagsForCertificateRequest
        {
            CertificateArn = arn,
        });

        tagsResp.Tags.ShouldContain(t => t.Key == "env" && t.Value == "test");

        await _acm.RemoveTagsFromCertificateAsync(new RemoveTagsFromCertificateRequest
        {
            CertificateArn = arn,
            Tags = [new Tag { Key = "team", Value = "platform" }],
        });

        var tags2 = await _acm.ListTagsForCertificateAsync(new ListTagsForCertificateRequest
        {
            CertificateArn = arn,
        });

        tags2.Tags.ShouldNotContain(t => t.Key == "team");
    }

    // -- GetCertificate -------------------------------------------------------

    [Fact]
    public async Task GetCertificate()
    {
        var reqResp = await _acm.RequestCertificateAsync(new RequestCertificateRequest
        {
            DomainName = "pem.example.com",
        });

        var resp = await _acm.GetCertificateAsync(new GetCertificateRequest
        {
            CertificateArn = reqResp.CertificateArn,
        });

        resp.Certificate.ShouldContain("BEGIN CERTIFICATE");
    }

    // -- ImportCertificate ----------------------------------------------------

    [Fact]
    public async Task ImportCertificate()
    {
        var fakeCert = System.Text.Encoding.UTF8.GetBytes("-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----");
        var fakeKey = System.Text.Encoding.UTF8.GetBytes("-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----");

        var resp = await _acm.ImportCertificateAsync(new ImportCertificateRequest
        {
            Certificate = new MemoryStream(fakeCert),
            PrivateKey = new MemoryStream(fakeKey),
        });

        var desc = await _acm.DescribeCertificateAsync(new DescribeCertificateRequest
        {
            CertificateArn = resp.CertificateArn,
        });

        desc.Certificate.Type.ShouldBe(CertificateType.IMPORTED);
    }

    // -- DeleteCertificate ----------------------------------------------------

    [Fact]
    public async Task DeleteCertificate()
    {
        var reqResp = await _acm.RequestCertificateAsync(new RequestCertificateRequest
        {
            DomainName = "delete.example.com",
        });

        await _acm.DeleteCertificateAsync(new DeleteCertificateRequest
        {
            CertificateArn = reqResp.CertificateArn,
        });

        var resp = await _acm.ListCertificatesAsync(new ListCertificatesRequest());
        var arns = resp.CertificateSummaryList.ConvertAll(c => c.CertificateArn);
        arns.ShouldNotContain(reqResp.CertificateArn);
    }

    // -- DescribeCertificate not found ----------------------------------------

    [Fact]
    public async Task DescribeCertificateNotFound()
    {
        var ex = await Should.ThrowAsync<ResourceNotFoundException>(() =>
            _acm.DescribeCertificateAsync(new DescribeCertificateRequest
            {
                CertificateArn = "arn:aws:acm:us-east-1:000000000000:certificate/nonexistent",
            }));

        ex.ErrorCode.ShouldBe("ResourceNotFoundException");
    }
}
