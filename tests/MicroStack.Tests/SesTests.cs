using Amazon;
using Amazon.SimpleEmail;
using Amazon.SimpleEmail.Model;
using Amazon.SimpleEmailV2;
using Amazon.SimpleEmailV2.Model;
using Amazon.Runtime;
using SesV1 = Amazon.SimpleEmail;
using SesV2 = Amazon.SimpleEmailV2;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the SES service handler (v1 + v2).
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_ses.py.
/// </summary>
public sealed class SesTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonSimpleEmailServiceClient _ses;
    private readonly AmazonSimpleEmailServiceV2Client _sesV2;

    public SesTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _ses = CreateSesClient(fixture);
        _sesV2 = CreateSesV2Client(fixture);
    }

    private static AmazonSimpleEmailServiceClient CreateSesClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonSimpleEmailServiceConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonSimpleEmailServiceClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    private static AmazonSimpleEmailServiceV2Client CreateSesV2Client(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonSimpleEmailServiceV2Config
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonSimpleEmailServiceV2Client(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _ses.Dispose();
        _sesV2.Dispose();
        return Task.CompletedTask;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SES v1 — SendEmail
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SendEmail()
    {
        await _ses.VerifyEmailIdentityAsync(new SesV1.Model.VerifyEmailIdentityRequest
        {
            EmailAddress = "sender@example.com",
        });

        var resp = await _ses.SendEmailAsync(new SesV1.Model.SendEmailRequest
        {
            Source = "sender@example.com",
            Destination = new SesV1.Model.Destination
            {
                ToAddresses = ["recipient@example.com"],
            },
            Message = new SesV1.Model.Message
            {
                Subject = new SesV1.Model.Content { Data = "Test Subject" },
                Body = new SesV1.Model.Body
                {
                    Text = new SesV1.Model.Content { Data = "Hello from MicroStack SES" },
                },
            },
        });

        Assert.NotNull(resp.MessageId);
        Assert.Contains("@email.amazonses.com", resp.MessageId);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SES v1 — SendRawEmail
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SendRawEmail()
    {
        await _ses.VerifyEmailIdentityAsync(new SesV1.Model.VerifyEmailIdentityRequest
        {
            EmailAddress = "raw@example.com",
        });

        var raw = "From: raw@example.com\r\nTo: dest@example.com\r\nSubject: Raw\r\nContent-Type: text/plain\r\n\r\nRaw body";
        var resp = await _ses.SendRawEmailAsync(new SesV1.Model.SendRawEmailRequest
        {
            RawMessage = new SesV1.Model.RawMessage
            {
                Data = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(raw)),
            },
        });

        Assert.NotNull(resp.MessageId);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SES v1 — Identity operations
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task VerifyEmailAndListIdentities()
    {
        await _ses.VerifyEmailIdentityAsync(new SesV1.Model.VerifyEmailIdentityRequest
        {
            EmailAddress = "test@example.com",
        });

        var resp = await _ses.ListIdentitiesAsync(new SesV1.Model.ListIdentitiesRequest());
        Assert.Contains("test@example.com", resp.Identities);
    }

    [Fact]
    public async Task VerifyDomainIdentity()
    {
        var resp = await _ses.VerifyDomainIdentityAsync(new SesV1.Model.VerifyDomainIdentityRequest
        {
            Domain = "example.com",
        });

        Assert.NotNull(resp.VerificationToken);
        Assert.True(resp.VerificationToken.Length > 0);

        var ids = await _ses.ListIdentitiesAsync(new SesV1.Model.ListIdentitiesRequest
        {
            IdentityType = "Domain",
        });
        Assert.Contains("example.com", ids.Identities);
    }

    [Fact]
    public async Task GetIdentityVerificationAttributes()
    {
        await _ses.VerifyEmailIdentityAsync(new SesV1.Model.VerifyEmailIdentityRequest
        {
            EmailAddress = "verify@example.com",
        });

        var attrs = await _ses.GetIdentityVerificationAttributesAsync(
            new SesV1.Model.GetIdentityVerificationAttributesRequest
            {
                Identities = ["verify@example.com"],
            });

        Assert.True(attrs.VerificationAttributes.ContainsKey("verify@example.com"));
        Assert.Equal("Success", attrs.VerificationAttributes["verify@example.com"].VerificationStatus.Value);
    }

    [Fact]
    public async Task DeleteIdentity()
    {
        await _ses.VerifyEmailIdentityAsync(new SesV1.Model.VerifyEmailIdentityRequest
        {
            EmailAddress = "delete@example.com",
        });

        await _ses.DeleteIdentityAsync(new SesV1.Model.DeleteIdentityRequest
        {
            Identity = "delete@example.com",
        });

        var resp = await _ses.ListIdentitiesAsync(new SesV1.Model.ListIdentitiesRequest());
        Assert.DoesNotContain("delete@example.com", resp.Identities ?? []);
    }

    [Fact]
    public async Task ListIdentitiesByType()
    {
        await _ses.VerifyEmailIdentityAsync(new SesV1.Model.VerifyEmailIdentityRequest
        {
            EmailAddress = "type@example.com",
        });
        await _ses.VerifyDomainIdentityAsync(new SesV1.Model.VerifyDomainIdentityRequest
        {
            Domain = "type-domain.com",
        });

        var emailIds = await _ses.ListIdentitiesAsync(new SesV1.Model.ListIdentitiesRequest
        {
            IdentityType = "EmailAddress",
        });
        Assert.Contains("type@example.com", emailIds.Identities);

        var domainIds = await _ses.ListIdentitiesAsync(new SesV1.Model.ListIdentitiesRequest
        {
            IdentityType = "Domain",
        });
        Assert.Contains("type-domain.com", domainIds.Identities);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SES v1 — Send Quota
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GetSendQuota()
    {
        var resp = await _ses.GetSendQuotaAsync(new SesV1.Model.GetSendQuotaRequest());
        Assert.Equal(50000.0, resp.Max24HourSend);
        Assert.Equal(14.0, resp.MaxSendRate);
        Assert.True(resp.SentLast24Hours >= 0);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SES v1 — Configuration sets
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ConfigurationSetCrud()
    {
        await _ses.CreateConfigurationSetAsync(new SesV1.Model.CreateConfigurationSetRequest
        {
            ConfigurationSet = new SesV1.Model.ConfigurationSet { Name = "test-config" },
        });

        var desc = await _ses.DescribeConfigurationSetAsync(new SesV1.Model.DescribeConfigurationSetRequest
        {
            ConfigurationSetName = "test-config",
        });
        Assert.Equal("test-config", desc.ConfigurationSet.Name);

        var sets = await _ses.ListConfigurationSetsAsync(new SesV1.Model.ListConfigurationSetsRequest());
        Assert.Contains(sets.ConfigurationSets, s => s.Name == "test-config");

        await _ses.DeleteConfigurationSetAsync(new SesV1.Model.DeleteConfigurationSetRequest
        {
            ConfigurationSetName = "test-config",
        });

        var sets2 = await _ses.ListConfigurationSetsAsync(new SesV1.Model.ListConfigurationSetsRequest());
        Assert.DoesNotContain(sets2.ConfigurationSets ?? [], s => s.Name == "test-config");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SES v1 — Templates
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TemplateCrud()
    {
        await _ses.CreateTemplateAsync(new SesV1.Model.CreateTemplateRequest
        {
            Template = new SesV1.Model.Template
            {
                TemplateName = "test-tpl",
                SubjectPart = "Hello {{name}}",
                TextPart = "Hi {{name}}, order #{{oid}}",
                HtmlPart = "<h1>Hi {{name}}</h1>",
            },
        });

        var resp = await _ses.GetTemplateAsync(new SesV1.Model.GetTemplateRequest
        {
            TemplateName = "test-tpl",
        });
        Assert.Equal("test-tpl", resp.Template.TemplateName);
        Assert.Contains("{{name}}", resp.Template.SubjectPart);

        var listed = await _ses.ListTemplatesAsync(new SesV1.Model.ListTemplatesRequest());
        Assert.Contains(listed.TemplatesMetadata, t => t.Name == "test-tpl");

        await _ses.UpdateTemplateAsync(new SesV1.Model.UpdateTemplateRequest
        {
            Template = new SesV1.Model.Template
            {
                TemplateName = "test-tpl",
                SubjectPart = "Updated {{name}}",
                TextPart = "Updated",
                HtmlPart = "<p>Updated</p>",
            },
        });

        var resp2 = await _ses.GetTemplateAsync(new SesV1.Model.GetTemplateRequest
        {
            TemplateName = "test-tpl",
        });
        Assert.Contains("Updated", resp2.Template.SubjectPart);

        await _ses.DeleteTemplateAsync(new SesV1.Model.DeleteTemplateRequest
        {
            TemplateName = "test-tpl",
        });

        // After deletion, get should throw
        var ex = await Assert.ThrowsAsync<Amazon.SimpleEmail.Model.TemplateDoesNotExistException>(() =>
            _ses.GetTemplateAsync(new SesV1.Model.GetTemplateRequest
            {
                TemplateName = "test-tpl",
            }));
        Assert.NotNull(ex);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SES v1 — SendTemplatedEmail
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SendTemplatedEmail()
    {
        await _ses.VerifyEmailIdentityAsync(new SesV1.Model.VerifyEmailIdentityRequest
        {
            EmailAddress = "tpl-sender@example.com",
        });
        await _ses.CreateTemplateAsync(new SesV1.Model.CreateTemplateRequest
        {
            Template = new SesV1.Model.Template
            {
                TemplateName = "send-tpl",
                SubjectPart = "Hey {{name}}",
                TextPart = "Hi {{name}}",
                HtmlPart = "<h1>Hi {{name}}</h1>",
            },
        });

        var resp = await _ses.SendTemplatedEmailAsync(new SesV1.Model.SendTemplatedEmailRequest
        {
            Source = "tpl-sender@example.com",
            Destination = new SesV1.Model.Destination
            {
                ToAddresses = ["r@example.com"],
            },
            Template = "send-tpl",
            TemplateData = System.Text.Json.JsonSerializer.Serialize(new { name = "Alice" }),
        });

        Assert.NotNull(resp.MessageId);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SES v1 — VerifyDomainDkim
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task VerifyDomainDkim()
    {
        var resp = await _ses.VerifyDomainDkimAsync(new SesV1.Model.VerifyDomainDkimRequest
        {
            Domain = "dkim.example.com",
        });

        Assert.Equal(3, resp.DkimTokens.Count);
        Assert.All(resp.DkimTokens, t => Assert.True(t.Length > 0));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SES v1 — Send statistics
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GetSendStatistics()
    {
        // Send an email first to have data
        await _ses.VerifyEmailIdentityAsync(new SesV1.Model.VerifyEmailIdentityRequest
        {
            EmailAddress = "stats@example.com",
        });
        await _ses.SendEmailAsync(new SesV1.Model.SendEmailRequest
        {
            Source = "stats@example.com",
            Destination = new SesV1.Model.Destination { ToAddresses = ["to@example.com"] },
            Message = new SesV1.Model.Message
            {
                Subject = new SesV1.Model.Content { Data = "Stat test" },
                Body = new SesV1.Model.Body { Text = new SesV1.Model.Content { Data = "body" } },
            },
        });

        var resp = await _ses.GetSendStatisticsAsync(new SesV1.Model.GetSendStatisticsRequest());
        Assert.NotNull(resp.SendDataPoints);
        Assert.True(resp.SendDataPoints.Count >= 1);
        Assert.True(resp.SendDataPoints[0].DeliveryAttempts >= 1);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SES v2 — SendEmail
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task V2SendEmail()
    {
        var resp = await _sesV2.SendEmailAsync(new SesV2.Model.SendEmailRequest
        {
            FromEmailAddress = "sender@example.com",
            Destination = new SesV2.Model.Destination
            {
                ToAddresses = ["recipient@example.com"],
            },
            Content = new EmailContent
            {
                Simple = new SesV2.Model.Message
                {
                    Subject = new SesV2.Model.Content { Data = "Test Subject" },
                    Body = new SesV2.Model.Body
                    {
                        Text = new SesV2.Model.Content { Data = "Hello world" },
                    },
                },
            },
        });

        Assert.StartsWith("ministack-", resp.MessageId);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SES v2 — Email identity CRUD
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task V2EmailIdentityCrud()
    {
        var createResp = await _sesV2.CreateEmailIdentityAsync(new SesV2.Model.CreateEmailIdentityRequest
        {
            EmailIdentity = "test-domain.com",
        });
        Assert.True(createResp.VerifiedForSendingStatus);

        var getResp = await _sesV2.GetEmailIdentityAsync(new SesV2.Model.GetEmailIdentityRequest
        {
            EmailIdentity = "test-domain.com",
        });
        Assert.True(getResp.VerifiedForSendingStatus);

        var listResp = await _sesV2.ListEmailIdentitiesAsync(new SesV2.Model.ListEmailIdentitiesRequest());
        var names = listResp.EmailIdentities.ConvertAll(e => e.IdentityName);
        Assert.Contains("test-domain.com", names);

        await _sesV2.DeleteEmailIdentityAsync(new SesV2.Model.DeleteEmailIdentityRequest
        {
            EmailIdentity = "test-domain.com",
        });

        var list2 = await _sesV2.ListEmailIdentitiesAsync(new SesV2.Model.ListEmailIdentitiesRequest());
        var names2 = list2.EmailIdentities.ConvertAll(e => e.IdentityName);
        Assert.DoesNotContain("test-domain.com", names2);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SES v2 — Configuration set CRUD
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task V2ConfigurationSetCrud()
    {
        await _sesV2.CreateConfigurationSetAsync(new SesV2.Model.CreateConfigurationSetRequest
        {
            ConfigurationSetName = "my-cfg-set",
        });

        var getResp = await _sesV2.GetConfigurationSetAsync(new SesV2.Model.GetConfigurationSetRequest
        {
            ConfigurationSetName = "my-cfg-set",
        });
        Assert.Equal("my-cfg-set", getResp.ConfigurationSetName);

        var listResp = await _sesV2.ListConfigurationSetsAsync(new SesV2.Model.ListConfigurationSetsRequest());
        Assert.Contains("my-cfg-set", listResp.ConfigurationSets);

        await _sesV2.DeleteConfigurationSetAsync(new SesV2.Model.DeleteConfigurationSetRequest
        {
            ConfigurationSetName = "my-cfg-set",
        });

        var list2 = await _sesV2.ListConfigurationSetsAsync(new SesV2.Model.ListConfigurationSetsRequest());
        Assert.DoesNotContain("my-cfg-set", list2.ConfigurationSets);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SES v2 — GetAccount
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task V2GetAccount()
    {
        var resp = await _sesV2.GetAccountAsync(new SesV2.Model.GetAccountRequest());
        Assert.True(resp.SendingEnabled);
        Assert.True(resp.ProductionAccessEnabled);
        Assert.Equal(50000.0, resp.SendQuota.Max24HourSend);
    }
}
