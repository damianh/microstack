using Bunit;
using MicroStack.Admin.Contracts;
using MicroStack.UI.Client.Components;
using MicroStack.UI.Client.Services;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MicroStack.UI.Tests;

public sealed class InspectionComponentTests
{
    [Fact]
    public void Text_content_is_escaped_and_never_creates_elements()
    {
        using var context = new BunitContext();
        var view = context.Render<ContentPreview>(parameters => parameters.Add(component => component.Content,
            new("text", "text/html", Text: "<script>alert('x')</script><img src=x onerror=alert(1)>")));
        Assert.Empty(view.FindAll("script"));
        Assert.Empty(view.FindAll("img"));
        Assert.Contains("<script>", view.Find("pre").TextContent);
    }

    [Fact]
    public void Sensitive_content_is_masked_even_if_server_accidentally_includes_text()
    {
        using var context = new BunitContext();
        var view = context.Render<ContentPreview>(parameters => parameters.Add(component => component.Content,
            new("text", "text/plain", Text: "do-not-display", Sensitive: true)));
        Assert.DoesNotContain("do-not-display", view.Markup);
        Assert.Empty(view.FindAll("pre"));
        Assert.Contains("masked", view.Markup);
    }

    [Theory]
    [InlineData("binary")]
    [InlineData("oversized")]
    [InlineData("unavailable")]
    public void Metadata_only_content_does_not_render_body(string kind)
    {
        using var context = new BunitContext();
        var view = context.Render<ContentPreview>(parameters => parameters.Add(component => component.Content,
            new(kind, "application/octet-stream", Text: "do-not-display", Reason: "Metadata only.")));
        Assert.DoesNotContain("do-not-display", view.Markup);
        Assert.Contains("Metadata only.", view.Markup);
    }

    [Fact]
    public void Preview_limit_counts_utf8_bytes_not_characters()
    {
        using var context = new BunitContext();
        var view = context.Render<ContentPreview>(parameters => parameters.Add(component => component.Content,
            new("text", "text/plain", Text: new string('é', 524_289))));
        Assert.Empty(view.FindAll("pre"));
        Assert.Contains("1 MiB", view.Markup);
    }

    [Fact]
    public void Exact_preview_limit_is_allowed()
    {
        using var context = new BunitContext();
        var text = new string('a', 1_048_576);
        var view = context.Render<ContentPreview>(parameters => parameters.Add(component => component.Content, new("text", "text/plain", Text: text)));
        Assert.Equal(text, view.Find("pre").TextContent);
    }

    [Fact]
    public void Malformed_json_preserves_original_text_and_explains_it()
    {
        using var context = new BunitContext();
        var view = context.Render<ContentPreview>(parameters => parameters.Add(component => component.Content, new("json", "application/json", Text: "{bad")));
        Assert.Equal("{bad", view.Find("pre").TextContent);
        Assert.Contains("not valid JSON", view.Markup);
    }

    [Fact]
    public void Private_sensitive_fields_have_no_reveal_button()
    {
        using var context = new BunitContext();
        context.Services.AddSingleton(new AdminApiClient(new HttpClient()));
        var view = context.Render<FieldList>(parameters => parameters.Add(component => component.Fields,
            [new AdminField("Private key", "server-should-mask", Sensitive: true, CanReveal: false)]));
        Assert.DoesNotContain("server-should-mask", view.Markup);
        Assert.Empty(view.FindAll("button"));
        Assert.Contains("Not revealable", view.Markup);
    }

    [Fact]
    public void Service_picker_uses_catalog_labels_and_alphabetical_order()
    {
        using var context = new BunitContext();
        var services = new[]
        {
            new AdminService("z", "Z service", "Zulu", "Test", "s3", "z", "enabled", "account"),
            new AdminService("a", "A service", "Alpha", "Test", "sqs", "a", "disabled", "account")
        };
        var view = context.Render<ServicePicker>(parameters => parameters.Add(component => component.Services, services));
        var options = view.FindAll("[role=option]");
        Assert.Equal(2, options.Count);
        Assert.Contains("Alpha", options[0].TextContent);
        Assert.Contains("Disabled", options[0].TextContent);
        view.Find("input").Input("Zulu");
        Assert.Single(view.FindAll("[role=option]"));
    }

    [Fact]
    public void Utc_timestamp_preserves_source_precision()
    {
        var timestamp = DateTimeOffset.Parse("2026-09-12T13:42:03.1234567+02:00", System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal("2026-09-12T11:42:03.1234567Z", ExplorerLocation.Utc(timestamp));
    }

    [Theory]
    [InlineData("000000000000", true)]
    [InlineData("123456789012", true)]
    [InlineData("123", false)]
    [InlineData("１２３４５６７８９０１２", false)]
    [InlineData("12345678901a", false)]
    public void Account_requires_twelve_ascii_digits(string account, bool expected) => Assert.Equal(expected, ExplorerLocation.ValidAccount(account));

    [Fact]
    public void Resource_identity_roundtrips_opaque_keys()
    {
        AdminKey[] keys = [new("buckets", "bucket"), new("objects", "a/b?x=#文"), new("items", """{"PK":{"S":"customer/1"}}""")];
        Assert.Equal(keys, ExplorerLocation.DecodePath(ExplorerLocation.EncodePath(keys)));
    }

    [Theory]
    [InlineData("//evil.example/")]
    [InlineData("https://evil.example/")]
    [InlineData("/\\evil.example/")]
    [InlineData("/requests")]
    public void Return_context_rejects_non_inspector_urls(string url) => Assert.Null(ExplorerLocation.SafeReturn(url));

    [Fact]
    public void Explorer_urls_stay_under_ui_base_path()
    {
        var navigation = new StubNavigationManager("https://localhost:8443/ui/");
        var service = ExplorerLocation.ServiceUrl(navigation, "s3", "000000000000");

        Assert.StartsWith("https://localhost:8443/ui/services/s3", service, StringComparison.Ordinal);
        Assert.Equal(
            "https://localhost:8443/ui/services/sqs?account=000000000000",
            ExplorerLocation.ResolveReturn(navigation, "/services/sqs?account=000000000000"));
    }

    private sealed class StubNavigationManager : NavigationManager
    {
        internal StubNavigationManager(string baseUri) => Initialize(baseUri, baseUri);
    }
}
