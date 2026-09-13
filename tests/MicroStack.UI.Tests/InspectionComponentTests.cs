using Bunit;
using MicroStack.Admin.Contracts;
using MicroStack.UI.Client.Components;
using MicroStack.UI.Client.Services;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.JSInterop;
using Xunit;

namespace MicroStack.UI.Tests;

public sealed class InspectionComponentTests
{
    [Fact]
    public void Payload_precedes_key_facts_and_expandable_secondary_metadata()
    {
        using var context = new BunitContext();
        context.Services.AddSingleton(new AdminApiClient(new HttpClient()));
        var detail = new AdminResourceDetail(new(new("items", "opaque"), "Item"))
        {
            HasContent = true,
            Fields = [new("Partition key", "customer/1"), new AdminField("Stored settings", "{}") { Secondary = true }]
        };
        var view = context.Render<ResourceInspector>(parameters => parameters
            .Add(component => component.Service, "dynamodb")
            .Add(component => component.Detail, detail)
            .Add(component => component.Content, new("json", "application/json", Text: """{"PK":{"S":"customer/1"},"active":{"BOOL":true},"count":{"N":"12"}}""")));
        Assert.True(view.Markup.IndexOf("Escaped content", StringComparison.Ordinal) < view.Markup.IndexOf("Partition key", StringComparison.Ordinal));
        Assert.Contains("DynamoDB JSON", view.Markup);
        Assert.Contains("\"BOOL\": true", view.Find("pre").TextContent);
        Assert.Contains("\"N\": \"12\"", view.Find("pre").TextContent);
        Assert.Contains("Stored settings", view.Find("details").TextContent);
        Assert.False(view.Find("details").HasAttribute("open"));
    }

    [Fact]
    public async Task Exact_summary_duplicates_appear_once_and_remain_fully_copyable()
    {
        using var context = new BunitContext();
        context.JSInterop.SetupVoid("microstack.copy", _ => true).SetVoidResult();
        context.Services.AddSingleton(new AdminApiClient(new HttpClient()));
        var sent = new AdminField("Sent", "2026-09-13T10:00:00.1234567Z", Format: "datetime");
        var detail = new AdminResourceDetail(new(new("messages", "message"), "Message"))
        {
            Summary = [sent],
            Fields = [sent, new("Receive count", "0"), new AdminField("Attributes", "{}") { Secondary = true }]
        };
        var view = context.Render<ResourceInspector>(parameters => parameters.Add(component => component.Detail, detail));
        Assert.Single(view.FindAll("dt").Where(element => element.TextContent == "Sent"));
        Assert.DoesNotContain(view.FindAll(".metadata dt"), element => element.TextContent == "Sent");
        Assert.Contains("Receive count", view.Find(".entry-facts").TextContent);
        Assert.Contains("Attributes", view.Find("details").TextContent);
        await view.Find("button[aria-label='Copy Sent']").ClickAsync(new());
        Assert.Equal(sent.Value, context.JSInterop.Invocations["microstack.copy"][0].Arguments[0]);
    }

    [Fact]
    public void Summary_deduplication_keeps_different_values_secondary_and_sensitive_reveal_fields()
    {
        using var context = new BunitContext();
        context.Services.AddSingleton(new AdminApiClient(new HttpClient()));
        var detail = new AdminResourceDetail(new(new("items", "item"), "Item"))
        {
            Summary = [new("Key", "short"), new("Settings", "{}"), new("Secret", null, Sensitive: true)],
            Fields =
            [
                new("Key", "full-key"),
                new AdminField("Settings", "{}") { Secondary = true },
                new("Secret", null, Sensitive: true, CanReveal: true)
            ]
        };
        var view = context.Render<ResourceInspector>(parameters => parameters.Add(component => component.Detail, detail));
        Assert.Contains("full-key", view.Find(".entry-facts").TextContent);
        Assert.Contains("Settings", view.Find("details").TextContent);
        Assert.Contains("Reveal Secret", view.Find(".entry-facts").TextContent);
        Assert.Empty(view.FindAll(".resource-facts button[aria-label='Copy Secret']"));
    }

    [Theory]
    [InlineData("Standard", "available", null)]
    [InlineData("FIFO", "Available", null)]
    [InlineData(null, "available", "available")]
    [InlineData(" ", "available", "available")]
    [InlineData("Message", "In flight", "In flight")]
    [InlineData("Rule", "DISABLED", "DISABLED")]
    public void Meaningful_types_replace_only_generic_available_status(string? type, string status, string? expected)
    {
        var resource = new AdminResourceSummary(new("kind", "id"), "Resource", Status: status) { Type = type };
        Assert.Equal(expected, InspectionProfile.DisplayStatus(resource));
    }

    [Fact]
    public void Generic_child_filter_uses_declared_kind_label_without_implying_body_search()
    {
        var detail = new AdminResourceDetail(new(new("roots", "root"), "Root"))
        {
            HasChildren = true,
            ChildKinds = [new("opaque", "Configurations")]
        };
        Assert.Equal("Filter configurations by name or identifier", InspectionProfile.ChildFilterLabel("custom", detail, null));
        Assert.Equal("Filter entries by name or identifier", InspectionProfile.ChildFilterLabel("custom", detail with { ChildKinds = [] }, null));
    }

    [Fact]
    public void Configuration_only_resource_keeps_sensitive_reveal_controls_visible()
    {
        using var context = new BunitContext();
        context.Services.AddSingleton(new AdminApiClient(new HttpClient()));
        var detail = new AdminResourceDetail(new(new("connections", "connection"), "Connection"))
        {
            Fields = [new AdminField("Authentication", "do-not-display", Sensitive: true, CanReveal: true) { Secondary = true }]
        };
        var view = context.Render<ResourceInspector>(parameters => parameters.Add(component => component.Detail, detail));
        Assert.Empty(view.FindAll("details"));
        Assert.Empty(view.FindAll("section[aria-label='Content preview']"));
        Assert.Contains("Reveal Authentication", view.Find("button").TextContent);
        Assert.DoesNotContain("do-not-display", view.Markup);
    }

    [Fact]
    public async Task Icon_copy_preserves_full_identifier_and_accessible_feedback()
    {
        using var context = new BunitContext();
        context.JSInterop.SetupVoid("microstack.copy", _ => true).SetVoidResult();
        const string text = "arn:aws:s3:::bucket/folder/a?x=#文";
        var view = context.Render<CopyButton>(parameters => parameters.Add(component => component.Text, text).Add(component => component.Label, "Copy identifier"));
        Assert.Equal("Copy identifier", view.Find("button").GetAttribute("aria-label"));
        Assert.Equal("Copy identifier", view.Find("button").GetAttribute("title"));
        Assert.Single(view.FindAll("svg"));
        var copy = view.Find("button").ClickAsync(new());
        view.WaitForAssertion(() => Assert.Equal("Copied.", view.Find("[role=status]").TextContent));
        Assert.Equal(text, context.JSInterop.Invocations["microstack.copy"][0].Arguments[0]);
        Assert.Equal("Copied.", view.Find("[role=status]").TextContent);
        view.Render();
        Assert.Equal("Copied.", view.Find("[role=status]").TextContent);
        await copy;
        Assert.Empty(view.Find("[role=status]").TextContent);
    }

    [Fact]
    public async Task Repeated_copy_cancels_previous_dismissal_without_clearing_new_feedback()
    {
        using var context = new BunitContext();
        context.JSInterop.SetupVoid("microstack.copy", _ => true).SetVoidResult();
        var view = context.Render<CopyButton>(parameters => parameters.Add(component => component.Text, "identifier"));
        var firstCopy = view.Find("button").ClickAsync(new());
        view.WaitForAssertion(() => Assert.Equal("Copied.", view.Find("[role=status]").TextContent));
        var secondCopy = view.Find("button").ClickAsync(new());
        await firstCopy;
        Assert.False(secondCopy.IsCompleted);
        Assert.Equal("Copied.", view.Find("[role=status]").TextContent);
        await secondCopy;
        Assert.Empty(view.Find("[role=status]").TextContent);
    }

    [Fact]
    public async Task Changing_copy_text_cancels_feedback_and_disposal_cancels_pending_dismissal()
    {
        using var context = new BunitContext();
        context.JSInterop.SetupVoid("microstack.copy", _ => true).SetVoidResult();
        var view = context.Render<CopyButton>(parameters => parameters.Add(component => component.Text, "first"));
        var copy = view.Find("button").ClickAsync(new());
        view.WaitForAssertion(() => Assert.Equal("Copied.", view.Find("[role=status]").TextContent));
        view.Render(parameters => parameters.Add(component => component.Text, "second"));
        await copy;
        Assert.Empty(view.Find("[role=status]").TextContent);

        var pending = view.Find("button").ClickAsync(new());
        view.WaitForAssertion(() => Assert.Equal("Copied.", view.Find("[role=status]").TextContent));
        await view.InvokeAsync(() => view.Instance.Dispose());
        await pending;
    }

    [Fact]
    public async Task Copy_failure_explains_manual_fallback()
    {
        using var context = new BunitContext();
        context.JSInterop.SetupVoid("microstack.copy", _ => true).SetException(new JSException("Denied"));
        var view = context.Render<CopyButton>(parameters => parameters.Add(component => component.Text, "identifier"));
        await view.Find("button").ClickAsync(new());
        Assert.Contains("copy it manually", view.Find("[role=status]").TextContent);
    }

    [Fact]
    public void Summary_formats_typed_values_without_leaking_sensitive_values()
    {
        using var context = new BunitContext();
        var view = context.Render<ResourceFacts>(parameters => parameters.Add(component => component.Fields,
            [new("Size", "2048", Format: "bytes"), new("When", "2026-09-12T12:00:00+02:00", Format: "datetime"), new("Secret", "do-not-display", Sensitive: true)]));
        Assert.Contains("2,048 bytes", view.Markup);
        Assert.Contains("2026-09-12T10:00:00Z", view.Markup);
        Assert.DoesNotContain("do-not-display", view.Markup);
    }

    [Fact]
    public void Connection_states_remain_truthful_and_external_endpoints_are_not_links()
    {
        using var context = new BunitContext();
        var view = context.Render<ConnectionList>(parameters => parameters
            .Add(component => component.Destination, _ => "/ui/services/sqs")
            .Add(component => component.Connections,
            [
                new("Local", "target", "sqs", [new("queues", "q")]),
                new("Missing", "target", "sqs", [new("queues", "gone")], State: "missing"),
                new("External", "subscription", ExternalUri: "https://external.example", State: "external"),
                new("Unavailable", "target", "sqs", [new("queues", "unavailable")], State: "unavailable")
            ]));
        Assert.Single(view.FindAll("a"));
        Assert.Contains("missing", view.Markup);
        Assert.Contains("external", view.Markup);
        Assert.Contains("unavailable", view.Markup);
    }

    [Fact]
    public void Parent_connection_exposes_original_configuration_even_when_destination_is_missing()
    {
        using var context = new BunitContext();
        var view = context.Render<ConnectionList>(parameters => parameters
            .Add(component => component.Destination, _ => "/ui/services/sqs")
            .Add(component => component.Source, _ => "/ui/services/events?path=opaque")
            .Add(component => component.Connections,
            [
                new AdminConnection("Queue", "target", "sqs", [new("queues", "deleted")], State: "missing")
                { SourceServiceId = "events", SourcePath = [new("event-buses", "default"), new("rules", "orders"), new("targets", "target-1")] }
            ]));
        Assert.Equal("Inspect configuration →", view.Find("a").TextContent);
        Assert.Contains("target-1", view.Markup);
        Assert.DoesNotContain("Inspect destination", view.Markup);
    }

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

        Assert.StartsWith("https://localhost:8443/ui/accounts/000000000000/services/s3", service, StringComparison.Ordinal);
        Assert.Equal(
            "https://localhost:8443/ui/services/sqs?account=000000000000",
            ExplorerLocation.ResolveReturn(navigation, "/services/sqs?account=000000000000"));
        Assert.Equal(
            "https://localhost:8443/ui/accounts/111111111111/services/sqs?tab=connections",
            ExplorerLocation.ResolveReturn(navigation, "/accounts/111111111111/services/sqs?tab=connections"));
        Assert.DoesNotContain("account=", service, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("/accounts/invalid/services/sqs")]
    [InlineData("/accounts/000000000000/services/..")]
    [InlineData("/accounts/000000000000/services/%2e%2e")]
    [InlineData("/accounts/000000000000/overview")]
    public void Account_return_routes_reject_invalid_or_non_explorer_paths(string route) =>
        Assert.Null(ExplorerLocation.SafeReturn(route));

    private sealed class StubNavigationManager : NavigationManager
    {
        internal StubNavigationManager(string baseUri) => Initialize(baseUri, baseUri);
    }
}
