using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Serialization.Metadata;
using Bunit;
using MicroStack.Admin.Contracts;
using MicroStack.UI.Client.Components;
using MicroStack.UI.Client.Pages;
using MicroStack.UI.Client.Services;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MicroStack.UI.Tests;

public sealed class ResourceExplorerTests
{
    [Theory]
    [InlineData("/")]
    [InlineData("/resources")]
    [InlineData("/services/sqs")]
    public void Legacy_routes_redirect_to_account_scope_and_preserve_inspection_query(string route)
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        var state = "&tab=configuration&filter=orders&cursor=opaque-page&returnTo=" +
            Uri.EscapeDataString("/services/sns?account=111111111111");
        var view = fixture.RenderLegacy(route + "?account=111111111111" + state);
        view.WaitForAssertion(() => Assert.Single(view.FindAll("#account-id")));
        var uri = new Uri(fixture.Navigation.Uri);
        Assert.Equal("/accounts/111111111111/services" + (route == "/services/sqs" ? "/sqs" : ""), uri.AbsolutePath);
        var query = Query(uri);
        Assert.False(query.ContainsKey("account"));
        Assert.Equal("configuration", query["tab"]);
        Assert.Equal("orders", query["filter"]);
        Assert.Equal("opaque-page", query["cursor"]);
        Assert.Equal("/services/sns?account=111111111111", query["returnTo"]);
    }

    [Fact]
    public void Header_and_resource_links_preserve_route_account_without_account_query()
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        var app = fixture.RenderLegacy("/accounts/111111111111/services/sqs");
        app.WaitForAssertion(() => Assert.Single(app.FindAll(".resource-button")));
        foreach (var link in app.FindAll(".brand,.global-nav a:first-child,.service-breadcrumb>a,.resource-button"))
        {
            Assert.Contains("/accounts/111111111111/services", link.GetAttribute("href"), StringComparison.Ordinal);
            Assert.DoesNotContain("account=", link.GetAttribute("href"), StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("overview")]
    [InlineData("requests")]
    public async Task Instance_pages_keep_explorer_account_without_claiming_account_scope(string page)
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        var view = fixture.Render("?account=111111111111");
        view.WaitForAssertion(() => Assert.Single(view.FindAll(".app-header #account-id")));
        Assert.Empty(view.FindAll("main #account-id"));
        await view.InvokeAsync(() => fixture.Navigation.NavigateTo("/" + page));
        view.WaitForAssertion(() => Assert.Equal("Instance-wide", view.Find(".header-context").TextContent.Trim()));
        Assert.Empty(view.FindAll("#account-id"));
        foreach (var link in view.FindAll(".brand,.global-nav a:first-child"))
            Assert.Equal("http://localhost/accounts/111111111111/services", link.GetAttribute("href"));
        await view.InvokeAsync(() => fixture.Navigation.NavigateTo(view.Find(".global-nav a:first-child").GetAttribute("href")!));
        view.WaitForAssertion(() => Assert.Single(view.FindAll(".directory-service")));
        Assert.Contains("/accounts/111111111111/services", fixture.Navigation.Uri, StringComparison.Ordinal);
        Assert.Equal("111111111111", view.Find("select").GetAttribute("value"));
    }

    [Theory]
    [InlineData("/overview")]
    [InlineData("/requests")]
    public void Reloading_instance_page_restores_tab_account_for_services_and_brand(string page)
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        fixture.JSInterop.Setup<string?>("sessionStorage.getItem", ExplorerAccountState.StorageKey).SetResult("111111111111");
        var view = fixture.RenderLegacy(page);
        view.WaitForAssertion(() => Assert.Equal("http://localhost/accounts/111111111111/services",
            view.Find(".global-nav a:first-child").GetAttribute("href")));
        Assert.Equal("http://localhost/accounts/111111111111/services", view.Find(".brand").GetAttribute("href"));
        Assert.Equal("Instance-wide", view.Find(".header-context").TextContent.Trim());
        Assert.Empty(view.FindAll("#account-id"));
    }

    [Theory]
    [InlineData("/", "111111111111")]
    [InlineData("/resources", "111111111111")]
    [InlineData("/services/sqs", "111111111111")]
    [InlineData("/services/sqs?account=222222222222", "222222222222")]
    [InlineData("/accounts/222222222222/services", "222222222222")]
    public void Saved_account_is_used_only_when_url_does_not_specify_one(string route, string expected)
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        fixture.JSInterop.Setup<string?>("sessionStorage.getItem", ExplorerAccountState.StorageKey).SetResult("111111111111");
        var view = fixture.RenderLegacy(route);
        view.WaitForAssertion(() => Assert.Single(view.FindAll("#account-id")));
        Assert.Contains("/accounts/" + expected + "/services", fixture.Navigation.Uri, StringComparison.Ordinal);
    }

    [Fact]
    public void Deleted_saved_account_requires_explicit_selection()
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        fixture.Accounts = ["000000000000"];
        fixture.JSInterop.Setup<string?>("sessionStorage.getItem", ExplorerAccountState.StorageKey).SetResult("111111111111");
        var view = fixture.RenderLegacy("/");
        view.WaitForAssertion(() => Assert.Contains("has no retained resources", view.Find("[role=alert]").TextContent));
        Assert.Contains("/accounts/111111111111/services", fixture.Navigation.Uri, StringComparison.Ordinal);
        Assert.Single(view.FindAll("#account-id option:not([disabled])"));
        Assert.Empty(fixture.ResourceQueries);
    }

    [Fact]
    public void Storage_failure_is_visible_without_blocking_account_inspection()
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        fixture.JSInterop.SetupVoid("sessionStorage.setItem", ExplorerAccountState.StorageKey, "111111111111")
            .SetException(new Microsoft.JSInterop.JSException("Storage blocked"));
        var view = fixture.Render("?account=111111111111");
        view.WaitForAssertion(() => Assert.Contains("cannot be saved", view.Find("[role=alert]").TextContent));
        Assert.Single(view.FindAll(".resource-button"));
        Assert.Equal("http://localhost/accounts/111111111111/services", view.Find(".brand").GetAttribute("href"));
    }

    [Fact]
    public async Task Known_account_picker_switches_immediately_and_clears_selection()
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        var view = fixture.Render(fixture.PathQuery + "&item=" +
            Uri.EscapeDataString(ExplorerLocation.EncodePath([new("messages", "message")])));
        view.WaitForAssertion(() => Assert.Equal(3, view.FindAll("#account-id option").Count));
        Assert.Empty(view.FindAll(".scope input"));
        Assert.Empty(view.FindAll(".scope button"));
        Assert.Contains("(default)", view.Find("#account-id option").TextContent);
        await view.Find("#account-id").ChangeAsync(new() { Value = "111111111111" });
        view.WaitForAssertion(() => Assert.Equal("111111111111", fixture.ResourceQueries.Last()["accountId"]));
        Assert.DoesNotContain("path=", fixture.Navigation.Uri, StringComparison.Ordinal);
        Assert.DoesNotContain("item=", fixture.Navigation.Uri, StringComparison.Ordinal);
        Assert.Empty(view.FindAll("#resource-title"));
    }

    [Fact]
    public void Only_default_account_is_plain_text()
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        fixture.Accounts = ["000000000000"];
        var view = fixture.RenderDirectory();
        view.WaitForAssertion(() => Assert.Equal("000000000000", view.Find(".scope code").TextContent));
        Assert.Contains("(default)", view.Find(".scope").TextContent);
        Assert.Empty(view.FindAll(".scope select,.scope input,.scope button"));
    }

    [Fact]
    public async Task Unknown_deep_link_does_not_become_a_selectable_account()
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        fixture.Accounts = ["000000000000"];
        var view = fixture.Render("?account=999999999999");
        view.WaitForAssertion(() => Assert.Contains("Select a known account", view.Find("[role=alert]").TextContent));
        Assert.Empty(fixture.ResourceQueries);
        Assert.DoesNotContain(view.FindAll("#account-id option"), option => option.GetAttribute("value") == "999999999999");
        await view.Find("#account-id").ChangeAsync(new() { Value = "000000000000" });
        view.WaitForAssertion(() => Assert.Single(fixture.ResourceQueries));
        Assert.Empty(view.FindAll("[role=alert]"));
        Assert.Single(view.FindAll(".scope code"));
    }

    [Fact]
    public void Account_discovery_failure_is_explicit_not_an_empty_or_fabricated_list()
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        fixture.FailAccounts = true;
        var view = fixture.Render();
        view.WaitForAssertion(() => Assert.Single(view.FindAll("[role=alert]")));
        Assert.Empty(view.FindAll("#account-id"));
        Assert.Empty(fixture.ResourceQueries);
    }

    [Fact]
    public async Task Refresh_removes_deleted_accounts_and_requires_an_explicit_known_selection()
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        var view = fixture.Render(fixture.PathQuery + "&account=111111111111");
        view.WaitForAssertion(() => Assert.Single(view.FindAll("#resource-title")));
        fixture.Accounts = ["000000000000"];
        await view.Find(".resource-heading button").ClickAsync(new());
        view.WaitForAssertion(() => Assert.Contains("has no retained resources", view.Find("[role=alert]").TextContent));
        Assert.Empty(view.FindAll("#resource-title"));
        Assert.Empty(view.FindAll(".resource-button"));
        Assert.DoesNotContain(view.FindAll("#account-id option"), option => option.GetAttribute("value") == "111111111111");
        Assert.Contains("/accounts/111111111111/services/", fixture.Navigation.Uri, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Directory_keeps_account_in_header_and_count_with_filters()
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        var view = fixture.RenderDirectory();
        view.WaitForAssertion(() => Assert.Single(view.FindAll(".directory-service")));
        Assert.Empty(view.FindAll(".workspace-context"));
        Assert.Single(view.FindAll(".app-header #account-id"));
        Assert.Empty(view.FindAll(".directory-heading #account-id"));
        Assert.Equal("Services", view.Find(".directory-heading h1").TextContent);
        Assert.Equal("1 of 1 services", view.Find(".directory-tools [role=status]").TextContent);

        await view.Find("input[aria-label='Find services']").InputAsync(new() { Value = "no-match" });
        view.WaitForAssertion(() => Assert.Equal("0 of 1 services", view.Find(".directory-tools [role=status]").TextContent));
        Assert.Empty(view.FindAll(".directory-service"));
        Assert.Contains("No services match", view.Find(".empty-list").TextContent);
        Assert.Single(view.FindAll("#account-id"));
    }

    [Theory]
    [InlineData("sqs", "queues", "Standard", "Visible messages")]
    [InlineData("s3", "buckets", "Bucket", "Object count")]
    public void Resource_selector_omits_status_and_summary_but_inspector_keeps_them(
        string service, string kind, string type, string countLabel)
    {
        using var fixture = new ExplorerFixture(service, [new(kind, kind)]);
        fixture.Detail = fixture.Detail with
        {
            Resource = fixture.Detail.Resource with
            {
                Type = type,
                Status = "active",
                Summary = [new(countLabel, "0")]
            }
        };
        var view = fixture.Render(fixture.PathQuery);
        view.WaitForAssertion(() => Assert.Single(view.FindAll("#resource-title")));

        var selector = view.Find(".resource-list");
        Assert.Equal("Root resource", selector.QuerySelector("strong")!.TextContent);
        Assert.Equal(type, selector.QuerySelector("small")!.TextContent);
        Assert.DoesNotContain("active", selector.TextContent, StringComparison.Ordinal);
        Assert.Empty(selector.QuerySelectorAll(".resource-facts"));
        Assert.Contains("active", view.Find(".resource-heading .tag").TextContent);
        Assert.Contains(countLabel, view.Find(".resource-heading .resource-facts").TextContent);
        Assert.Contains("0", view.Find(".resource-heading .resource-facts").TextContent);
    }

    [Fact]
    public void Single_root_kind_hides_selector_and_never_requests_a_child_only_kind()
    {
        using var fixture = new ExplorerFixture("s3",
            [new("objects", "Objects") { IsRoot = false }, new("buckets", "Buckets")]);
        var view = fixture.Render("?kind=objects");
        view.WaitForAssertion(() => Assert.Single(view.FindAll(".resource-button")));
        Assert.Empty(view.FindAll("#resource-kind"));
        Assert.Equal("buckets", fixture.ResourceQueries.Single()["kind"]);
    }

    [Fact]
    public void Multiple_roots_keep_selector_without_child_kinds()
    {
        using var fixture = new ExplorerFixture("sns",
            [new("topics", "Topics"), new("subscriptions", "Subscriptions") { IsRoot = false }, new("applications", "Platform applications")]);
        var view = fixture.Render();
        view.WaitForAssertion(() => Assert.Equal(2, view.FindAll("#resource-kind option").Count));
        Assert.Equal(["topics", "applications"], view.FindAll("#resource-kind option").Select(option => option.GetAttribute("value")));
    }

    [Fact]
    public void Empty_child_collection_uses_declared_label_not_loaded_rows()
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        fixture.Detail = fixture.Detail with { HasChildren = true, ChildKinds = [new("messages", "Messages") { IsRoot = false }] };
        var view = fixture.Render(fixture.PathQuery);
        view.WaitForAssertion(() => Assert.Equal("Messages", view.Find("#tab-contents").TextContent));
        Assert.Contains("No messages are retained", view.Markup);
        Assert.Empty(view.FindAll("#child-kind"));
    }

    [Fact]
    public void Child_group_labels_come_from_capabilities_even_when_catalog_omits_them()
    {
        using var fixture = new ExplorerFixture("custom", [new("roots", "Roots")]);
        fixture.Detail = fixture.Detail with
        {
            HasChildren = true,
            ChildKinds = [new("opaque-kind", "Retained configurations") { IsRoot = false }, new("another-kind", "Other entries") { IsRoot = false }]
        };
        fixture.Children = [new(new("opaque-kind", "child"), "A configuration")];
        var view = fixture.Render(fixture.PathQuery);
        view.WaitForAssertion(() => Assert.Single(view.FindAll(".record-button")));
        Assert.Contains("Retained configurations", view.Find(".record-meta").TextContent);
        Assert.DoesNotContain("Opaque Kind", view.Markup, StringComparison.Ordinal);
    }

    [Fact]
    public void Configuration_only_resource_opens_metadata_instead_of_empty_content_tab()
    {
        using var fixture = new ExplorerFixture("rds", [new("databases", "Databases")]);
        fixture.Detail = fixture.Detail with { Fields = [new("Engine", "postgres")] };
        var view = fixture.Render(fixture.PathQuery);
        view.WaitForAssertion(() => Assert.Contains("postgres", view.Find(".metadata").TextContent));
        Assert.Empty(view.FindAll("#tab-contents"));
        Assert.Equal("true", view.Find("#tab-configuration").GetAttribute("aria-selected"));
    }

    [Fact]
    public void Actual_events_catalog_id_uses_eventbridge_rule_payload_and_target_profile()
    {
        using var fixture = new ExplorerFixture("events", [new("event-buses", "Event buses"), new("rules", "Rules") { IsRoot = false }]);
        fixture.Detail = fixture.Detail with { HasChildren = true, ChildKinds = [new("rules", "Rules") { IsRoot = false }] };
        var rule = new AdminResourceSummary(new("rules", "orders"), "Orders rule");
        fixture.Children = [rule];
        fixture.SelectedDetail = new(rule)
        {
            HasContent = true,
            HasChildren = true,
            HasConnections = true,
            ConnectionCount = 2,
            ChildKinds = [new("targets", "Targets") { IsRoot = false }]
        };
        fixture.Connections =
        [
            new("Parent event bus", "belongs-to", "events", [fixture.Detail.Resource.Key]),
            new("Orders queue", "target", "sqs", [new("queues", "orders")])
        ];
        var view = fixture.Render(fixture.PathQuery + "&item=" + Uri.EscapeDataString(ExplorerLocation.EncodePath([rule.Key])));
        view.WaitForAssertion(() => Assert.Contains("Event pattern / schedule", view.Find(".entry-inspector .payload-head").TextContent));
        Assert.Contains("rule patterns or schedules", view.Find(".service-heading").TextContent);
        Assert.Contains(view.FindAll(".payload-head a"), link => link.TextContent == "Browse Targets →");
        Assert.Equal("Configured connections (2)", view.Find(".entry-connections h3").TextContent);
        Assert.Equal(2, view.FindAll(".entry-connections .connection").Count);
        Assert.Contains("Parent event bus", view.Find(".entry-connections").TextContent);
        Assert.Contains("Orders queue", view.Find(".entry-connections").TextContent);
        Assert.DoesNotContain("Targets (2)", view.Markup, StringComparison.Ordinal);
        Assert.DoesNotContain("Browse Rules", view.Markup, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Server_filter_resets_cursor_and_preserves_selected_detail_and_filter_component()
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        fixture.Detail = fixture.Detail with { HasChildren = true, ChildKinds = [new("messages", "Messages") { IsRoot = false }] };
        var view = fixture.Render(fixture.PathQuery + "&cursor=old-page");
        view.WaitForAssertion(() => Assert.Single(view.FindAll("#resource-title")));
        var filter = view.FindComponents<DebouncedFilter>()[0].Instance;
        var input = view.Find("input[aria-label='Filter queues']");
        var typing = input.InputAsync(new() { Value = "orders" });
        await input.KeyDownAsync(new KeyboardEventArgs { Key = "Enter" });
        await typing;
        view.WaitForAssertion(() => Assert.Equal("orders", fixture.ResourceQueries.Last().GetValueOrDefault("filter")));
        Assert.False(fixture.ResourceQueries.Last().ContainsKey("cursor"));
        Assert.Same(filter, view.FindComponents<DebouncedFilter>()[0].Instance);
        Assert.Contains("path=", fixture.Navigation.Uri, StringComparison.Ordinal);
        Assert.Contains("Root resource", view.Find("#resource-title").TextContent);
        Assert.DoesNotContain("Apply", view.Markup, StringComparison.Ordinal);
        Assert.Equal(1, fixture.DetailRequests);
    }

    [Fact]
    public async Task Child_filter_is_server_side_and_clears_selection_and_cursor()
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        fixture.Detail = fixture.Detail with { HasChildren = true, ChildKinds = [new("messages", "Messages") { IsRoot = false }] };
        var view = fixture.Render(fixture.PathQuery + "&childCursor=old&item=" + Uri.EscapeDataString(ExplorerLocation.EncodePath([new("messages", "m")])));
        view.WaitForAssertion(() => Assert.Single(view.FindAll("input[aria-label='Filter message IDs']")));
        var input = view.Find("input[aria-label='Filter message IDs']");
        var typing = input.InputAsync(new() { Value = "message-id" });
        await input.KeyDownAsync(new KeyboardEventArgs { Key = "Enter" });
        await typing;
        view.WaitForAssertion(() => Assert.Equal("message-id", fixture.ChildQueries.Last().GetValueOrDefault("filter")));
        Assert.DoesNotContain("item=", fixture.Navigation.Uri, StringComparison.Ordinal);
        Assert.DoesNotContain("childCursor=", fixture.Navigation.Uri, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Newer_server_filter_wins_even_when_older_response_ignores_cancellation()
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        var late = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationToken oldToken = default;
        fixture.ResourceResponse = (query, token) =>
        {
            if (query.GetValueOrDefault("filter") == "old") { oldToken = token; return late.Task; }
            return Task.FromResult(Json(new AdminPage<AdminResourceSummary>
            { Items = [new(new("queues", "new"), query.GetValueOrDefault("filter") ?? "initial")] }, AdminJsonContext.Default.AdminPageAdminResourceSummary));
        };
        var view = fixture.Render();
        view.WaitForAssertion(() => Assert.Single(view.FindAll(".resource-button")));
        fixture.Navigation.NavigateTo("/accounts/000000000000/services/sqs?filter=old");
        view.WaitForAssertion(() => Assert.True(oldToken.CanBeCanceled));
        fixture.Navigation.NavigateTo("/accounts/000000000000/services/sqs?filter=new");
        view.WaitForAssertion(() => Assert.Equal("new", view.Find(".resource-button strong").TextContent));
        Assert.True(oldToken.IsCancellationRequested);
        late.SetResult(Json(new AdminPage<AdminResourceSummary> { Items = [new(new("queues", "old"), "stale-result")] }, AdminJsonContext.Default.AdminPageAdminResourceSummary));
        await view.InvokeAsync(() => Task.CompletedTask);
        view.WaitForAssertion(() => Assert.DoesNotContain("stale-result", view.Markup, StringComparison.Ordinal));
    }

    [Fact]
    public void S3_prefix_rows_navigate_directly_and_preserve_opaque_full_keys()
    {
        using var fixture = new ExplorerFixture("s3", [new("buckets", "Buckets")]);
        fixture.Detail = fixture.Detail with { HasChildren = true, ChildKinds = [new("prefixes", "Prefixes") { IsRoot = false }, new("objects", "Objects") { IsRoot = false }] };
        var key = new AdminKey("prefixes", "folder/文?#/");
        fixture.Children = [new(key, key.Id)];
        var view = fixture.Render(fixture.PathQuery + "&prefix=folder%2F");
        view.WaitForAssertion(() => Assert.Single(view.FindAll(".record-button")));
        var link = view.Find(".record-button");
        Assert.Equal("文?#/", link.QuerySelector(".record-title")!.TextContent);
        var query = Query(new Uri(link.GetAttribute("href")!));
        Assert.False(query.ContainsKey("item"));
        Assert.Equal([fixture.Detail.Resource.Key, key], ExplorerLocation.DecodePath(query["path"]));
        Assert.Contains("Bucket root", view.Find(".prefix-breadcrumb").TextContent);
    }

    [Fact]
    public void Account_scope_does_not_repeat_but_global_scope_is_explicit()
    {
        using var fixture = new ExplorerFixture("firehose", [new("streams", "Delivery streams")], "instance-global");
        var view = fixture.Render(fixture.PathQuery);
        view.WaitForAssertion(() => Assert.Contains("Instance-global data", view.Find(".scope-exception").TextContent));
        Assert.DoesNotContain("Account-scoped data", view.Markup, StringComparison.Ordinal);
    }

    [Fact]
    public void Summary_rows_do_not_fetch_details_or_content_for_each_entry()
    {
        using var fixture = new ExplorerFixture("sns", [new("topics", "Topics")]);
        fixture.Detail = fixture.Detail with { HasChildren = true, ChildKinds = [new("subscriptions", "Subscriptions") { IsRoot = false }] };
        fixture.Children = Enumerable.Range(1, 4).Select(index =>
            new AdminResourceSummary(new("subscriptions", index.ToString()), $"Endpoint {index}")
            { Summary = [new("Protocol", "https"), new("Endpoint", $"https://example.test/{index}")] }).ToArray();
        var view = fixture.Render(fixture.PathQuery);
        view.WaitForAssertion(() => Assert.Equal(4, view.FindAll(".record-button").Count));
        Assert.Equal(1, fixture.DetailRequests);
        Assert.Equal(0, fixture.ContentRequests);
        Assert.Contains("https://example.test/4", view.Markup);
    }

    [Fact]
    public void Prefix_breadcrumbs_do_not_normalize_repeated_slashes_or_partial_prefixes()
    {
        using var fixture = new ExplorerFixture("s3", [new("buckets", "Buckets")]);
        fixture.Detail = fixture.Detail with { HasChildren = true };
        var view = fixture.Render(fixture.PathQuery + "&prefix=a%2F%2Fb");
        view.WaitForAssertion(() => Assert.Equal(4, view.FindAll(".prefix-breadcrumb a").Count));
        var prefixes = view.FindAll(".prefix-breadcrumb a").Skip(1).Select(link => Query(new Uri(link.GetAttribute("href")!))["prefix"]).ToArray();
        Assert.Equal(["a/", "a//", "a//b"], prefixes);
    }

    [Fact]
    public async Task Fast_resource_selection_keeps_existing_layout_until_replacement_is_ready()
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        var view = fixture.Render(fixture.PathQuery);
        view.WaitForAssertion(() => Assert.Single(view.FindAll("#resource-title")));
        var pending = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.DetailResponse = (_, _) => pending.Task;
        var replacement = new AdminResourceDetail(new(new("queues", "next"), "Next queue"));
        var renders = view.FindComponent<Resources>().RenderCount;
        await view.InvokeAsync(() => fixture.Navigation.NavigateTo("/accounts/000000000000/services/sqs?path=" + Uri.EscapeDataString(ExplorerLocation.EncodePath([replacement.Resource.Key]))));

        Assert.Equal(renders, view.FindComponent<Resources>().RenderCount);
        Assert.Equal("Root resource", view.Find("#resource-title").TextContent);
        Assert.Empty(view.FindAll(".skeleton"));
        Assert.Single(fixture.ResourceQueries);
        pending.SetResult(Json(replacement, AdminJsonContext.Default.AdminResourceDetail));
        await view.InvokeAsync(() => Task.CompletedTask);
        view.WaitForAssertion(() => Assert.Equal("Next queue", view.Find("#resource-title").TextContent));
        Assert.Empty(view.FindAll(".skeleton"));
    }

    [Fact]
    public async Task Slow_resource_selection_keeps_index_and_shows_delayed_loading_feedback()
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        var view = fixture.Render(fixture.PathQuery);
        view.WaitForAssertion(() => Assert.Single(view.FindAll("#resource-title")));
        var pending = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.DetailResponse = (_, _) => pending.Task;
        fixture.Navigation.NavigateTo("/accounts/000000000000/services/sqs?path=" + Uri.EscapeDataString(ExplorerLocation.EncodePath([new("queues", "next")])));

        view.WaitForAssertion(() => Assert.Contains("Loading snapshot", view.Find(".inspector").TextContent));
        Assert.Single(view.FindAll(".resource-list .resource-button"));
        Assert.Empty(view.FindAll("#resource-title"));
        Assert.Single(fixture.ResourceQueries);
        pending.SetResult(Json(new AdminResourceDetail(new(new("queues", "next"), "Next queue")), AdminJsonContext.Default.AdminResourceDetail));
        await view.InvokeAsync(() => Task.CompletedTask);
        view.WaitForAssertion(() => Assert.Equal("Next queue", view.Find("#resource-title").TextContent));
        Assert.Empty(view.FindAll(".skeleton"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Sensitive_or_cross_account_navigation_clears_old_inspection_immediately(bool changeAccount)
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        if (!changeAccount)
            fixture.Detail = fixture.Detail with { Fields = [new("Secret", "masked", Sensitive: true, CanReveal: true)] };
        var view = fixture.Render(fixture.PathQuery);
        view.WaitForAssertion(() => Assert.Single(view.FindAll("#resource-title")));
        var pending = new TaskCompletionSource<AdminResourceDetail>(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.DetailResponse = async (_, _) => Json(await pending.Task, AdminJsonContext.Default.AdminResourceDetail);
        await view.InvokeAsync(() => fixture.Navigation.NavigateTo("/accounts/" +
            (changeAccount ? "111111111111" : "000000000000") +
            "/services/sqs?path=" + Uri.EscapeDataString(ExplorerLocation.EncodePath([new("queues", "next")]))));

        Assert.Empty(view.FindAll("#resource-title"));
        Assert.Empty(view.FindAll(".metadata"));
        pending.SetResult(new AdminResourceDetail(new(new("queues", "next"), "Next queue")));
        await view.InvokeAsync(() => Task.CompletedTask);
        view.WaitForAssertion(() => Assert.Equal("Next queue", view.Find("#resource-title").TextContent));
    }

    [Fact]
    public async Task Account_navigation_cancels_inflight_server_response()
    {
        using var fixture = new ExplorerFixture("sqs", [new("queues", "Queues")]);
        var late = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationToken oldToken = default;
        fixture.ResourceResponse = (query, token) =>
        {
            if (query["accountId"] == "111111111111") { oldToken = token; return late.Task; }
            return Task.FromResult(Json(new AdminPage<AdminResourceSummary>
            { Items = [new(new("queues", "current"), "current-account")] }, AdminJsonContext.Default.AdminPageAdminResourceSummary));
        };
        var view = fixture.Render("?account=111111111111");
        view.WaitForAssertion(() => Assert.True(oldToken.CanBeCanceled));
        fixture.Navigation.NavigateTo("/accounts/222222222222/services/sqs");
        view.WaitForAssertion(() => Assert.Contains("current-account", view.Markup));
        Assert.True(oldToken.IsCancellationRequested);
        late.SetResult(Json(new AdminPage<AdminResourceSummary> { Items = [new(new("queues", "old"), "other-account")] }, AdminJsonContext.Default.AdminPageAdminResourceSummary));
        await view.InvokeAsync(() => Task.CompletedTask);
        Assert.DoesNotContain("other-account", view.Markup, StringComparison.Ordinal);
    }

    private static Dictionary<string, string> Query(Uri uri) => uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries)
        .Select(part => part.Split('=', 2)).ToDictionary(part => part[0], part => Uri.UnescapeDataString(part[1]));
    private static HttpResponseMessage Json<T>(T value, JsonTypeInfo<T> type) => new(HttpStatusCode.OK) { Content = JsonContent.Create(value, type) };

    private sealed class ExplorerFixture : IDisposable
    {
        private readonly BunitContext _context = new();
        private readonly AdminService _service;
        private readonly Handler _handler;
        public AdminResourceDetail Detail { get; set; }
        public AdminResourceDetail? SelectedDetail { get; set; }
        public IReadOnlyList<AdminResourceSummary> Children { get; set; } = [];
        public IReadOnlyList<AdminConnection> Connections { get; set; } = [];
        public string[] Accounts { get; set; } = ["000000000000", "111111111111", "222222222222"];
        public bool FailAccounts { get; set; }
        public List<Dictionary<string, string>> ResourceQueries { get; } = [];
        public List<Dictionary<string, string>> ChildQueries { get; } = [];
        public int DetailRequests { get; private set; }
        public int ContentRequests { get; private set; }
        public Func<Dictionary<string, string>, CancellationToken, Task<HttpResponseMessage>>? ResourceResponse { get; set; }
        public Func<Dictionary<string, string>, CancellationToken, Task<HttpResponseMessage>>? DetailResponse { get; set; }
        public NavigationManager Navigation => _context.Services.GetRequiredService<NavigationManager>();
        public BunitJSInterop JSInterop => _context.JSInterop;
        public string PathQuery => "?path=" + Uri.EscapeDataString(ExplorerLocation.EncodePath([Detail.Resource.Key]));
        public ExplorerFixture(string service, IReadOnlyList<AdminResourceKind> kinds, string scope = "account")
        {
            _context.JSInterop.Mode = JSRuntimeMode.Loose;
            _service = new(service, service, service, "Test", "s3", service, "enabled", scope) { Kinds = kinds };
            Detail = new(new(new(kinds.First(kind => kind.IsRoot).Id, "root"), "Root resource", Scope: scope));
            _handler = new Handler(SendAsync);
            var http = new HttpClient(_handler) { BaseAddress = new("http://localhost:4566") };
            _context.Services.AddSingleton(new AdminApiClient(http));
            _context.Services.AddSingleton(new MicroStackApiService(http));
            _context.Services.AddScoped<ExplorerAccountState>();
        }
        public IRenderedComponent<MicroStack.UI.Client.App> Render(string query = "")
        {
            var parameters = Query(new Uri("http://localhost/" + query));
            var account = parameters.GetValueOrDefault("account") ?? "000000000000";
            var canonicalQuery = string.Join("&", parameters.Where(pair => pair.Key != "account")
                .Select(pair => pair.Key + "=" + Uri.EscapeDataString(pair.Value)));
            Navigation.NavigateTo($"/accounts/{account}/services/{_service.Id}" +
                (canonicalQuery.Length == 0 ? "" : "?" + canonicalQuery));
            return _context.Render<MicroStack.UI.Client.App>();
        }
        public IRenderedComponent<MicroStack.UI.Client.App> RenderDirectory()
        {
            Navigation.NavigateTo("/accounts/000000000000/services");
            return _context.Render<MicroStack.UI.Client.App>();
        }
        public IRenderedComponent<MicroStack.UI.Client.App> RenderLegacy(string url)
        {
            Navigation.NavigateTo(url);
            return _context.Render<MicroStack.UI.Client.App>();
        }
        private Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
        {
            if (request.RequestUri!.AbsolutePath is "/_microstack/health" or "/_microstack/resources" or "/_microstack/requests")
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(request.RequestUri.AbsolutePath == "/_microstack/health"
                        ? """{"services":{},"edition":"test","version":"1"}""" : "[]")
                });
            var query = Query(request.RequestUri!);
            var endpoint = request.RequestUri!.AbsolutePath.Split('/').Last();
            if (endpoint == "resources")
            {
                ResourceQueries.Add(query);
                if (ResourceResponse is not null) return ResourceResponse(query, token);
            }
            if (endpoint == "children") ChildQueries.Add(query);
            if (endpoint == "resource")
            {
                DetailRequests++;
                if (DetailResponse is not null) return DetailResponse(query, token);
            }
            if (endpoint == "content") ContentRequests++;
            return Task.FromResult(endpoint switch
            {
                "context" => Json(new AdminContext("000000000000", "eu-west-1"), AdminJsonContext.Default.AdminContext),
                "accounts" when FailAccounts => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable),
                "accounts" => Json(Accounts, AdminJsonContext.Default.StringArray),
                "services" => Json(new[] { _service }, AdminJsonContext.Default.AdminServiceArray),
                "resources" => Json(new AdminPage<AdminResourceSummary> { Items = [Detail.Resource] }, AdminJsonContext.Default.AdminPageAdminResourceSummary),
                "resource" => Json(SelectedDetail is not null && ExplorerLocation.DecodePath(query["path"]).Length > 1 ? SelectedDetail : Detail, AdminJsonContext.Default.AdminResourceDetail),
                "children" => Json(new AdminPage<AdminResourceSummary> { Items = Children }, AdminJsonContext.Default.AdminPageAdminResourceSummary),
                "content" => Json(new AdminContent("json", "application/json", Text: "{}"), AdminJsonContext.Default.AdminContent),
                "connections" => Json(new AdminPage<AdminConnection> { Items = Connections }, AdminJsonContext.Default.AdminPageAdminConnection),
                _ => throw new InvalidOperationException(endpoint)
            });
        }
        public void Dispose() { _context.Dispose(); _handler.Dispose(); }
    }
    private sealed class Handler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) => send(request, cancellationToken);
    }
}
