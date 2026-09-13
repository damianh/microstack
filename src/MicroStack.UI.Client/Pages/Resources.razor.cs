using System.Net;
using System.Text.Json;
using MicroStack.Admin.Contracts;
using MicroStack.UI.Client.Services;
using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;

namespace MicroStack.UI.Client.Pages;

public partial class Resources
{
    [Parameter] public string? ServiceId { get; set; }
    [SupplyParameterFromQuery(Name = "account")] public string? Account { get; set; }
    [SupplyParameterFromQuery(Name = "kind")] public string? Kind { get; set; }
    [SupplyParameterFromQuery(Name = "path")] public string? PathQuery { get; set; }
    [SupplyParameterFromQuery(Name = "item")] public string? ItemQuery { get; set; }
    [SupplyParameterFromQuery(Name = "tab")] public string? Tab { get; set; }
    [SupplyParameterFromQuery(Name = "filter")] public string? Filter { get; set; }
    [SupplyParameterFromQuery(Name = "enabled")] public bool? Enabled { get; set; }
    [SupplyParameterFromQuery(Name = "cursor")] public string? Cursor { get; set; }
    [SupplyParameterFromQuery(Name = "childKind")] public string? ChildKind { get; set; }
    [SupplyParameterFromQuery(Name = "childFilter")] public string? ChildFilter { get; set; }
    [SupplyParameterFromQuery(Name = "childCursor")] public string? ChildCursor { get; set; }
    [SupplyParameterFromQuery(Name = "connectionCursor")] public string? ConnectionCursor { get; set; }
    [SupplyParameterFromQuery(Name = "prefix")] public string? Prefix { get; set; }
    [SupplyParameterFromQuery(Name = "returnTo")] public string? ReturnTo { get; set; }
    private CancellationTokenSource _cts = new();
    private AdminContext? _context;
    private IReadOnlyList<AdminService> _services = [];
    private AdminService? _service;
    private AdminPage<AdminResourceSummary>? _resources, _children;
    private AdminPage<AdminConnection>? _connections;
    private AdminResourceDetail? _detail, _selectedDetail;
    private AdminContent? _content, _selectedContent;
    private AdminKey[] _path = [], _item = [];
    private string _account = "", _kind = "", _resourceFilter = "", _childFilter = "", _prefixFilter = "", _scopeKey = "";
    private string? _error, _childError;
    private DateTimeOffset? _captured;
    private bool _busy, _missing, _browsing;
    private long _generation, _restoredGeneration;
    private string? _focusTab;
    private string ActiveTab => Tab is "configuration" or "connections" or "activity" ? Tab : "contents";
    private IReadOnlyList<(string Id, string Label)> Tabs =>
        [("contents", ChildrenLabel), ("configuration", "Configuration"), ("connections", "Connections"), ("activity", "Activity")];
    private List<AdminService> DirectoryServices => _services.Where(service =>
        (Enabled != true || service.Availability != "disabled") &&
        $"{service.Name} {service.Label} {service.Id} {service.Category}".Contains(Filter ?? "", StringComparison.OrdinalIgnoreCase)).ToList();
    private string RootKindLabel => _service?.Kinds.FirstOrDefault(kind => kind.Id == _kind)?.Label ?? "Resources";
    private IReadOnlyList<AdminResourceKind> ChildKinds => _children?.Items.Select(child => child.Key.Kind).Distinct(StringComparer.Ordinal)
        .Select(kind => new AdminResourceKind(kind, KindLabel(kind))).ToArray() ?? [];
    private string ChildrenLabel => _detail?.HasChildren == true && ChildKinds.Count == 1 ? ChildKinds[0].Label : "Contents";
    private AdminKey[] SelectedPath => [.. _path, .. _item];
    private string HomeUrl => Navigation.GetUriWithQueryParameters(Navigation.BaseUri, new Dictionary<string, object?> { ["account"] = _account });
    private string OpenChildUrl => UpdateUrl(new() { ["path"] = ExplorerLocation.EncodePath(SelectedPath), ["item"] = null, ["childKind"] = null, ["childFilter"] = null, ["childCursor"] = null, ["prefix"] = null, ["tab"] = "contents" });
    private string SelectedConnectionsUrl => Navigation.GetUriWithQueryParameters(OpenChildUrl, new Dictionary<string, object?> { ["tab"] = "connections" });

    protected override Task OnParametersSetAsync() => LoadAsync();

    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (_busy || _restoredGeneration == _generation) return;
        _restoredGeneration = _generation;
        await JS.InvokeVoidAsync("microstack.restore", Navigation.Uri);
        if (_focusTab is not null && _detail is not null)
        {
            await JS.InvokeVoidAsync("microstack.focus", $"tab-{_focusTab}");
            _focusTab = null;
        }
    }

    private async Task LoadAsync()
    {
        _cts.Cancel(); _cts.Dispose(); _cts = new();
        var token = _cts.Token;
        var generation = ++_generation;
        _scopeKey = $"{generation}:{Navigation.Uri}";
        _busy = true; _error = null; _childError = null; _missing = false; _browsing = false;
        _detail = null; _selectedDetail = null; _content = null; _selectedContent = null;
        _resources = null; _children = null; _connections = null; _captured = null;
        _resourceFilter = Filter ?? ""; _childFilter = ChildFilter ?? ""; _prefixFilter = Prefix ?? "";
        try
        {
            var context = _context ?? await Api.ContextAsync(token);
            var account = Account ?? context.DefaultAccount;
            if (token.IsCancellationRequested) return;
            _context = context; _account = account;
            if (!ExplorerLocation.ValidAccount(account)) throw new FormatException("Account ID must contain exactly 12 digits. Choose a valid account.");
            var path = ExplorerLocation.DecodePath(PathQuery);
            var item = ExplorerLocation.DecodePath(ItemQuery);
            if (item.Length > 1) throw new FormatException("Select one child entry at a time.");
            var services = await Api.ServicesAsync(account, token);
            if (token.IsCancellationRequested) return;
            _context = context; _account = account; _path = path; _item = item; _services = services;
            _service = services.FirstOrDefault(service => service.Id == ServiceId);
            if (ServiceId is null) return;
            if (_service is null) throw new FormatException("This service is not in the instance catalog. Choose a service from the breadcrumb.");
            if (_service.Availability == "disabled") return;
            _kind = Kind ?? path.FirstOrDefault()?.Kind ?? _service.Kinds.FirstOrDefault()?.Id ?? "";
            if (_service.Kinds.Count > 0)
            {
                var resources = await Api.ResourcesAsync(ServiceId, account, _kind, Filter, Cursor, token);
                if (token.IsCancellationRequested) return;
                _resources = resources; _captured = resources.CapturedAt;
            }
            if (path.Length == 0)
            {
                _browsing = true;
                return;
            }
            var detail = await Api.DetailAsync(ServiceId, account, path, token);
            if (token.IsCancellationRequested) return;
            _detail = detail;
            if (ActiveTab == "connections" && detail.HasConnections)
            {
                var connections = await Api.ConnectionsAsync(ServiceId, account, path, ConnectionCursor, token);
                if (!token.IsCancellationRequested) { _connections = connections; _captured = connections.CapturedAt; }
            }
            if (ActiveTab != "contents") return;
            if (detail.HasChildren)
            {
                var children = await Api.ChildrenAsync(ServiceId, account, path, ChildKind, ChildFilter, Prefix, ChildCursor, token);
                if (token.IsCancellationRequested) return;
                _children = children; _captured = children.CapturedAt;
            }
            if (detail.HasContent)
            {
                var content = await Api.ContentAsync(ServiceId, account, path, token);
                if (token.IsCancellationRequested) return;
                _content = content;
            }
            if (item.Length > 0) await LoadSelectedAsync(token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        catch (AdminApiException exception)
        {
            if (token.IsCancellationRequested) return;
            _missing = exception.Status == HttpStatusCode.NotFound;
            _error = _missing ? "This resource no longer exists in the selected account. Return to the resource list or refresh." : exception.Message;
        }
        catch (Exception exception) when (exception is JsonException or FormatException)
        { if (!token.IsCancellationRequested) _error = exception is FormatException ? exception.Message : "The API returned an invalid inspection response. Refresh to retry."; }
        catch (Exception) { if (!token.IsCancellationRequested) _error = "The inspection API is unavailable. Check the API connection, then retry this snapshot."; }
        finally { if (generation == _generation) _busy = false; }
    }

    private async Task LoadSelectedAsync(CancellationToken token)
    {
        try
        {
            var selected = await Api.DetailAsync(ServiceId!, _account, SelectedPath, token);
            if (token.IsCancellationRequested) return;
            _selectedDetail = selected;
            if (!selected.HasContent) return;
            var content = await Api.ContentAsync(ServiceId!, _account, SelectedPath, token);
            if (!token.IsCancellationRequested) _selectedContent = content;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        catch (AdminApiException exception) when (exception.Status == HttpStatusCode.NotFound)
        { if (!token.IsCancellationRequested) _childError = "This entry no longer exists. Refresh the collection and select another entry."; }
        catch (Exception) { if (!token.IsCancellationRequested) _childError = "This entry could not be loaded. Refresh the snapshot to retry."; }
    }

    private string UpdateUrl(Dictionary<string, object?> changes) => Navigation.GetUriWithQueryParameters(changes);
    private void Update(Dictionary<string, object?> changes, bool replace = false) => Navigation.NavigateTo(UpdateUrl(changes), replace: replace);
    private string ServiceUrl(string service) => ExplorerLocation.ServiceUrl(Navigation, service, _account);
    private void SwitchService(string service) { if (service != ServiceId) Navigation.NavigateTo(ServiceUrl(service)); }
    private void SwitchAccount(string account)
    {
        _cts.Cancel();
        _detail = null; _selectedDetail = null; _content = null; _selectedContent = null; _resources = null; _children = null; _connections = null;
        Navigation.NavigateTo(ServiceId is null ? Navigation.GetUriWithQueryParameters(Navigation.BaseUri, new Dictionary<string, object?> { ["account"] = account })
            : ExplorerLocation.ServiceUrl(Navigation, ServiceId, account));
    }
    private void FilterServices(ChangeEventArgs args) => Update(new() { ["filter"] = args.Value?.ToString() }, replace: true);
    private void FilterEnabled(ChangeEventArgs args) => Update(new() { ["enabled"] = args.Value is true ? true : null }, replace: true);
    private void ChangeKind(ChangeEventArgs args) => Update(new() { ["kind"] = args.Value?.ToString(), ["path"] = null, ["item"] = null, ["cursor"] = null, ["filter"] = null, ["childKind"] = null, ["childFilter"] = null, ["childCursor"] = null, ["prefix"] = null });
    private void ApplyResourceFilter() => Update(new() { ["filter"] = _resourceFilter, ["cursor"] = null });
    private void ApplyChildFilter() => Update(new() { ["childFilter"] = _childFilter, ["prefix"] = _prefixFilter, ["childCursor"] = null, ["item"] = null });
    private void ChangeChildKind(ChangeEventArgs args) => Update(new() { ["childKind"] = args.Value?.ToString(), ["childCursor"] = null, ["item"] = null });
    private void ChangeResourcePage(string? cursor) => Update(new() { ["cursor"] = cursor });
    private void ChangeChildPage(string? cursor) => Update(new() { ["childCursor"] = cursor, ["item"] = null });
    private void ChangeConnectionPage(string? cursor) => Update(new() { ["connectionCursor"] = cursor });
    private void ChangeTab(string tab) { _focusTab = tab; Update(new() { ["tab"] = tab }); }
    private void ClearSelection() => Update(new() { ["path"] = null, ["item"] = null, ["cursor"] = null, ["childCursor"] = null });
    private string RootUrl(AdminKey key) => UpdateUrl(new() { ["path"] = ExplorerLocation.EncodePath([key]), ["item"] = null, ["tab"] = "contents", ["childKind"] = null, ["childFilter"] = null, ["childCursor"] = null, ["connectionCursor"] = null, ["prefix"] = null });
    private string ChildUrl(AdminKey key) => UpdateUrl(new() { ["item"] = ExplorerLocation.EncodePath([key]) });
    private string AncestorUrl(int index) => UpdateUrl(new() { ["path"] = ExplorerLocation.EncodePath(_path[..(index + 1)]), ["item"] = null, ["tab"] = "contents", ["childKind"] = null, ["childFilter"] = null, ["childCursor"] = null, ["prefix"] = null });
    private bool IsRootSelected(AdminKey key) => _path.Length > 0 && _path[0] == key;
    private bool IsChildSelected(AdminKey key) => _item.Length == 1 && _item[0] == key;
    private static string ScopeLabel(string scope) => scope switch
    {
        "global" or "instance" or "instance-global" => "Instance-global data",
        "account" => "Account-scoped data",
        _ => scope
    };
    private static string AvailabilityLabel(AdminService service) => service.Availability == "disabled" ? "Disabled" : service.Kinds.Count == 0 ? "Enabled · no retained resources" : "Enabled";
    private string KindLabel(string kind) => _service?.Kinds.FirstOrDefault(candidate => candidate.Id == kind)?.Label ??
        System.Globalization.CultureInfo.InvariantCulture.TextInfo.ToTitleCase(kind.Replace('-', ' ').Replace('_', ' '));
    private static bool CanFollow(AdminConnection connection) => connection.TargetServiceId is not null && connection.TargetPath is { Length: > 0 } &&
        connection.State is not ("missing" or "disabled" or "external" or "inaccessible");
    private string ConnectionUrl(AdminConnection connection)
    {
        var current = Navigation.GetUriWithQueryParameters(new Dictionary<string, object?> { ["returnTo"] = null });
        var local = "/" + Navigation.ToBaseRelativePath(current);
        return ExplorerLocation.ServiceUrl(Navigation, connection.TargetServiceId!, _account, connection.TargetPath, local);
    }
    private async Task ToggleBrowse()
    {
        _browsing = !_browsing;
        await JS.InvokeVoidAsync("microstack.focus", _browsing ? "resource-index" : "resource-title");
    }
    public void Dispose() { _cts.Cancel(); _cts.Dispose(); }
}
