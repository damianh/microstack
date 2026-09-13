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
    [Parameter] public string? AccountId { get; set; }
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
    private IReadOnlyList<string> _accounts = [];
    private IReadOnlyList<AdminService> _services = [];
    private AdminService? _service;
    private AdminPage<AdminResourceSummary>? _resources, _children;
    private AdminPage<AdminConnection>? _connections, _selectedConnections;
    private AdminResourceDetail? _detail, _selectedDetail;
    private AdminContent? _content, _selectedContent;
    private AdminKey[] _path = [], _item = [];
    private string _account = "", _kind = "", _prefixFilter = "", _scopeKey = "";
    private string? _error, _childError, _selectedConnectionError, _loadedScope, _loadedItem;
    private DateTimeOffset? _captured;
    private bool _busy, _missing, _browsing, _showLoading, _deferSelectionRender;
    private long _generation, _restoredGeneration;
    private RequestState? _lastRequest;
    private string? _focusTab;
    private string ActiveTab => Tab is "configuration" or "connections" or "activity" ? Tab :
        _detail is { HasChildren: false, HasContent: false } ? "configuration" : "contents";
    private IReadOnlyList<(string Id, string Label)> Tabs
    {
        get
        {
            List<(string, string)> tabs = [];
            if (_detail is { HasChildren: true } or { HasContent: true }) tabs.Add(("contents", ChildrenLabel));
            tabs.Add(("configuration", "Configuration"));
            if (_detail?.HasConnections == true)
                tabs.Add(("connections", _detail.ConnectionCount is { } count ? $"Connections ({count:N0})" : "Connections"));
            else if (Tab == "connections") tabs.Add(("connections", "Connections"));
            tabs.Add(("activity", "Activity"));
            return tabs;
        }
    }
    private List<AdminService> DirectoryServices => _services.Where(service =>
        (Enabled != true || service.Availability != "disabled") &&
        $"{service.Name} {service.Label} {service.Id} {service.Category}".Contains(Filter ?? "", StringComparison.OrdinalIgnoreCase)).ToList();
    private string RootKindLabel => _service?.Kinds.FirstOrDefault(kind => kind.Id == _kind)?.Label ?? "Resources";
    private string RootFilterLabel => $"Filter {RootKindLabel.ToLowerInvariant()}";
    private string ChildFilterLabel => InspectionProfile.ChildFilterLabel(ServiceId, _detail, ChildKind);
    private IReadOnlyList<AdminResourceKind> RootKinds => InspectionProfile.RootKinds(_service);
    private IReadOnlyList<AdminResourceKind> ChildKinds => _detail?.ChildKinds ?? [];
    private string ChildrenLabel => InspectionProfile.CollectionLabel(ServiceId, _detail);
    private bool IsSelectedEventRule => ServiceId == "events" && _selectedDetail?.Resource.Key.Kind == "rules";
    private string SelectedBrowseLabel => IsSelectedEventRule
        ? InspectionProfile.CollectionLabel(ServiceId, _selectedDetail) : KindLabel(_selectedDetail!.Resource.Key.Kind);
    private string FilterScope => $"{_generation}:{ServiceId}:{AccountId}";
    private bool IsS3PrefixBrowser => ServiceId == "s3" && _detail?.Resource.Key.Kind is "buckets" or "prefixes";
    private string CurrentPrefix => Prefix ?? (_path.LastOrDefault()?.Kind == "prefixes" ? _path[^1].Id : "");
    private IReadOnlyList<(string Name, string Prefix)> PrefixAncestors
    {
        get
        {
            List<(string, string)> ancestors = [];
            var prefix = CurrentPrefix;
            var start = 0;
            for (var index = 0; index < prefix.Length; index++)
            {
                if (prefix[index] != '/') continue;
                ancestors.Add((index == start ? "/" : prefix[start..index], prefix[..(index + 1)]));
                start = index + 1;
            }
            if (start < prefix.Length) ancestors.Add((prefix[start..], prefix));
            return ancestors;
        }
    }
    private AdminKey[] SelectedPath => [.. _path, .. _item];
    private string HomeUrl => ExplorerLocation.DirectoryUrl(Navigation, _account);
    private string OpenChildUrl => UpdateUrl(new() { ["path"] = ExplorerLocation.EncodePath(SelectedPath), ["item"] = null, ["childKind"] = null, ["childFilter"] = null, ["childCursor"] = null, ["prefix"] = null, ["tab"] = "contents" });
    private string SelectedConnectionsUrl => Navigation.GetUriWithQueryParameters(OpenChildUrl, new Dictionary<string, object?> { ["tab"] = "connections" });

    protected override Task OnParametersSetAsync() => LoadAsync();
    protected override bool ShouldRender() => !_deferSelectionRender;

    private bool HasSensitiveInspection =>
        new[] { _detail, _selectedDetail }.Any(detail => detail is not null &&
            (detail.RevealableFields.Count > 0 || detail.Fields.Any(value => value.Sensitive || value.CanReveal))) ||
        _content?.Sensitive == true || _selectedContent?.Sensitive == true;

    private async Task ShowLoadingAsync(long generation, CancellationToken token)
    {
        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(150), token);
            await InvokeAsync(() =>
            {
                if (generation != _generation) return;
                _deferSelectionRender = false;
                _showLoading = true;
                StateHasChanged();
            });
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
    }

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
        var request = new RequestState(ServiceId, AccountId, Kind, PathQuery, ItemQuery, Tab, Filter, Cursor, ChildKind, ChildFilter, Prefix, ChildCursor, ConnectionCursor);
        var resourceFilterOnly = _lastRequest is { } previous && request != previous &&
            request with { Filter = previous.Filter, Cursor = previous.Cursor } == previous;
        var childFilterOnly = _lastRequest is { } previousChild && request != previousChild && string.IsNullOrEmpty(ItemQuery) &&
            request with { ChildFilter = previousChild.ChildFilter, ChildCursor = previousChild.ChildCursor, Item = previousChild.Item } == previousChild;
        var scope = $"{ServiceId}:{AccountId}:{Kind}:{PathQuery}";
        var sameScope = _loadedScope == scope;
        var sameIndexScope = _lastRequest is { } indexRequest &&
            indexRequest.Service == request.Service && indexRequest.Account == request.Account &&
            indexRequest.Kind == request.Kind && indexRequest.Filter == request.Filter && indexRequest.Cursor == request.Cursor;
        // Keep the previous frame for fast selections, but never retain sensitive content across navigation.
        _deferSelectionRender = !sameScope && sameIndexScope && !HasSensitiveInspection;
        _showLoading = false;
        using var loadingDelay = CancellationTokenSource.CreateLinkedTokenSource(token);
        var loadingFeedback = ShowLoadingAsync(generation, loadingDelay.Token);
        _loadedScope = scope;
        _scopeKey = $"{scope}:{ItemQuery}";
        _busy = true; _error = null; _childError = null; _selectedConnectionError = null; _missing = false;
        if (!sameScope)
        {
            _detail = null; _content = null; _children = null; _connections = null; _captured = null;
            if (!sameIndexScope) _resources = null;
            _browsing = false;
        }
        if (!sameScope || _loadedItem != ItemQuery)
        {
            _selectedDetail = null; _selectedContent = null; _selectedConnections = null;
        }
        _loadedItem = ItemQuery;
        _prefixFilter = Prefix ?? "";
        try
        {
            if (sameScope && _service is not null && ServiceId is not null && (resourceFilterOnly || childFilterOnly))
            {
                if (resourceFilterOnly)
                {
                    var resources = await Api.ResourcesAsync(ServiceId, _account, _kind, Filter, Cursor, token);
                    if (token.IsCancellationRequested) return;
                    _resources = resources; _captured = resources.CapturedAt;
                }
                else if (_detail?.HasChildren == true)
                {
                    _item = [];
                    var children = await Api.ChildrenAsync(ServiceId, _account, _path, ChildKind, ChildFilter, Prefix, ChildCursor, token);
                    if (token.IsCancellationRequested) return;
                    _children = children; _captured = children.CapturedAt;
                }
                return;
            }
            var context = _context ?? await Api.ContextAsync(token);
            var account = AccountId ?? context.DefaultAccount;
            if (token.IsCancellationRequested) return;
            _context = context; _account = account;
            if (!ExplorerLocation.ValidAccount(account)) throw new FormatException("Account ID must contain exactly 12 digits. Choose a valid account.");
            if (!sameIndexScope || request == _lastRequest || _accounts.Count == 0)
            {
                var accounts = await Api.AccountsAsync(token);
                if (token.IsCancellationRequested) return;
                _accounts = accounts;
            }
            if (!_accounts.Contains(account))
            {
                _detail = null; _selectedDetail = null; _content = null; _selectedContent = null;
                _resources = null; _children = null; _connections = null; _selectedConnections = null;
                throw new FormatException($"Account {account} has no retained resources and is not the default. Select a known account.");
            }
            await AccountState.RememberAsync(account);
            if (token.IsCancellationRequested) return;
            var path = ExplorerLocation.DecodePath(PathQuery);
            var item = ExplorerLocation.DecodePath(ItemQuery);
            if (item.Length > 1) throw new FormatException("Select one child entry at a time.");
            var reuseIndex = sameIndexScope && request != _lastRequest && _resources is not null &&
                (Kind ?? path.FirstOrDefault()?.Kind ?? _kind) == _kind;
            var services = reuseIndex ? _services : await Api.ServicesAsync(account, token);
            if (token.IsCancellationRequested) return;
            _context = context; _account = account; _path = path; _item = item; _services = services;
            _service = services.FirstOrDefault(service => service.Id == ServiceId);
            if (ServiceId is null) return;
            if (_service is null) throw new FormatException("This service is not in the instance catalog. Choose a service from the breadcrumb.");
            if (_service.Availability == "disabled") return;
            var requestedKind = Kind ?? path.FirstOrDefault()?.Kind;
            _kind = RootKinds.FirstOrDefault(kind => kind.Id == requestedKind)?.Id ?? RootKinds.FirstOrDefault()?.Id ?? "";
            if (RootKinds.Count > 0 && !reuseIndex)
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
            if (!detail.HasConnections) _connections = null;
            _browsing = false;
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
            if (_missing) { _detail = null; _selectedDetail = null; _content = null; _selectedContent = null; _selectedConnections = null; }
            _error = _missing ? "This resource no longer exists in the selected account. Return to the resource list or refresh." : exception.Message;
        }
        catch (Exception exception) when (exception is JsonException or FormatException)
        { if (!token.IsCancellationRequested) _error = exception is FormatException ? exception.Message : "The API returned an invalid inspection response. Refresh to retry."; }
        catch (Exception) { if (!token.IsCancellationRequested) _error = "The inspection API is unavailable. Check the API connection, then retry this snapshot."; }
        finally
        {
            loadingDelay.Cancel();
            await loadingFeedback;
            if (generation == _generation)
            {
                _busy = false;
                _showLoading = false;
                _deferSelectionRender = false;
                if (!token.IsCancellationRequested && _error is null) _lastRequest = request;
            }
        }
    }

    private async Task LoadSelectedAsync(CancellationToken token)
    {
        try
        {
            var selected = await Api.DetailAsync(ServiceId!, _account, SelectedPath, token);
            if (token.IsCancellationRequested) return;
            _selectedDetail = selected;
            if (selected.HasContent)
            {
                var content = await Api.ContentAsync(ServiceId!, _account, SelectedPath, token);
                if (token.IsCancellationRequested) return;
                _selectedContent = content;
            }
            if (selected.HasConnections)
            {
                try
                {
                    var connections = await Api.ConnectionsAsync(ServiceId!, _account, SelectedPath, null, token);
                    if (!token.IsCancellationRequested) _selectedConnections = connections;
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { }
                catch (Exception) { if (!token.IsCancellationRequested) _selectedConnectionError = "Connections could not be loaded. Refresh the snapshot to retry."; }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        catch (AdminApiException exception) when (exception.Status == HttpStatusCode.NotFound)
        {
            if (!token.IsCancellationRequested)
            {
                _selectedDetail = null; _selectedContent = null; _selectedConnections = null;
                _childError = "This entry no longer exists. Refresh the collection and select another entry.";
            }
        }
        catch (Exception) { if (!token.IsCancellationRequested) _childError = "This entry could not be loaded. Refresh the snapshot to retry."; }
    }

    private string UpdateUrl(Dictionary<string, object?> changes) => Navigation.GetUriWithQueryParameters(changes);
    private void Update(Dictionary<string, object?> changes, bool replace = false) => Navigation.NavigateTo(UpdateUrl(changes), replace: replace);
    private string ServiceUrl(string service) => ExplorerLocation.ServiceUrl(Navigation, service, _account);
    private void SwitchService(string service) { if (service != ServiceId) Navigation.NavigateTo(ServiceUrl(service)); }
    private void SwitchAccount(string account)
    {
        _cts.Cancel();
        _detail = null; _selectedDetail = null; _content = null; _selectedContent = null; _resources = null; _children = null; _connections = null; _selectedConnections = null;
        Navigation.NavigateTo(ServiceId is null ? ExplorerLocation.DirectoryUrl(Navigation, account)
            : ExplorerLocation.ServiceUrl(Navigation, ServiceId, account));
    }
    private void FilterServices(ChangeEventArgs args) => Update(new() { ["filter"] = args.Value?.ToString() }, replace: true);
    private void FilterEnabled(ChangeEventArgs args) => Update(new() { ["enabled"] = args.Value is true ? true : null }, replace: true);
    private void ChangeKind(ChangeEventArgs args) => Update(new() { ["kind"] = args.Value?.ToString(), ["path"] = null, ["item"] = null, ["cursor"] = null, ["filter"] = null, ["childKind"] = null, ["childFilter"] = null, ["childCursor"] = null, ["prefix"] = null });
    private void ApplyResourceFilter(string value) => Update(new() { ["filter"] = value, ["cursor"] = null }, replace: true);
    private void ApplyChildFilter(string value) => Update(new() { ["childFilter"] = value, ["childCursor"] = null, ["item"] = null }, replace: true);
    private void ApplyPrefix() => Update(new() { ["prefix"] = _prefixFilter, ["childCursor"] = null, ["item"] = null });
    private void ChangeChildKind(ChangeEventArgs args) => Update(new() { ["childKind"] = args.Value?.ToString(), ["childCursor"] = null, ["item"] = null });
    private void ChangeResourcePage(string? cursor) => Update(new() { ["cursor"] = cursor });
    private void ChangeChildPage(string? cursor) => Update(new() { ["childCursor"] = cursor, ["item"] = null });
    private void ChangeConnectionPage(string? cursor) => Update(new() { ["connectionCursor"] = cursor });
    private void ChangeTab(string tab) { _focusTab = tab; Update(new() { ["tab"] = tab }); }
    private void ClearSelection() => Update(new() { ["path"] = null, ["item"] = null, ["cursor"] = null, ["childCursor"] = null });
    private string RootUrl(AdminKey key) => UpdateUrl(new() { ["path"] = ExplorerLocation.EncodePath([key]), ["item"] = null, ["tab"] = "contents", ["childKind"] = null, ["childFilter"] = null, ["childCursor"] = null, ["connectionCursor"] = null, ["prefix"] = null });
    private string ChildUrl(AdminKey key) => ServiceId == "s3" && key.Kind == "prefixes"
        ? UpdateUrl(new() { ["path"] = ExplorerLocation.EncodePath([_path[0], key]), ["item"] = null, ["prefix"] = null, ["childFilter"] = null, ["childCursor"] = null, ["childKind"] = null })
        : UpdateUrl(new() { ["item"] = ExplorerLocation.EncodePath([key]) });
    private string PrefixUrl(string prefix) => UpdateUrl(new() { ["path"] = ExplorerLocation.EncodePath([_path[0]]), ["prefix"] = prefix, ["item"] = null, ["childFilter"] = null, ["childCursor"] = null, ["childKind"] = null, ["tab"] = "contents" });
    private string ChildName(AdminResourceSummary child) => IsS3PrefixBrowser && CurrentPrefix.Length > 0 && child.Name.StartsWith(CurrentPrefix, StringComparison.Ordinal)
        ? child.Name[CurrentPrefix.Length..] : child.Name;
    private string AncestorUrl(int index) => UpdateUrl(new() { ["path"] = ExplorerLocation.EncodePath(_path[..(index + 1)]), ["item"] = null, ["tab"] = "contents", ["childKind"] = null, ["childFilter"] = null, ["childCursor"] = null, ["prefix"] = null });
    private bool IsRootSelected(AdminKey key) => _path.Length > 0 && _path[0] == key;
    private bool IsChildSelected(AdminKey key) => _item.Length == 1 && _item[0] == key;
    private static bool IsGlobalScope(string scope) => scope is "global" or "instance" or "instance-global";
    private static string ScopeLabel(string scope) => scope switch
    {
        "global" or "instance" or "instance-global" => "Instance-global data",
        "account" => "Account-scoped data",
        _ => scope
    };
    private static string AvailabilityLabel(AdminService service) => service.Availability == "disabled" ? "Disabled" : service.Kinds.Count == 0 ? "Enabled · no retained resources" : "Enabled";
    private string KindLabel(string kind) => ChildKinds.FirstOrDefault(candidate => candidate.Id == kind)?.Label ??
        _service?.Kinds.FirstOrDefault(candidate => candidate.Id == kind)?.Label ??
        System.Globalization.CultureInfo.InvariantCulture.TextInfo.ToTitleCase(kind.Replace('-', ' ').Replace('_', ' '));
    private string ConnectionUrl(AdminConnection connection)
    {
        var local = "/" + Navigation.ToBaseRelativePath(Navigation.Uri);
        return ExplorerLocation.ServiceUrl(Navigation, connection.TargetServiceId!, _account, connection.TargetPath, local);
    }
    private string? ConnectionSourceUrl(AdminConnection connection)
    {
        if (connection.SourceServiceId is null || connection.SourcePath is not { Length: > 0 } sourcePath) return null;
        if (connection.SourceServiceId == ServiceId && (sourcePath.SequenceEqual(_path) || sourcePath.SequenceEqual(SelectedPath))) return null;
        if (connection.SourceServiceId == connection.TargetServiceId && connection.TargetPath is { } targetPath && sourcePath.SequenceEqual(targetPath)) return null;
        return ExplorerLocation.ServiceUrl(Navigation, connection.SourceServiceId, _account, sourcePath, "/" + Navigation.ToBaseRelativePath(Navigation.Uri));
    }
    private async Task ToggleBrowse()
    {
        _browsing = !_browsing;
        await JS.InvokeVoidAsync("microstack.focus", _browsing ? "resource-index" : "resource-title");
    }
    public void Dispose() { _cts.Cancel(); _cts.Dispose(); }

    private sealed record RequestState(string? Service, string? Account, string? Kind, string? Path, string? Item, string? Tab,
        string? Filter, string? Cursor, string? ChildKind, string? ChildFilter, string? Prefix, string? ChildCursor, string? ConnectionCursor);
}
