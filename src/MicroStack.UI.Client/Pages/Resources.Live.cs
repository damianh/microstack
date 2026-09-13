using System.Net;
using MicroStack.Admin.Contracts;
using MicroStack.UI.Client.Services;

namespace MicroStack.UI.Client.Pages;

public partial class Resources
{
    private long _revealRevision;
    private long _subscriptionVersion;
    private IAsyncDisposable? _liveRegistration;
    private bool _disposed;
    private string? _catalogEpoch;

    internal Task RefreshLiveAsync(CancellationToken cancellationToken) =>
        InvokeAsync(() => ReadLiveAsync(cancellationToken));

    private async Task ReadLiveAsync(CancellationToken cancellationToken)
    {
        using var read = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
        var token = read.Token;
        var generation = _generation;
        var account = AccountId ?? _account;
        var service = ServiceId;
        var path = ExplorerLocation.DecodePath(PathQuery);
        var item = ExplorerLocation.DecodePath(ItemQuery);
        if (item.Length > 1) throw new FormatException("Select one child entry at a time.");
        var tab = ActiveTab;
        var kind = _kind;
        var filter = Filter;
        var childKind = ChildKind;
        var childFilter = ChildFilter;
        var prefix = Prefix;
        token.ThrowIfCancellationRequested();

        // A refresh revokes explicit reveals without changing navigation or debounce identity.
        _revealRevision++;
        StateHasChanged();
        try
        {
            var epoch = Live.InstanceEpoch;
            var reloadCatalog = epoch != _catalogEpoch;
            var context = _context is null || reloadCatalog ? await Api.ContextAsync(token) : _context;
            if (!ExplorerLocation.ValidAccount(account))
                throw new FormatException("Account ID must contain exactly 12 digits. Choose a valid account.");
            var accounts = await Api.AccountsAsync(token);
            token.ThrowIfCancellationRequested();
            if (!accounts.Contains(account))
            {
                _accounts = accounts;
                _context = context; _account = account;
                ClearLiveInspection();
                _resources = null;
                _error = $"Account {account} has no retained resources and is not the default. Select a known account.";
                return;
            }

            var services = _services.Count > 0 && !reloadCatalog ? _services : await Api.ServicesAsync(account, token);
            var selectedService = services.FirstOrDefault(value => value.Id == service);
            if (selectedService is not null && string.IsNullOrEmpty(kind))
                kind = InspectionProfile.RootKinds(selectedService).FirstOrDefault(value => value.Id == (Kind ?? path.FirstOrDefault()?.Kind))?.Id
                    ?? InspectionProfile.RootKinds(selectedService).FirstOrDefault()?.Id ?? "";
            AdminPage<AdminResourceSummary>? resources = null, children = null;
            AdminResourceDetail? detail = null, selected = null;
            AdminContent? content = null, selectedContent = null;
            AdminPage<AdminConnection>? connections = null, selectedConnections = null;
            string? error = null, childError = null;
            var missing = false;

            if (service is not null)
            {
                if (selectedService is null)
                    throw new FormatException("This service is not in the instance catalog. Choose a service from the breadcrumb.");
                if (selectedService.Availability != "disabled")
                {
                    if (InspectionProfile.RootKinds(selectedService).Count > 0)
                        resources = await Api.ResourcesAsync(service, account, kind, filter, token);
                    if (path.Length > 0)
                    {
                        try
                        {
                            detail = await Api.DetailAsync(service, account, path, token);
                            // A previously missing/configuration-only resource may now expose content.
                            tab = Tab is "configuration" or "connections" or "activity" ? Tab :
                                detail is { HasChildren: false, HasContent: false } ? "configuration" : "contents";
                            if (tab == "connections" && detail.HasConnections)
                                connections = await Api.ConnectionsAsync(service, account, path, token);
                            if (tab == "contents")
                            {
                                if (detail.HasChildren)
                                    children = await Api.ChildrenAsync(service, account, path, childKind, childFilter, prefix, token);
                                if (detail.HasContent)
                                    content = await Api.ContentAsync(service, account, path, token);
                                if (item.Length > 0)
                                {
                                    try
                                    {
                                        AdminKey[] selectedPath = [.. path, .. item];
                                        selected = await Api.DetailAsync(service, account, selectedPath, token);
                                        if (selected.HasContent)
                                            selectedContent = await Api.ContentAsync(service, account, selectedPath, token);
                                        if (selected.HasConnections)
                                            selectedConnections = await Api.ConnectionsAsync(service, account, selectedPath, token);
                                    }
                                    catch (AdminApiException exception) when (exception.Status == HttpStatusCode.NotFound)
                                    {
                                        selected = null; selectedContent = null; selectedConnections = null;
                                        childError = "This entry no longer exists. Select another entry or wait for it to return.";
                                    }
                                }
                            }
                        }
                        catch (AdminApiException exception) when (exception.Status == HttpStatusCode.NotFound)
                        {
                            missing = true;
                            detail = null; content = null; children = null; connections = null;
                            error = "This resource no longer exists in the selected account. Return to the resource list or wait for it to return.";
                        }
                    }
                }
            }

            token.ThrowIfCancellationRequested();
            if (generation != _generation) return;
            _context = context; _account = account; _kind = kind;
            _catalogEpoch = epoch;
            _accounts = accounts; _services = services; _service = selectedService;
            _path = path; _item = item;
            _resources = resources; _detail = detail; _content = content;
            _children = children; _connections = connections;
            _selectedDetail = selected; _selectedContent = selectedContent; _selectedConnections = selectedConnections;
            _error = error; _childError = childError; _missing = missing; _selectedConnectionError = null;
            _captured = resources?.CapturedAt ?? DateTimeOffset.UtcNow;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { throw; }
        catch (Exception)
        {
            if (!token.IsCancellationRequested && generation == _generation)
                _error = "Live inspection could not be refreshed. Previously loaded data may be stale. Use Retry in the live controls.";
            throw;
        }
        finally
        {
            if (!token.IsCancellationRequested && generation == _generation) StateHasChanged();
        }
    }

    private void ClearLiveInspection()
    {
        _detail = null; _selectedDetail = null; _content = null; _selectedContent = null;
        _children = null; _connections = null; _selectedConnections = null;
        _childError = null; _selectedConnectionError = null; _missing = false;
    }
}
