using System.Text.Json;
using Microsoft.JSInterop;

namespace MicroStack.UI.Client.Services;

/// <summary>One bounded refresh scheduler and browser event stream for the current tab.</summary>
public sealed class LiveUpdateCoordinator(IJSRuntime js, TimeProvider? timeProvider = null) : IAsyncDisposable
{
    private readonly TimeProvider _clock = timeProvider ?? TimeProvider.System;
    private readonly SemaphoreSlim _lifecycle = new(1, 1);
    private readonly object _sync = new();
    private readonly List<Registration> _registrations = [];
    private CancellationTokenSource _generation = new();
    private IJSObjectReference? _module;
    private IJSObjectReference? _bridge;
    private DotNetObjectReference<LiveUpdateCoordinator>? _reference;
    private Task _worker = Task.CompletedTask;
    private bool _working, _dirty, _manualDirty, _disposed, _open, _started, _failedProtocol;
    private string? _account;
    private long _transportGeneration;
    private DateTimeOffset? _lastStart;
    private string? _epoch;
    private long _sequence;

    public event Action? Changed;
    public string? InstanceEpoch => _epoch;
    public bool Paused { get; private set; }
    public bool Hidden { get; private set; }
    public DateTimeOffset? LastSuccess { get; private set; }
    public string? Error { get; private set; }
    public string? ConnectionError { get; private set; }
    internal Task PendingRefresh { get { lock (_sync) return _worker; } }
    public string Mode => Paused || Hidden ? "Paused"
        : Error is not null || _failedProtocol ? "Stale"
        : _open && LastSuccess is not null ? "Live"
        : ConnectionError is not null ? "Reconnecting" : "Connecting";
    private bool Automatic => !Paused && !Hidden && !_disposed;

    // Register after the page's independent navigation load is ready to accept refreshes.
    public async Task<IAsyncDisposable> RegisterAsync(Func<CancellationToken, Task> refresh, string? accountId = null)
    {
        ArgumentNullException.ThrowIfNull(refresh);
        if (accountId is not null && !ExplorerLocation.ValidAccount(accountId))
            throw new ArgumentException("Account ID must contain exactly 12 ASCII digits.", nameof(accountId));
        await _lifecycle.WaitAsync();
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var registration = new Registration(this, refresh, accountId);
            lock (_sync) _registrations.Add(registration);
            await ReconfigureAsync();
            return registration;
        }
        finally { _lifecycle.Release(); }
    }

    public Task PauseAsync() => SetPausedAsync(true);
    public Task ResumeAsync() => SetPausedAsync(false);
    public Task Pause() => PauseAsync();
    public Task Resume() => ResumeAsync();

    private async Task SetPausedAsync(bool paused)
    {
        await _lifecycle.WaitAsync();
        try
        {
            if (_disposed || Paused == paused) return;
            Paused = paused;
            await ReconfigureAsync();
        }
        finally { _lifecycle.Release(); }
    }

    public Task RefreshOnceAsync() => QueueRefresh(manual: true);

    public async Task RetryAsync()
    {
        await _lifecycle.WaitAsync();
        try
        {
            if (_disposed) return;
            _failedProtocol = false;
            await StopStreamAsync();
            await ReconfigureAsync(catchup: false);
        }
        finally { _lifecycle.Release(); }
        await RefreshOnceAsync();
    }

    [JSInvokable]
    public async Task OnVisibilityChanged(bool visible)
    {
        await _lifecycle.WaitAsync();
        try
        {
            if (_disposed || Hidden == !visible) return;
            Hidden = !visible;
            await ReconfigureAsync();
        }
        finally { _lifecycle.Release(); }
    }

    [JSInvokable]
    public Task OnConnectionChanged(long generation, bool open)
    {
        if (_disposed || generation != _transportGeneration || !Automatic) return Task.CompletedTask;
        _open = open;
        ConnectionError = open ? null : "The live connection was interrupted. Reconnecting automatically.";
        Changed?.Invoke();
        if (open) _ = QueueRefresh();
        return Task.CompletedTask;
    }

    [JSInvokable]
    public async Task OnChange(long generation, string json)
    {
        if (_disposed || generation != _transportGeneration || !Automatic) return;
        try
        {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            if (root.GetProperty("version").GetInt32() != 1) throw new JsonException();
            var epoch = root.GetProperty("epoch").GetString();
            var sequence = root.GetProperty("sequence").GetInt64();
            if (string.IsNullOrWhiteSpace(epoch) || sequence < 0) throw new JsonException();
            var relevant = root.GetProperty("resync").GetBoolean();
            foreach (var category in new[] { "resources", "accounts", "instance", "activity" })
                relevant |= root.GetProperty(category).GetBoolean();
            // Gaps and restarts are resync hints, never a claim that events can be replayed.
            relevant |= _epoch != epoch || sequence != _sequence + 1;
            _epoch = epoch;
            _sequence = sequence;
            if (relevant) _ = QueueRefresh();
        }
        catch (Exception exception) when (exception is JsonException or InvalidOperationException or KeyNotFoundException or FormatException)
        {
            await _lifecycle.WaitAsync();
            try
            {
                if (_disposed || generation != _transportGeneration) return;
                _failedProtocol = true;
                await StopStreamAsync();
                ConnectionError = "The live server sent an unsupported or malformed notification. Retry to reconnect.";
                Changed?.Invoke();
            }
            finally { _lifecycle.Release(); }
        }
    }

    private async Task ReconfigureAsync(bool catchup = true)
    {
        Invalidate();
        string? account;
        bool hasRegistrations;
        lock (_sync)
        {
            account = _registrations.FirstOrDefault()?.Account;
            // Filter only when every consumer observes the same account. Global or mixed
            // consumers need the union of resource hints, without account identifiers.
            if (_registrations.Any(item => item.Account != account)) account = null;
            hasRegistrations = _registrations.Count != 0;
        }
        if (!Automatic || !hasRegistrations || _account != account) await StopStreamAsync();
        _account = account;
        if (Automatic && hasRegistrations)
        {
            try
            {
                if (_bridge is null)
                {
                    _module ??= await js.InvokeAsync<IJSObjectReference>("import", "./live.js");
                    if (_module is null) throw new InvalidOperationException("The live module is unavailable.");
                    _reference ??= DotNetObjectReference.Create(this);
                    _bridge = await _module.InvokeAsync<IJSObjectReference>("create", _reference);
                    if (_bridge is null) throw new InvalidOperationException("The live transport is unavailable.");
                    Hidden = !await _bridge.InvokeAsync<bool>("isVisible");
                }
                if (Automatic && !_started && !_failedProtocol)
                {
                    _open = false;
                    ConnectionError = null;
                    await _bridge.InvokeVoidAsync("start", _account, ++_transportGeneration);
                    _started = true;
                }
            }
            catch (Exception exception) when (exception is JSException or InvalidOperationException)
            {
                ConnectionError = "The live connection could not start. Retry to reconnect.";
            }
            if (Automatic)
            {
                _ = ReconcileAsync(_generation.Token);
                if (catchup) _ = QueueRefresh();
            }
        }
        Changed?.Invoke();
    }

    private void Invalidate()
    {
        CancellationTokenSource old;
        lock (_sync)
        {
            old = _generation;
            _generation = new();
            _dirty = _manualDirty = false;
        }
        old.Cancel();
        old.Dispose();
    }

    private async Task ReconcileAsync(CancellationToken token)
    {
        try
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(5), _clock, token);
                _ = QueueRefresh();
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
    }

    private Task QueueRefresh(bool manual = false)
    {
        lock (_sync)
        {
            if (_disposed || _registrations.Count == 0 || Hidden || (!Automatic && !manual))
                return Task.CompletedTask;
            _dirty = true;
            _manualDirty |= manual;
            if (!_working)
            {
                _working = true;
                _worker = RefreshLoopAsync();
            }
            return _worker;
        }
    }

    private async Task RefreshLoopAsync()
    {
        await Task.Yield();
        while (true)
        {
            CancellationToken token;
            Registration[] registrations;
            lock (_sync)
            {
                if (_disposed || !_dirty || Hidden || (!Automatic && !_manualDirty))
                {
                    _working = false;
                    return;
                }
                token = _generation.Token;
                registrations = _registrations.ToArray();
                _dirty = _manualDirty = false;
            }
            try
            {
                if (_lastStart is { } lastStart)
                {
                    var remaining = TimeSpan.FromMilliseconds(500) - (_clock.GetUtcNow() - lastStart);
                    if (remaining > TimeSpan.Zero) await Task.Delay(remaining, _clock, token);
                }
                token.ThrowIfCancellationRequested();
                _lastStart = _clock.GetUtcNow();
                string? error = null;
                foreach (var registration in registrations)
                {
                    token.ThrowIfCancellationRequested();
                    try { await registration.Refresh(token); }
                    catch (OperationCanceledException) when (token.IsCancellationRequested) { throw; }
                    catch (Exception) { error = "The visible snapshot could not refresh. Retry to catch up."; }
                }
                token.ThrowIfCancellationRequested();
                Error = error;
                if (error is null) LastSuccess = _clock.GetUtcNow();
                Changed?.Invoke();
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        }
    }

    private async Task StopStreamAsync()
    {
        ++_transportGeneration;
        _open = _started = false;
        _epoch = null;
        _sequence = 0;
        if (_bridge is not null)
        {
            try { await _bridge.InvokeVoidAsync("stop"); }
            catch (JSException) { }
        }
    }

    private async ValueTask RemoveAsync(Registration registration)
    {
        await _lifecycle.WaitAsync();
        try
        {
            if (_disposed) return;
            lock (_sync) _registrations.Remove(registration);
            await ReconfigureAsync();
        }
        finally { _lifecycle.Release(); }
    }

    public async ValueTask DisposeAsync()
    {
        await _lifecycle.WaitAsync();
        try
        {
            if (_disposed) return;
            _disposed = true;
            Invalidate();
            lock (_sync) _registrations.Clear();
            await StopStreamAsync();
            if (_bridge is not null)
            {
                try { await _bridge.InvokeVoidAsync("dispose"); }
                catch (JSException) { }
                await _bridge.DisposeAsync();
            }
            if (_module is not null) await _module.DisposeAsync();
            _reference?.Dispose();
            _generation.Dispose();
            Changed = null;
        }
        finally { _lifecycle.Release(); }
    }

    private sealed class Registration(LiveUpdateCoordinator owner, Func<CancellationToken, Task> refresh, string? account) : IAsyncDisposable
    {
        private bool _disposed;
        public Func<CancellationToken, Task> Refresh { get; } = refresh;
        public string? Account { get; } = account;
        public ValueTask DisposeAsync()
        {
            if (_disposed) return ValueTask.CompletedTask;
            _disposed = true;
            return owner.RemoveAsync(this);
        }
    }
}
