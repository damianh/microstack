using Bunit;
using MicroStack.UI.Client.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.JSInterop;
using Xunit;

namespace MicroStack.UI.Tests;

public sealed class LiveUpdateCoordinatorTests
{
    private const string Account = "123456789012";
    private const string Change = """{"version":1,"epoch":"test","sequence":1,"resync":true,"resources":true,"accounts":false,"instance":false,"activity":false}""";

    [Fact]
    public async Task Initializes_stream_before_catchup_and_disposes_all_browser_resources()
    {
        var browser = new Browser();
        var clock = new ManualClock();
        var coordinator = new LiveUpdateCoordinator(browser, clock);
        var calls = 0;
        var registration = await coordinator.RegisterAsync(_ =>
        {
            Assert.True(browser.Started);
            calls++;
            return Task.CompletedTask;
        }, Account);
        await coordinator.PendingRefresh;
        Assert.Equal(1, calls);
        Assert.Equal(Account, browser.Account);
        Assert.Equal("./live.js", browser.Import);
        Assert.NotNull(coordinator.LastSuccess);
        await coordinator.OnConnectionChanged(browser.Generation, true);
        Assert.Equal("Live", coordinator.Mode);
        await registration.DisposeAsync();
        Assert.False(browser.Started);
        await coordinator.DisposeAsync();
        Assert.True(browser.ListenerDisposed);
        Assert.Equal(2, browser.ReferencesDisposed);
    }

    [Fact]
    public async Task Bursts_merge_into_one_trailing_batch_without_overlapping_reads()
    {
        var browser = new Browser();
        var clock = new ManualClock();
        await using var coordinator = new LiveUpdateCoordinator(browser, clock);
        var entered = Completion();
        var release = Completion();
        var calls = 0;
        var active = 0;
        await using var registration = await coordinator.RegisterAsync(async _ =>
        {
            Assert.Equal(1, ++active);
            if (++calls == 1)
            {
                entered.SetResult();
                await release.Task;
            }
            active--;
        });
        await entered.Task;
        for (var i = 0; i < 100; i++) await coordinator.OnChange(browser.Generation, Change);
        Assert.Equal(1, calls);
        release.SetResult();
        await clock.WaitForTimerAsync(500);
        clock.Advance(499);
        Assert.Equal(1, calls);
        clock.Advance(1);
        await coordinator.PendingRefresh;
        Assert.Equal(2, calls);
        clock.Advance(500);
        await coordinator.PendingRefresh;
        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task Pause_cancels_inflight_snapshot_and_manual_refresh_does_not_resume()
    {
        var browser = new Browser();
        var clock = new ManualClock();
        await using var coordinator = new LiveUpdateCoordinator(browser, clock);
        var entered = Completion();
        var release = Completion();
        CancellationToken received = default;
        var committed = 0;
        var calls = 0;
        await using var registration = await coordinator.RegisterAsync(async token =>
        {
            received = token;
            if (++calls == 1)
            {
                entered.SetResult();
                await release.Task;
            }
            token.ThrowIfCancellationRequested();
            committed++;
        });
        await entered.Task;
        await coordinator.PauseAsync();
        Assert.True(received.IsCancellationRequested);
        Assert.False(browser.Started);
        release.SetResult();
        await coordinator.PendingRefresh;
        Assert.Equal(0, committed);
        clock.Advance(5_000);
        await coordinator.PendingRefresh;
        Assert.Equal(1, calls);
        await coordinator.RefreshOnceAsync();
        Assert.Equal(1, committed);
        Assert.True(coordinator.Paused);
        Assert.Equal("Paused", coordinator.Mode);
        Assert.False(browser.Started);
    }

    [Fact]
    public async Task Pause_survives_navigation_and_resume_catches_up_new_scope()
    {
        var browser = new Browser();
        var clock = new ManualClock();
        await using var coordinator = new LiveUpdateCoordinator(browser, clock);
        await coordinator.PauseAsync();
        var first = await coordinator.RegisterAsync(_ => throw new InvalidOperationException(), Account);
        await first.DisposeAsync();
        var calls = 0;
        await using var second = await coordinator.RegisterAsync(_ => { calls++; return Task.CompletedTask; }, "999999999999");
        Assert.True(coordinator.Paused);
        Assert.Equal(0, calls);
        Assert.False(browser.Started);
        await coordinator.ResumeAsync();
        await coordinator.PendingRefresh;
        Assert.Equal(1, calls);
        Assert.Equal("999999999999", browser.Account);
    }

    [Fact]
    public async Task Pause_discards_throttled_trailing_work_and_old_stream_hints()
    {
        var browser = new Browser();
        var clock = new ManualClock();
        await using var coordinator = new LiveUpdateCoordinator(browser, clock);
        var calls = 0;
        await using var registration = await coordinator.RegisterAsync(_ => { calls++; return Task.CompletedTask; });
        await coordinator.PendingRefresh;
        var generation = browser.Generation;
        await coordinator.OnChange(generation, Change);
        await clock.WaitForTimerAsync(500);
        await coordinator.PauseAsync();
        await coordinator.OnChange(generation, Change);
        clock.Advance(5_000);
        await coordinator.PendingRefresh;
        Assert.Equal(1, calls);
        await coordinator.ResumeAsync();
        await coordinator.PendingRefresh;
        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task Retry_invalidates_pre_reset_manual_read_and_catches_up_without_resuming()
    {
        var browser = new Browser();
        var clock = new ManualClock();
        await using var coordinator = new LiveUpdateCoordinator(browser, clock);
        await coordinator.PauseAsync();
        var entered = Completion();
        var release = Completion();
        var calls = 0;
        var commits = 0;
        CancellationToken received = default;
        await using var registration = await coordinator.RegisterAsync(async token =>
        {
            if (++calls == 1)
            {
                received = token;
                entered.SetResult();
                await release.Task;
            }
            token.ThrowIfCancellationRequested();
            commits++;
        });
        var manual = coordinator.RefreshOnceAsync();
        await entered.Task;
        var retry = coordinator.RetryAsync();
        Assert.True(received.IsCancellationRequested);
        release.SetResult();
        await clock.WaitForTimerAsync(500);
        clock.Advance(500);
        await Task.WhenAll(manual, retry);
        Assert.Equal(2, calls);
        Assert.Equal(1, commits);
        Assert.True(coordinator.Paused);
        Assert.False(browser.Started);
    }

    [Fact]
    public async Task Hidden_tab_suspends_stream_and_timer_then_catches_up_when_visible()
    {
        var browser = new Browser { Visible = false };
        var clock = new ManualClock();
        await using var coordinator = new LiveUpdateCoordinator(browser, clock);
        var calls = 0;
        await using var registration = await coordinator.RegisterAsync(_ => { calls++; return Task.CompletedTask; });
        clock.Advance(20_000);
        Assert.Equal(0, calls);
        Assert.False(browser.Started);
        browser.Visible = true;
        await coordinator.OnVisibilityChanged(true);
        await coordinator.PendingRefresh;
        Assert.Equal(1, calls);
        Assert.True(browser.Started);
        await coordinator.OnVisibilityChanged(false);
        clock.Advance(20_000);
        Assert.Equal(1, calls);
        Assert.False(browser.Started);
    }

    [Fact]
    public async Task Safety_timer_reconciles_while_connection_is_reconnecting()
    {
        var browser = new Browser();
        var clock = new ManualClock();
        await using var coordinator = new LiveUpdateCoordinator(browser, clock);
        var calls = 0;
        await using var registration = await coordinator.RegisterAsync(_ => { calls++; return Task.CompletedTask; });
        await coordinator.PendingRefresh;
        await coordinator.OnConnectionChanged(browser.Generation, false);
        clock.Advance(5_000);
        await clock.WaitForTimerAsync(5_000);
        await coordinator.PendingRefresh;
        Assert.Equal(2, calls);
        Assert.Equal("Reconnecting", coordinator.Mode);
        Assert.Null(coordinator.Error);
        Assert.NotNull(coordinator.ConnectionError);
    }

    [Fact]
    public async Task Snapshot_failure_stays_stale_even_when_stream_is_open_then_retry_recovers()
    {
        var browser = new Browser();
        var clock = new ManualClock();
        await using var coordinator = new LiveUpdateCoordinator(browser, clock);
        var fail = true;
        await using var registration = await coordinator.RegisterAsync(_ =>
            fail ? Task.FromException(new HttpRequestException("private diagnostic")) : Task.CompletedTask);
        await coordinator.PendingRefresh;
        await coordinator.OnConnectionChanged(browser.Generation, true);
        Assert.Equal("Stale", coordinator.Mode);
        Assert.NotNull(coordinator.Error);
        Assert.Null(coordinator.LastSuccess);
        fail = false;
        var retry = coordinator.RetryAsync();
        await clock.WaitForTimerAsync(500);
        clock.Advance(500);
        await retry;
        Assert.Null(coordinator.Error);
        Assert.NotNull(coordinator.LastSuccess);
        await coordinator.OnConnectionChanged(browser.Generation, true);
        Assert.Equal("Live", coordinator.Mode);
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("not json")]
    [InlineData("""{"version":2,"epoch":"test","sequence":1,"resync":true,"resources":true,"accounts":false,"instance":false,"activity":false}""")]
    public async Task Malformed_protocol_is_explicitly_stale_and_closes_stream(string json)
    {
        var browser = new Browser();
        await using var coordinator = new LiveUpdateCoordinator(browser, new ManualClock());
        await using var registration = await coordinator.RegisterAsync(_ => Task.CompletedTask);
        await coordinator.PendingRefresh;
        await coordinator.OnChange(browser.Generation, json);
        Assert.Equal("Stale", coordinator.Mode);
        Assert.NotNull(coordinator.ConnectionError);
        Assert.Null(coordinator.Error);
        Assert.False(browser.Started);
    }

    [Fact]
    public async Task Scope_change_cancels_old_generation_and_ignores_old_stream_notifications()
    {
        var browser = new Browser();
        var clock = new ManualClock();
        await using var coordinator = new LiveUpdateCoordinator(browser, clock);
        var entered = Completion();
        var release = Completion();
        CancellationToken received = default;
        var first = await coordinator.RegisterAsync(async token =>
        {
            received = token;
            entered.SetResult();
            await release.Task;
        }, Account);
        await entered.Task;
        var oldGeneration = browser.Generation;
        await first.DisposeAsync();
        var calls = 0;
        await using var second = await coordinator.RegisterAsync(_ => { calls++; return Task.CompletedTask; }, "999999999999");
        Assert.True(received.IsCancellationRequested);
        await coordinator.OnChange(oldGeneration, "{}");
        Assert.Null(coordinator.ConnectionError);
        Assert.Equal(0, calls);
        release.SetResult();
        await clock.WaitForTimerAsync(500);
        clock.Advance(500);
        await coordinator.PendingRefresh;
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task Shell_and_page_share_one_connection_and_every_visible_callback_runs()
    {
        var browser = new Browser();
        var clock = new ManualClock();
        await using var coordinator = new LiveUpdateCoordinator(browser, clock);
        var shellCalls = 0;
        var pageCalls = 0;
        await coordinator.PauseAsync();
        await using var shell = await coordinator.RegisterAsync(_ => { shellCalls++; return Task.CompletedTask; });
        await using var page = await coordinator.RegisterAsync(_ => { pageCalls++; return Task.CompletedTask; }, Account);
        await coordinator.ResumeAsync();
        await coordinator.PendingRefresh;
        Assert.Equal(1, browser.Starts);
        Assert.Null(browser.Account);
        Assert.Equal(1, shellCalls);
        Assert.Equal(1, pageCalls);
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData("999999999999", null)]
    [InlineData(Account, Account)]
    public async Task Transport_scope_is_the_union_of_all_registrations(string? secondAccount, string? expectedAccount)
    {
        var browser = new Browser();
        var clock = new ManualClock();
        await using var coordinator = new LiveUpdateCoordinator(browser, clock);
        await coordinator.PauseAsync();
        await using var first = await coordinator.RegisterAsync(_ => Task.CompletedTask, Account);
        var second = await coordinator.RegisterAsync(_ => Task.CompletedTask, secondAccount);
        await coordinator.ResumeAsync();
        await coordinator.PendingRefresh;
        Assert.Equal(expectedAccount, browser.Account);
        await second.DisposeAsync();
        Assert.Equal(Account, browser.Account);
        await clock.WaitForTimerAsync(500);
        clock.Advance(500);
        await coordinator.PendingRefresh;
    }

    [Fact]
    public async Task Adding_same_account_activity_registration_keeps_existing_stream()
    {
        var browser = new Browser();
        var clock = new ManualClock();
        await using var coordinator = new LiveUpdateCoordinator(browser, clock);
        var resources = 0;
        var activity = 0;
        await using var first = await coordinator.RegisterAsync(_ => { resources++; return Task.CompletedTask; }, Account);
        await coordinator.PendingRefresh;
        var generation = browser.Generation;
        await using var second = await coordinator.RegisterAsync(_ => { activity++; return Task.CompletedTask; }, Account);
        await clock.WaitForTimerAsync(500);
        clock.Advance(500);
        await coordinator.PendingRefresh;
        Assert.Equal(1, browser.Starts);
        Assert.Equal(generation, browser.Generation);
        Assert.Equal(2, resources);
        Assert.Equal(1, activity);
    }

    [Fact]
    public async Task Missing_browser_module_is_an_explicit_connection_failure_not_a_null_reference()
    {
        var browser = new Browser { ModuleAvailable = false };
        await using var coordinator = new LiveUpdateCoordinator(browser, new ManualClock());
        await using var registration = await coordinator.RegisterAsync(_ => Task.CompletedTask);
        await coordinator.PendingRefresh;
        Assert.Equal("Reconnecting", coordinator.Mode);
        Assert.NotNull(coordinator.ConnectionError);
        Assert.NotNull(coordinator.LastSuccess);
        Assert.Null(coordinator.Error);
    }

    [Fact]
    public async Task Scoped_bunit_service_uses_an_explicit_injected_browser_transport()
    {
        await using var context = new BunitContext();
        context.JSInterop.Mode = JSRuntimeMode.Loose;
        var browser = new Browser();
        context.Services.AddSingleton<IJSRuntime>(browser);
        context.Services.AddScoped<LiveUpdateCoordinator>();
        var coordinator = context.Services.GetRequiredService<LiveUpdateCoordinator>();
        await using var registration = await coordinator.RegisterAsync(_ => Task.CompletedTask);
        await coordinator.PendingRefresh;
        Assert.False(coordinator.Hidden);
        Assert.True(browser.Started);
        Assert.Null(coordinator.ConnectionError);
        Assert.NotNull(coordinator.LastSuccess);
        Assert.Null(coordinator.Error);
    }

    private static TaskCompletionSource Completion() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class Browser : IJSRuntime, IJSObjectReference
    {
        public string? Import { get; private set; }
        public string? Account { get; private set; }
        public long Generation { get; private set; }
        public bool Visible { get; set; } = true;
        public bool ModuleAvailable { get; set; } = true;
        public bool Started { get; private set; }
        public bool ListenerDisposed { get; private set; }
        public int ReferencesDisposed { get; private set; }
        public int Starts { get; private set; }
        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, object?[]? args) =>
            InvokeAsync<TValue>(identifier, CancellationToken.None, args);
        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, CancellationToken cancellationToken, object?[]? args)
        {
            object? result = default(TValue);
            switch (identifier)
            {
                case "import": Import = (string)args![0]!; result = ModuleAvailable ? this : null; break;
                case "create": result = this; break;
                case "isVisible": result = Visible; break;
                case "start":
                    Account = (string?)args![0];
                    Generation = (long)args[1]!;
                    Started = true;
                    Starts++;
                    break;
                case "stop": Started = false; break;
                case "dispose": ListenerDisposed = true; break;
                default: throw new InvalidOperationException(identifier);
            }
            return ValueTask.FromResult((TValue)result!);
        }
        public ValueTask DisposeAsync() { ReferencesDisposed++; return ValueTask.CompletedTask; }
    }

    private sealed class ManualClock : TimeProvider
    {
        private readonly object _sync = new();
        private readonly List<ManualTimer> _timers = [];
        private readonly List<(long Due, TaskCompletionSource Signal)> _waiters = [];
        private long _milliseconds;
        public override DateTimeOffset GetUtcNow() => DateTimeOffset.UnixEpoch.AddMilliseconds(_milliseconds);
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            lock (_sync)
            {
                var timer = new ManualTimer(this, callback, state);
                _timers.Add(timer);
                timer.Change(dueTime, period);
                return timer;
            }
        }
        public Task WaitForTimerAsync(long milliseconds)
        {
            lock (_sync)
            {
                var due = _milliseconds + milliseconds;
                if (_timers.Any(timer => timer.Due == due)) return Task.CompletedTask;
                var signal = Completion();
                _waiters.Add((due, signal));
                return signal.Task;
            }
        }
        public void Advance(long milliseconds)
        {
            ManualTimer[] timers;
            lock (_sync)
            {
                _milliseconds += milliseconds;
                timers = _timers.Where(timer => timer.Due <= _milliseconds).ToArray();
                foreach (var timer in timers) timer.Due = long.MaxValue;
            }
            foreach (var timer in timers) timer.Tick();
        }
        private sealed class ManualTimer(ManualClock clock, TimerCallback callback, object? state) : ITimer
        {
            public long Due { get; set; } = long.MaxValue;
            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                lock (clock._sync)
                {
                    Due = dueTime == Timeout.InfiniteTimeSpan ? long.MaxValue : clock._milliseconds + (long)dueTime.TotalMilliseconds;
                    foreach (var waiter in clock._waiters.Where(waiter => waiter.Due == Due).ToArray())
                    {
                        clock._waiters.Remove(waiter);
                        waiter.Signal.SetResult();
                    }
                    return true;
                }
            }
            public void Tick() => callback(state);
            public void Dispose() { lock (clock._sync) Due = long.MaxValue; }
            public ValueTask DisposeAsync() { Dispose(); return ValueTask.CompletedTask; }
        }
    }
}
