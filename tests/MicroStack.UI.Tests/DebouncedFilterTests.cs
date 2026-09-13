using Bunit;
using MicroStack.UI.Client.Components;
using MicroStack.UI.Client.Services;
using Microsoft.AspNetCore.Components.Web;
using Xunit;

namespace MicroStack.UI.Tests;

public sealed class DebouncedFilterTests
{
    [Fact]
    public async Task Burst_waits_250ms_and_only_applies_latest_value()
    {
        var clock = new ManualClock();
        using var debounce = new DebouncedAction(clock);
        var values = new List<string>();
        var first = debounce.RunAsync(_ => { values.Add("first"); return Task.CompletedTask; });
        clock.Advance(200);
        var second = debounce.RunAsync(_ => { values.Add("second"); return Task.CompletedTask; });
        clock.Advance(249);
        Assert.Empty(values);
        clock.Advance(1);
        await Task.WhenAll(first, second);
        Assert.Equal(["second"], values);
    }

    [Fact]
    public async Task Immediate_apply_cancels_pending_delay()
    {
        var clock = new ManualClock();
        using var debounce = new DebouncedAction(clock);
        var values = new List<string>();
        var pending = debounce.RunAsync(_ => { values.Add("delayed"); return Task.CompletedTask; });
        await debounce.RunAsync(_ => { values.Add("immediate"); return Task.CompletedTask; }, immediately: true);
        clock.Advance(250);
        await pending;
        Assert.Equal(["immediate"], values);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Scope_cancellation_and_disposal_cancel_inflight_work(bool dispose)
    {
        var clock = new ManualClock();
        using var debounce = new DebouncedAction(clock);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationToken received = default;
        var applied = false;
        var work = debounce.RunAsync(async token =>
        {
            received = token;
            entered.SetResult();
            await release.Task;
            if (!token.IsCancellationRequested) applied = true;
        });
        clock.Advance(250);
        await entered.Task;
        if (dispose) debounce.Dispose(); else debounce.Cancel();
        Assert.True(received.IsCancellationRequested);
        release.SetResult();
        await work;
        Assert.False(applied);
    }

    [Fact]
    public async Task Enter_and_clear_apply_immediately_in_the_same_filter()
    {
        using var context = new BunitContext();
        context.JSInterop.Mode = JSRuntimeMode.Loose;
        var clock = new ManualClock();
        var values = new List<string>();
        var view = context.Render<DebouncedFilter>(parameters => parameters
            .Add(component => component.Clock, clock)
            .Add(component => component.Changed, value => values.Add(value)));
        var input = view.Find("input");
        var typing = input.InputAsync(new() { Value = "orders" });
        Assert.Empty(values);
        await input.KeyDownAsync(new KeyboardEventArgs { Key = "Enter" });
        await typing;
        Assert.Equal(["orders"], values);
        Assert.Equal("orders", view.Find("input").GetAttribute("value"));
        await view.Find("button").ClickAsync(new());
        Assert.Equal(["orders", ""], values);
        Assert.Equal("", view.Find("input").GetAttribute("value"));
        Assert.Contains(context.JSInterop.Invocations, invocation => invocation.Identifier == "Blazor._internal.domWrapper.focus");
    }

    [Fact]
    public async Task Scope_change_discards_pending_text()
    {
        using var context = new BunitContext();
        var clock = new ManualClock();
        var values = new List<string>();
        var view = context.Render<DebouncedFilter>(parameters => parameters
            .Add(component => component.Clock, clock)
            .Add(component => component.Scope, "account-a")
            .Add(component => component.Changed, value => values.Add(value)));
        var typing = view.Find("input").InputAsync(new() { Value = "old-account" });
        view.Render(parameters => parameters.Add(component => component.Scope, "account-b").Add(component => component.Value, "new-account"));
        clock.Advance(250);
        await typing;
        Assert.Empty(values);
        Assert.Equal("new-account", view.Find("input").GetAttribute("value"));
    }

    [Fact]
    public async Task Acknowledging_an_older_submission_keeps_newer_typed_text()
    {
        using var context = new BunitContext();
        var clock = new ManualClock();
        var view = context.Render<DebouncedFilter>(parameters => parameters.Add(component => component.Clock, clock));
        var first = view.Find("input").InputAsync(new() { Value = "old" });
        await view.Find("input").KeyDownAsync(new KeyboardEventArgs { Key = "Enter" });
        await first;
        var second = view.Find("input").InputAsync(new() { Value = "newer" });
        view.Render(parameters => parameters.Add(component => component.Value, "old"));
        Assert.Equal("newer", view.Find("input").GetAttribute("value"));
        clock.Advance(250);
        await second;
    }

    [Fact]
    public async Task Disposing_filter_cancels_pending_callback()
    {
        using var context = new BunitContext();
        var clock = new ManualClock();
        var values = new List<string>();
        var view = context.Render<DebouncedFilter>(parameters => parameters
            .Add(component => component.Clock, clock)
            .Add(component => component.Changed, value => values.Add(value)));
        var typing = view.Find("input").InputAsync(new() { Value = "discard" });
        await view.InvokeAsync(() => view.Instance.Dispose());
        clock.Advance(250);
        await typing;
        Assert.Empty(values);
    }

    private sealed class ManualClock : TimeProvider
    {
        private readonly List<ManualTimer> _timers = [];
        private long _milliseconds;
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new ManualTimer(this, callback, state);
            _timers.Add(timer);
            timer.Change(dueTime, period);
            return timer;
        }
        public void Advance(long milliseconds)
        {
            _milliseconds += milliseconds;
            foreach (var timer in _timers.ToArray()) timer.Tick();
        }
        private sealed class ManualTimer(ManualClock clock, TimerCallback callback, object? state) : ITimer
        {
            private long _due = long.MaxValue;
            private bool _disposed;
            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                if (_disposed) return false;
                _due = dueTime == Timeout.InfiniteTimeSpan ? long.MaxValue : clock._milliseconds + (long)dueTime.TotalMilliseconds;
                return true;
            }
            public void Tick()
            {
                if (_disposed || _due > clock._milliseconds) return;
                _due = long.MaxValue;
                callback(state);
            }
            public void Dispose() => _disposed = true;
            public ValueTask DisposeAsync() { Dispose(); return ValueTask.CompletedTask; }
        }
    }
}
