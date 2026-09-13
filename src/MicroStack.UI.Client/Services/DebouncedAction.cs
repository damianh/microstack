namespace MicroStack.UI.Client.Services;

public sealed class DebouncedAction(TimeProvider? timeProvider = null) : IDisposable
{
    private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
    private CancellationTokenSource? _pending;
    private bool _disposed;

    public async Task RunAsync(Func<CancellationToken, Task> action, bool immediately = false)
    {
        Cancel();
        if (_disposed) return;
        var pending = _pending = new();
        var token = pending.Token;
        try
        {
            if (!immediately) await Task.Delay(TimeSpan.FromMilliseconds(250), _timeProvider, token);
            if (!token.IsCancellationRequested) await action(token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        finally
        {
            if (ReferenceEquals(_pending, pending)) _pending = null;
            pending.Dispose();
        }
    }

    public void Cancel()
    {
        _pending?.Cancel();
        _pending = null;
    }

    public void Dispose() { _disposed = true; Cancel(); }
}
