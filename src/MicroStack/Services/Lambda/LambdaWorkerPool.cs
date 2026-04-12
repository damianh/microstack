namespace MicroStack.Services.Lambda;

/// <summary>
/// Manages a pool of Lambda worker processes, one per function name.
/// Workers are kept warm for subsequent invocations (warm start).
/// Thread-safe: all access is serialized via a lock.
/// </summary>
internal sealed class LambdaWorkerPool
{
    private readonly Dictionary<string, LambdaWorker> _workers = new(StringComparer.Ordinal);
    private readonly Lock _lock = new();

    /// <summary>
    /// Gets an existing alive worker for the function, or creates a new one.
    /// </summary>
    internal LambdaWorker GetOrCreate(string functionName, Dictionary<string, object?> config, byte[] codeZip)
    {
        lock (_lock)
        {
            if (_workers.TryGetValue(functionName, out var existing) && existing.IsAlive)
            {
                return existing;
            }

            // Dispose the old worker if it exists but is dead
            if (existing is not null)
            {
                existing.Dispose();
            }

            var worker = new LambdaWorker(functionName, config, codeZip);
            _workers[functionName] = worker;
            return worker;
        }
    }

    /// <summary>
    /// Invalidates (kills) the worker for the specified function, if any.
    /// Called when function code or configuration is updated, or when the function is deleted.
    /// </summary>
    internal void Invalidate(string functionName)
    {
        lock (_lock)
        {
            if (_workers.Remove(functionName, out var worker))
            {
                worker.Dispose();
            }
        }
    }

    /// <summary>
    /// Kills all workers and clears the pool. Called on reset.
    /// </summary>
    internal void Reset()
    {
        lock (_lock)
        {
            foreach (var worker in _workers.Values)
            {
                worker.Dispose();
            }

            _workers.Clear();
        }
    }
}
