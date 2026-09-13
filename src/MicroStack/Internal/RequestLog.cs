using MicroStack.Internal.Admin;

namespace MicroStack.Internal;

internal sealed record RequestLogEntry(
    string Service,
    string Action,
    string AccountId,
    DateTimeOffset Timestamp,
    int StatusCode,
    long DurationMs);

/// <summary>
/// In-memory fixed-size request log for admin diagnostics.
/// </summary>
internal sealed class RequestLog
{
    private readonly RequestLogEntry[] _entries;
    private int _nextIndex;
    private int _count;
    private readonly Lock _lock = new();
    private readonly AdminChangeHub? _changes;

    internal RequestLog(int capacity = 1000, AdminChangeHub? changes = null)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be greater than zero.");

        _entries = new RequestLogEntry[capacity];
        _changes = changes;
    }

    internal void Add(RequestLogEntry entry)
    {
        lock (_lock)
        {
            _entries[_nextIndex] = entry;
            _nextIndex = (_nextIndex + 1) % _entries.Length;
            if (_count < _entries.Length)
                _count++;
        }
        _changes?.Publish(AdminDirty.Activity);
    }

    internal List<RequestLogEntry> GetEntries(int limit = 1000)
    {
        if (limit <= 0)
            return [];

        lock (_lock)
        {
            if (_count == 0)
                return [];

            var take = Math.Min(limit, _count);
            var result = new List<RequestLogEntry>(take);
            var newestIndex = (_nextIndex - 1 + _entries.Length) % _entries.Length;

            for (var i = 0; i < take; i++)
            {
                var index = (newestIndex - i + _entries.Length) % _entries.Length;
                result.Add(_entries[index]);
            }

            return result;
        }
    }

    internal void Clear()
    {
        lock (_lock)
        {
            Array.Clear(_entries);
            _nextIndex = 0;
            _count = 0;
        }
        _changes?.Publish(AdminDirty.Activity);
    }
}
