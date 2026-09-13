using System.Threading.Channels;
using MicroStack.Admin.Contracts;

namespace MicroStack.Internal.Admin;

[Flags]
internal enum AdminDirty
{
    None = 0, Resources = 1, Accounts = 2, Instance = 4, Activity = 8, All = 15
}

internal sealed class AdminChangeHub : IDisposable
{
    internal const int MaxSubscribers = 64;
    private readonly Lock _lock = new();
    private readonly HashSet<Subscription> _subscriptions = [];
    private readonly string _epoch = Guid.NewGuid().ToString("N");
    private long _sequence;
    private bool _disposed;

    internal Subscription? Subscribe(string? accountId)
    {
        lock (_lock)
        {
            if (_disposed || _subscriptions.Count >= MaxSubscribers)
                return null;
            var subscription = new Subscription(this, accountId);
            _subscriptions.Add(subscription);
            return subscription;
        }
    }

    internal void DispatchCompleted(string service, string accountId)
    {
        // These handlers have shared stores as well as account-owned resources.
        // Unknown handlers have unknown dependency scope, so broaden rather than drop hints.
        var global = service is "athena" or "events" or "firehose"
            || !AdminCatalog.Entries.Any(entry => entry.CanonicalHandler == service);
        Publish(AdminDirty.Resources | AdminDirty.Accounts, global ? null : accountId);
    }

    internal void Publish(AdminDirty dirty, string? accountId = null)
    {
        lock (_lock)
        {
            if (_disposed)
                return;
            var sequence = ++_sequence;
            foreach (var subscription in _subscriptions)
            {
                var filtered = accountId is not null && subscription.AccountId is not null
                    && accountId != subscription.AccountId
                    ? dirty & ~AdminDirty.Resources : dirty;
                if (filtered == AdminDirty.None)
                    continue;
                subscription.Dirty |= filtered;
                subscription.Sequence = sequence;
                subscription.Wakeup.Writer.TryWrite(true);
            }
        }
    }

    private AdminChangeEvent Take(Subscription subscription, bool resync)
    {
        lock (_lock)
        {
            var dirty = resync ? AdminDirty.All : subscription.Dirty;
            var sequence = resync ? _sequence : subscription.Sequence;
            subscription.Dirty = AdminDirty.None;
            subscription.Wakeup.Reader.TryRead(out _);
            return new(1, _epoch, sequence, resync,
                dirty.HasFlag(AdminDirty.Resources), dirty.HasFlag(AdminDirty.Accounts),
                dirty.HasFlag(AdminDirty.Instance), dirty.HasFlag(AdminDirty.Activity));
        }
    }

    private void Remove(Subscription subscription)
    {
        lock (_lock)
        {
            _subscriptions.Remove(subscription);
            subscription.Wakeup.Writer.TryComplete();
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            _disposed = true;
            foreach (var subscription in _subscriptions)
                subscription.Wakeup.Writer.TryComplete();
            _subscriptions.Clear();
        }
    }

    internal sealed class Subscription(AdminChangeHub owner, string? accountId) : IDisposable
    {
        internal string? AccountId { get; } = accountId;
        internal AdminDirty Dirty { get; set; }
        internal long Sequence { get; set; }
        internal Channel<bool> Wakeup { get; } = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            AllowSynchronousContinuations = false
        });
        internal AdminChangeEvent Take(bool resync = false) => owner.Take(this, resync);
        public void Dispose() => owner.Remove(this);
    }
}
