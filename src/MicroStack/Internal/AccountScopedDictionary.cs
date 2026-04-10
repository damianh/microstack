using System.Collections;
using System.Collections.Concurrent;

namespace MicroStack.Internal;

/// <summary>
/// A dictionary that automatically namespaces all keys by the current request's
/// AWS account ID (via <see cref="AccountContext"/>). Equivalent of Python's
/// <c>AccountScopedDict</c> in ministack/core/responses.py.
///
/// All reads and writes are scoped to the account returned by
/// <see cref="AccountContext.GetAccountId()"/> at the time of the call.
/// <see cref="Clear"/> wipes ALL accounts' data (used by reset).
/// </summary>
internal sealed class AccountScopedDictionary<TKey, TValue>
    : IEnumerable<KeyValuePair<TKey, TValue>>
    where TKey : notnull
{
    private readonly ConcurrentDictionary<(string AccountId, TKey Key), TValue> _data = new();

    // ── key helpers ───────────────────────────────────────────────────────────

    private (string, TKey) Scoped(TKey key) => (AccountContext.GetAccountId(), key);

    private bool IsMine((string AccountId, TKey Key) scoped) =>
        scoped.AccountId == AccountContext.GetAccountId();

    // ── dict-style API ────────────────────────────────────────────────────────

    internal TValue this[TKey key]
    {
        get => _data[Scoped(key)];
        set => _data[Scoped(key)] = value;
    }

    internal bool ContainsKey(TKey key) => _data.ContainsKey(Scoped(key));

    internal bool TryGetValue(TKey key, [System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] out TValue value) =>
        _data.TryGetValue(Scoped(key), out value);

    internal bool TryRemove(TKey key, [System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] out TValue value) =>
        _data.TryRemove(Scoped(key), out value);

    internal TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory) =>
        _data.GetOrAdd(Scoped(key), sk => valueFactory(sk.Key));

    internal TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory) =>
        _data.AddOrUpdate(Scoped(key), addValue, (sk, existing) => updateValueFactory(sk.Key, existing));

    internal bool TryAdd(TKey key, TValue value) =>
        _data.TryAdd(Scoped(key), value);

    internal int Count =>
        _data.Keys.Count(IsMine);

    internal IEnumerable<TKey> Keys =>
        _data.Keys.Where(IsMine).Select(k => k.Key);

    internal IEnumerable<TValue> Values =>
        _data.Where(kv => IsMine(kv.Key)).Select(kv => kv.Value);

    internal IEnumerable<KeyValuePair<TKey, TValue>> Items =>
        _data.Where(kv => IsMine(kv.Key))
             .Select(kv => new KeyValuePair<TKey, TValue>(kv.Key.Key, kv.Value));

    /// <summary>Clear ALL accounts' data (used by service Reset).</summary>
    internal void Clear() => _data.Clear();

    // ── serialization helpers ─────────────────────────────────────────────────

    /// <summary>
    /// Return ALL data (all accounts) as a flat dictionary keyed by
    /// ("accountId", key) pairs — for JSON persistence.
    /// </summary>
    internal IReadOnlyDictionary<(string AccountId, TKey Key), TValue> ToRaw() =>
        new Dictionary<(string, TKey), TValue>(_data);

    /// <summary>Restore ALL data from a previously serialized snapshot.</summary>
    internal void FromRaw(IEnumerable<KeyValuePair<(string AccountId, TKey Key), TValue>> entries)
    {
        _data.Clear();
        foreach (var kv in entries)
            _data[kv.Key] = kv.Value;
    }

    // ── IEnumerable (scoped to current account) ────────────────────────────────

    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() =>
        Items.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
