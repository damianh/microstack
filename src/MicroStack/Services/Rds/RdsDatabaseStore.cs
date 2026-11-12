using Microsoft.Data.Sqlite;

namespace MicroStack.Services.Rds;

internal sealed class RdsDatabaseStore : IDisposable
{
    private readonly Lock _lock = new();
    private readonly Dictionary<string, RdsClusterDatabase> _databases = new(StringComparer.Ordinal);

    internal event Action<string>? ClusterRemoved;

    internal RdsClusterDatabase Create(string resourceArn)
    {
        lock (_lock)
        {
            if (_databases.ContainsKey(resourceArn))
                throw new InvalidOperationException($"A database already exists for cluster ARN {resourceArn}.");

            var database = new RdsClusterDatabase();
            _databases.Add(resourceArn, database);
            return database;
        }
    }

    internal bool TryGet(string resourceArn, out RdsClusterDatabase? database)
    {
        lock (_lock)
            return _databases.TryGetValue(resourceArn, out database);
    }

    internal void Delete(string resourceArn)
    {
        RdsClusterDatabase? database;
        lock (_lock)
        {
            if (!_databases.Remove(resourceArn, out database))
                return;
            database.Dispose();
        }
        ClusterRemoved?.Invoke(resourceArn);
    }

    internal void Reset()
    {
        string[] resourceArns;
        lock (_lock)
        {
            resourceArns = _databases.Keys.ToArray();
            foreach (var database in _databases.Values)
                database.Dispose();
            _databases.Clear();
        }

        foreach (var resourceArn in resourceArns)
            ClusterRemoved?.Invoke(resourceArn);
    }

    public void Dispose() => Reset();
}

internal sealed class RdsClusterDatabase : IDisposable
{
    private readonly Lock _lock = new();
    private readonly string _connectionString;
    private readonly SqliteConnection _anchorConnection;
    private readonly Dictionary<string, ActiveTransaction> _transactions = new(StringComparer.Ordinal);
    private bool _disposed;

    internal RdsClusterDatabase()
    {
        _connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = $"rds-{Guid.NewGuid():N}",
            Mode = SqliteOpenMode.Memory,
            Cache = SqliteCacheMode.Shared,
            Pooling = false,
            ForeignKeys = true,
        }.ToString();

        _anchorConnection = OpenConnection();
    }

    internal bool BeginTransaction(string transactionId)
    {
        lock (_lock)
        {
            ThrowIfDisposed();
            if (_transactions.ContainsKey(transactionId))
                return false;

            var connection = OpenConnection();
            SqliteTransaction transaction;
            try
            {
                transaction = connection.BeginTransaction();
            }
            catch (SqliteException)
            {
                connection.Dispose();
                throw;
            }

            _transactions.Add(transactionId, new ActiveTransaction(connection, transaction));
            return true;
        }
    }

    internal T Execute<T>(
        string? transactionId,
        Func<SqliteConnection, SqliteTransaction?, T> operation)
    {
        lock (_lock)
        {
            ThrowIfDisposed();

            if (transactionId is not null)
            {
                if (!_transactions.TryGetValue(transactionId, out var activeTransaction))
                    throw new KeyNotFoundException($"Transaction {transactionId} not found.");
                return operation(activeTransaction.Connection, activeTransaction.Transaction);
            }

            using var connection = OpenConnection();
            return operation(connection, null);
        }
    }

    internal bool CompleteTransaction(string transactionId, bool commit)
    {
        lock (_lock)
        {
            if (_disposed || !_transactions.Remove(transactionId, out var activeTransaction))
                return false;

            try
            {
                if (commit)
                    activeTransaction.Transaction.Commit();
                else
                    activeTransaction.Transaction.Rollback();
                return true;
            }
            finally
            {
                activeTransaction.Transaction.Dispose();
                activeTransaction.Connection.Dispose();
            }
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed)
                return;

            _disposed = true;
            foreach (var activeTransaction in _transactions.Values)
            {
                activeTransaction.Transaction.Dispose();
                activeTransaction.Connection.Dispose();
            }
            _transactions.Clear();
            _anchorConnection.Dispose();
        }
    }

    private SqliteConnection OpenConnection()
    {
        var connection = new SqliteConnection(_connectionString);
        connection.Open();
        return connection;
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    private sealed record ActiveTransaction(SqliteConnection Connection, SqliteTransaction Transaction);
}
