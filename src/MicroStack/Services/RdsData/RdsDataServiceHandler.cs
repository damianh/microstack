using System.Globalization;
using System.Text.Json;
using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Internal.Admin;
using MicroStack.Services.Rds;
using Microsoft.Data.Sqlite;

namespace MicroStack.Services.RdsData;

/// <summary>
/// RDS Data API service handler backed by an in-memory SQLite database per emulated RDS cluster.
/// </summary>
internal sealed class RdsDataServiceHandler : IServiceHandler, IAdminResourceSource
{
    public string ServiceName => "rds-data";

    private readonly RdsDatabaseStore _databaseStore;
    private readonly Dictionary<string, TransactionInfo> _transactions = new(StringComparer.Ordinal);
    private readonly Lock _lock = new();

    internal RdsDataServiceHandler() : this(new RdsDatabaseStore()) { }

    internal RdsDataServiceHandler(RdsDatabaseStore databaseStore)
    {
        _databaseStore = databaseStore;
        _databaseStore.ClusterRemoved += RemoveTransactionsForCluster;
    }

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        JsonElement data;
        if (request.Body.Length > 0)
        {
            try
            {
                using var doc = JsonDocument.Parse(request.Body);
                if (doc.RootElement.ValueKind != JsonValueKind.Object)
                    return Task.FromResult(ErrorJson("BadRequestException", "Request body must be a JSON object"));
                data = doc.RootElement.Clone();
            }
            catch (JsonException)
            {
                return Task.FromResult(ErrorJson("BadRequestException", "Invalid JSON in request body"));
            }
        }
        else
        {
            data = JsonDocument.Parse("{}").RootElement.Clone();
        }

        ServiceResponse response;
        lock (_lock)
        {
            response = request.Path switch
            {
                "/Execute" => ExecuteStatement(data),
                "/BeginTransaction" => BeginTransaction(data),
                "/CommitTransaction" => CompleteTransaction(data, commit: true),
                "/RollbackTransaction" => CompleteTransaction(data, commit: false),
                "/BatchExecute" => BatchExecuteStatement(data),
                _ => ErrorJson("BadRequestException", $"Unknown RDS Data API path: {request.Path}"),
            };
        }

        return Task.FromResult(response);
    }

    public void Reset()
    {
        _databaseStore.Reset();
        lock (_lock)
            _transactions.Clear();
    }

    public JsonElement? GetState() => null;

    public void RestoreState(JsonElement state) { }

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) => [];

    public IEnumerable<AdminNode> GetAdminResources(string serviceId) => [];

    public string? GetAdminNotice(string serviceId) =>
        "RDS Data API retains no database resources or SQL results beyond each cluster's in-memory SQLite database. Data is cleared when the cluster is deleted, MicroStack is reset, or MicroStack restarts.";

    private ServiceResponse ExecuteStatement(JsonElement data)
    {
        var resourceArn = GetString(data, "resourceArn");
        var secretArn = GetString(data, "secretArn");
        var sql = GetString(data, "sql");

        if (string.IsNullOrEmpty(resourceArn))
            return ErrorJson("BadRequestException", "resourceArn is required");
        if (string.IsNullOrEmpty(secretArn))
            return ErrorJson("BadRequestException", "secretArn is required");
        if (string.IsNullOrWhiteSpace(sql))
            return ErrorJson("BadRequestException", "sql is required");
        if (!TryGetOptionalTransactionId(data, out var transactionId, out var transactionError))
            return ErrorJson("BadRequestException", transactionError);
        if (!TryParseParameters(data, out var parameters, out var parameterError))
            return ErrorJson("BadRequestException", parameterError);
        if (!TryGetDatabase(resourceArn, out var database, out var notFound))
            return notFound!;
        if (transactionId is not null &&
            (!TryGetTransaction(transactionId, resourceArn, database!, out var transactionErrorResponse)))
            return transactionErrorResponse!;

        try
        {
            var includeMetadata = data.TryGetProperty("includeResultMetadata", out var metadataElement) &&
                metadataElement.ValueKind == JsonValueKind.True;
            var result = ExecuteSql(database!, sql, parameters, transactionId, includeMetadata);
            return AwsResponseHelpers.JsonResponse(result.Response);
        }
        catch (SqliteException exception)
        {
            return ErrorJson("BadRequestException", exception.Message);
        }
        catch (KeyNotFoundException)
        {
            return ErrorJson("NotFoundException", $"Transaction {transactionId} not found", 404);
        }
        catch (ObjectDisposedException)
        {
            return ErrorJson("BadRequestException", $"Database cluster not found for ARN: {resourceArn}");
        }
    }

    private ServiceResponse BeginTransaction(JsonElement data)
    {
        var resourceArn = GetString(data, "resourceArn");
        var secretArn = GetString(data, "secretArn");

        if (string.IsNullOrEmpty(resourceArn))
            return ErrorJson("BadRequestException", "resourceArn is required");
        if (string.IsNullOrEmpty(secretArn))
            return ErrorJson("BadRequestException", "secretArn is required");
        if (!TryGetDatabase(resourceArn, out var database, out var notFound))
            return notFound!;

        var transactionId = Guid.NewGuid().ToString("N");
        try
        {
            database!.BeginTransaction(transactionId);
            _transactions.Add(transactionId, new TransactionInfo(resourceArn, database));
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["transactionId"] = transactionId,
            });
        }
        catch (SqliteException exception)
        {
            return ErrorJson("BadRequestException", exception.Message);
        }
        catch (ObjectDisposedException)
        {
            return ErrorJson("BadRequestException", $"Database cluster not found for ARN: {resourceArn}");
        }
    }

    private ServiceResponse CompleteTransaction(JsonElement data, bool commit)
    {
        var transactionId = GetString(data, "transactionId");
        if (string.IsNullOrEmpty(transactionId))
            return ErrorJson("BadRequestException", "transactionId is required");

        if (!_transactions.Remove(transactionId, out var transaction))
            return ErrorJson("NotFoundException", $"Transaction {transactionId} not found", 404);

        try
        {
            if (!transaction.Database.CompleteTransaction(transactionId, commit))
                return ErrorJson("NotFoundException", $"Transaction {transactionId} not found", 404);
        }
        catch (SqliteException exception)
        {
            return ErrorJson("BadRequestException", exception.Message);
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["transactionStatus"] = commit ? "Transaction Committed" : "Transaction Rolled Back",
        });
    }

    private ServiceResponse BatchExecuteStatement(JsonElement data)
    {
        var resourceArn = GetString(data, "resourceArn");
        var secretArn = GetString(data, "secretArn");
        var sql = GetString(data, "sql");

        if (string.IsNullOrEmpty(resourceArn))
            return ErrorJson("BadRequestException", "resourceArn is required");
        if (string.IsNullOrEmpty(secretArn))
            return ErrorJson("BadRequestException", "secretArn is required");
        if (string.IsNullOrWhiteSpace(sql))
            return ErrorJson("BadRequestException", "sql is required");
        if (!TryGetOptionalTransactionId(data, out var transactionId, out var transactionError))
            return ErrorJson("BadRequestException", transactionError);
        if (transactionId is not null)
            return ErrorJson("BadRequestException", "BatchExecuteStatement does not support transactionId");
        if (!TryGetDatabase(resourceArn, out var database, out var notFound))
            return notFound!;
        if (!data.TryGetProperty("parameterSets", out var parameterSets) ||
            parameterSets.ValueKind != JsonValueKind.Array ||
            parameterSets.GetArrayLength() == 0)
        {
            return ErrorJson("BadRequestException", "parameterSets must contain at least one parameter set");
        }

        var batchTransactionId = $"batch-{Guid.NewGuid():N}";
        try
        {
            database!.BeginTransaction(batchTransactionId);

            var updateResults = new List<object?>();
            foreach (var parameterSet in parameterSets.EnumerateArray())
            {
                if (!TryParseParameterArray(parameterSet, out var parameters, out var parameterError))
                {
                    database.CompleteTransaction(batchTransactionId, commit: false);
                    return ErrorJson("BadRequestException", parameterError);
                }

                var result = ExecuteSql(database, sql, parameters, batchTransactionId, includeMetadata: false);
                if (result.HasRows)
                {
                    database.CompleteTransaction(batchTransactionId, commit: false);
                    return ErrorJson("BadRequestException", "BatchExecute supports statements that do not return rows");
                }

                updateResults.Add(new Dictionary<string, object?>
                {
                    ["numberOfRecordsUpdated"] = result.NumberOfRecordsUpdated,
                });
            }

            if (!database.CompleteTransaction(batchTransactionId, commit: true))
                return ErrorJson("NotFoundException", $"Transaction {batchTransactionId} not found", 404);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["updateResults"] = updateResults,
            });
        }
        catch (SqliteException exception)
        {
            database!.CompleteTransaction(batchTransactionId, commit: false);
            return ErrorJson("BadRequestException", exception.Message);
        }
        catch (ObjectDisposedException)
        {
            return ErrorJson("BadRequestException", $"Database cluster not found for ARN: {resourceArn}");
        }
    }

    private static StatementResult ExecuteSql(
        RdsClusterDatabase database,
        string sql,
        IReadOnlyList<SqlParameterValue> parameters,
        string? transactionId,
        bool includeMetadata)
    {
        return database.Execute(transactionId, (connection, transaction) =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.Transaction = transaction;
            foreach (var parameter in parameters)
                command.Parameters.AddWithValue($":{parameter.Name}", parameter.Value);

            using var reader = command.ExecuteReader();
            var fieldCount = reader.FieldCount;
            var records = new List<object?>();
            while (reader.Read())
            {
                var row = new List<object?>(fieldCount);
                for (var index = 0; index < fieldCount; index++)
                    row.Add(ToField(reader.GetValue(index)));
                records.Add(row);
            }

            var recordsUpdated = Math.Max(reader.RecordsAffected, 0);
            var response = new Dictionary<string, object?>
            {
                ["numberOfRecordsUpdated"] = recordsUpdated,
            };
            if (fieldCount > 0)
            {
                response["records"] = records;
                if (includeMetadata)
                {
                    response["columnMetadata"] = Enumerable.Range(0, fieldCount)
                        .Select(index => (object?)new Dictionary<string, object?>
                        {
                            ["name"] = reader.GetName(index),
                            ["typeName"] = reader.GetDataTypeName(index),
                        })
                        .ToList();
                }
            }

            return new StatementResult(response, recordsUpdated, fieldCount > 0);
        });
    }

    private static object ToField(object value) =>
        value switch
        {
            DBNull => new Dictionary<string, object?> { ["isNull"] = true },
            string text => new Dictionary<string, object?> { ["stringValue"] = text },
            byte[] blob => new Dictionary<string, object?> { ["blobValue"] = Convert.ToBase64String(blob) },
            bool boolean => new Dictionary<string, object?> { ["booleanValue"] = boolean },
            byte number => new Dictionary<string, object?> { ["longValue"] = (long)number },
            short number => new Dictionary<string, object?> { ["longValue"] = (long)number },
            int number => new Dictionary<string, object?> { ["longValue"] = (long)number },
            long number => new Dictionary<string, object?> { ["longValue"] = number },
            float number => new Dictionary<string, object?> { ["doubleValue"] = (double)number },
            double number => new Dictionary<string, object?> { ["doubleValue"] = number },
            decimal number => new Dictionary<string, object?> { ["doubleValue"] = (double)number },
            _ => new Dictionary<string, object?>
            {
                ["stringValue"] = Convert.ToString(value, CultureInfo.InvariantCulture) ?? "",
            },
        };

    private static bool TryParseParameters(
        JsonElement data,
        out List<SqlParameterValue> parameters,
        out string error)
    {
        if (!data.TryGetProperty("parameters", out var parameterArray))
        {
            parameters = [];
            error = "";
            return true;
        }

        return TryParseParameterArray(parameterArray, out parameters, out error);
    }

    private static bool TryParseParameterArray(
        JsonElement parameterArray,
        out List<SqlParameterValue> parameters,
        out string error)
    {
        parameters = [];
        if (parameterArray.ValueKind != JsonValueKind.Array)
        {
            error = "parameters must be an array";
            return false;
        }

        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var parameter in parameterArray.EnumerateArray())
        {
            if (parameter.ValueKind != JsonValueKind.Object)
            {
                error = "Each parameter must be an object";
                return false;
            }

            var name = GetString(parameter, "name");
            if (string.IsNullOrWhiteSpace(name))
            {
                error = "Each parameter requires a name";
                return false;
            }
            if (name.Any(character => !char.IsLetterOrDigit(character) && character != '_'))
            {
                error = $"Parameter name {name} contains unsupported characters";
                return false;
            }
            if (!names.Add(name))
            {
                error = $"Parameter name {name} is repeated";
                return false;
            }
            if (!parameter.TryGetProperty("value", out var value) || value.ValueKind != JsonValueKind.Object)
            {
                error = $"Parameter {name} requires a value object";
                return false;
            }

            var valueProperties = value.EnumerateObject().ToArray();
            if (valueProperties.Length != 1)
            {
                error = $"Parameter {name} must specify exactly one supported value";
                return false;
            }

            var valueProperty = valueProperties[0];
            object? parsedValue = valueProperty.Name switch
            {
                "stringValue" when valueProperty.Value.ValueKind == JsonValueKind.String =>
                    valueProperty.Value.GetString(),
                "longValue" when valueProperty.Value.ValueKind == JsonValueKind.Number &&
                    valueProperty.Value.TryGetInt64(out var number) => number,
                "doubleValue" when valueProperty.Value.ValueKind == JsonValueKind.Number =>
                    valueProperty.Value.GetDouble(),
                "booleanValue" when valueProperty.Value.ValueKind is JsonValueKind.True or JsonValueKind.False =>
                    valueProperty.Value.GetBoolean(),
                "isNull" when valueProperty.Value.ValueKind == JsonValueKind.True =>
                    DBNull.Value,
                "blobValue" when valueProperty.Value.ValueKind == JsonValueKind.String =>
                    DecodeBlob(valueProperty.Value.GetString()),
                _ => UnsupportedParameterValue(valueProperty.Name),
            };

            if (parsedValue is UnsupportedValue unsupported)
            {
                error = $"Parameter {name} has unsupported value field {unsupported.Name}";
                return false;
            }

            parameters.Add(new SqlParameterValue(name, parsedValue ?? DBNull.Value));
        }

        error = "";
        return true;
    }

    private static object DecodeBlob(string? value)
    {
        try
        {
            return Convert.FromBase64String(value ?? "");
        }
        catch (FormatException)
        {
            return new UnsupportedValue("blobValue (expected base64)");
        }
    }

    private static bool TryGetOptionalTransactionId(
        JsonElement data,
        out string? transactionId,
        out string error)
    {
        transactionId = null;
        if (!data.TryGetProperty("transactionId", out var value))
        {
            error = "";
            return true;
        }

        if (value.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(value.GetString()))
        {
            error = "transactionId must be a non-empty string";
            return false;
        }

        transactionId = value.GetString();
        error = "";
        return true;
    }

    private bool TryGetDatabase(
        string resourceArn,
        out RdsClusterDatabase? database,
        out ServiceResponse? error)
    {
        var arnParts = resourceArn.Split(':');
        if (arnParts.Length != 7 ||
            !string.Equals(arnParts[0], "arn", StringComparison.Ordinal) ||
            !string.Equals(arnParts[2], "rds", StringComparison.Ordinal) ||
            !string.Equals(arnParts[4], AccountContext.GetAccountId(), StringComparison.Ordinal))
        {
            database = null;
            error = ErrorJson("BadRequestException", $"Database cluster not found for ARN: {resourceArn}");
            return false;
        }

        if (_databaseStore.TryGet(resourceArn, out database) && database is not null)
        {
            error = null;
            return true;
        }

        error = ErrorJson("BadRequestException", $"Database cluster not found for ARN: {resourceArn}");
        return false;
    }

    private bool TryGetTransaction(
        string transactionId,
        string resourceArn,
        RdsClusterDatabase database,
        out ServiceResponse? error)
    {
        if (_transactions.TryGetValue(transactionId, out var transaction) &&
            string.Equals(transaction.ResourceArn, resourceArn, StringComparison.Ordinal) &&
            ReferenceEquals(transaction.Database, database))
        {
            error = null;
            return true;
        }

        error = ErrorJson("NotFoundException", $"Transaction {transactionId} not found", 404);
        return false;
    }

    private void RemoveTransactionsForCluster(string resourceArn)
    {
        lock (_lock)
        {
            foreach (var transactionId in _transactions
                .Where(entry => string.Equals(entry.Value.ResourceArn, resourceArn, StringComparison.Ordinal))
                .Select(entry => entry.Key)
                .ToArray())
            {
                _transactions.Remove(transactionId);
            }
        }
    }

    private static object? UnsupportedParameterValue(string name) => new UnsupportedValue(name);

    private static string GetString(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString() ?? ""
            : "";

    private static ServiceResponse ErrorJson(string code, string message, int status = 400) =>
        AwsResponseHelpers.ErrorResponseJson(code, message, status);

    private sealed record SqlParameterValue(string Name, object Value);
    private sealed record StatementResult(Dictionary<string, object?> Response, int NumberOfRecordsUpdated, bool HasRows);
    private sealed record TransactionInfo(string ResourceArn, RdsClusterDatabase Database);
    private sealed record UnsupportedValue(string Name);
}
