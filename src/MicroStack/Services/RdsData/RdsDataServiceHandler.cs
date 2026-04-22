using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.RdsData;

/// <summary>
/// RDS Data API service handler -- REST/JSON protocol with path-based routing.
///
/// Port of ministack/services/rds_data.py.
///
/// Routes: /Execute, /BeginTransaction, /CommitTransaction,
///         /RollbackTransaction, /BatchExecute.
///
/// This is an in-memory stub that validates parameters and returns
/// appropriate error responses. Real database connectivity is not
/// implemented — the handler returns canned errors when a cluster
/// ARN cannot be resolved.
/// </summary>
internal sealed class RdsDataServiceHandler : IServiceHandler
{
    public string ServiceName => "rds-data";

    private readonly Dictionary<string, TransactionInfo> _transactions = new(StringComparer.Ordinal);
    private readonly Lock _lock = new();

    // ── IServiceHandler ───────────────────────────────────────────────────────

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        JsonElement data;
        if (request.Body.Length > 0)
        {
            try
            {
                using var doc = JsonDocument.Parse(request.Body);
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

        var path = request.Path;
        ServiceResponse response;
        lock (_lock)
        {
            response = path switch
            {
                "/Execute" => ExecuteStatement(data),
                "/BeginTransaction" => BeginTransaction(data),
                "/CommitTransaction" => CommitTransaction(data),
                "/RollbackTransaction" => RollbackTransaction(data),
                "/BatchExecute" => BatchExecuteStatement(data),
                _ => ErrorJson("BadRequestException", $"Unknown RDS Data API path: {path}"),
            };
        }

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _transactions.Clear();
        }
    }

    public JsonElement? GetState() => null;

    public void RestoreState(JsonElement state) { }

    // ── Execute Statement ─────────────────────────────────────────────────────

    private ServiceResponse ExecuteStatement(JsonElement data)
    {
        var resourceArn = GetString(data, "resourceArn");
        var secretArn = GetString(data, "secretArn");
        var sql = GetString(data, "sql");

        if (string.IsNullOrEmpty(resourceArn))
            return ErrorJson("BadRequestException", "resourceArn is required");
        if (string.IsNullOrEmpty(secretArn))
            return ErrorJson("BadRequestException", "secretArn is required");
        if (string.IsNullOrEmpty(sql))
            return ErrorJson("BadRequestException", "sql is required");

        // In the stub implementation, we cannot connect to a real database.
        // Return an error indicating the cluster was not found.
        return ErrorJson("BadRequestException",
            $"Database cluster not found for ARN: {resourceArn}");
    }

    // ── Begin Transaction ─────────────────────────────────────────────────────

    private ServiceResponse BeginTransaction(JsonElement data)
    {
        var resourceArn = GetString(data, "resourceArn");
        var secretArn = GetString(data, "secretArn");

        if (string.IsNullOrEmpty(resourceArn))
            return ErrorJson("BadRequestException", "resourceArn is required");
        if (string.IsNullOrEmpty(secretArn))
            return ErrorJson("BadRequestException", "secretArn is required");

        // In stub mode, return cluster-not-found since there's no real DB.
        return ErrorJson("BadRequestException",
            $"Database cluster not found for ARN: {resourceArn}");
    }

    // ── Commit Transaction ────────────────────────────────────────────────────

    private ServiceResponse CommitTransaction(JsonElement data)
    {
        var txnId = GetString(data, "transactionId");
        if (string.IsNullOrEmpty(txnId))
            return ErrorJson("BadRequestException", "transactionId is required");

        if (!_transactions.Remove(txnId))
            return ErrorJson("NotFoundException", $"Transaction {txnId} not found", 404);

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["transactionStatus"] = "Transaction Committed" });
    }

    // ── Rollback Transaction ──────────────────────────────────────────────────

    private ServiceResponse RollbackTransaction(JsonElement data)
    {
        var txnId = GetString(data, "transactionId");
        if (string.IsNullOrEmpty(txnId))
            return ErrorJson("BadRequestException", "transactionId is required");

        if (!_transactions.Remove(txnId))
            return ErrorJson("NotFoundException", $"Transaction {txnId} not found", 404);

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["transactionStatus"] = "Transaction Rolled Back" });
    }

    // ── Batch Execute Statement ───────────────────────────────────────────────

    private ServiceResponse BatchExecuteStatement(JsonElement data)
    {
        var resourceArn = GetString(data, "resourceArn");
        var secretArn = GetString(data, "secretArn");
        var sql = GetString(data, "sql");

        if (string.IsNullOrEmpty(resourceArn))
            return ErrorJson("BadRequestException", "resourceArn is required");
        if (string.IsNullOrEmpty(secretArn))
            return ErrorJson("BadRequestException", "secretArn is required");
        if (string.IsNullOrEmpty(sql))
            return ErrorJson("BadRequestException", "sql is required");

        return ErrorJson("BadRequestException",
            $"Database cluster not found for ARN: {resourceArn}");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static string GetString(JsonElement el, string prop)
    {
        return el.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.String
            ? v.GetString() ?? ""
            : "";
    }

    private static ServiceResponse ErrorJson(string code, string message, int status = 400)
    {
        return AwsResponseHelpers.ErrorResponseJson(code, message, status);
    }

    // ── Internal types ────────────────────────────────────────────────────────

    private sealed class TransactionInfo
    {
        internal string ResourceArn { get; init; } = "";
        internal string Database { get; init; } = "";
    }
}
