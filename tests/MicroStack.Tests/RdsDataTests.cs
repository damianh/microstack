using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Task = System.Threading.Tasks.Task;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the RDS Data API service handler.
/// Uses raw HTTP requests since rds-data uses REST paths (/Execute, etc.)
/// and the stub returns error responses for non-existent clusters.
///
/// Mirrors coverage from ministack/tests/test_rds_data.py.
/// </summary>
public sealed class RdsDataTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private const string Region = "us-east-1";
    private const string AccountId = "000000000000";
    private static readonly string FakeClusterArn = $"arn:aws:rds:{Region}:{AccountId}:cluster:nonexistent-cluster";
    private static readonly string FakeSecretArn = $"arn:aws:secretsmanager:{Region}:{AccountId}:secret:nonexistent-secret";

    private readonly MicroStackFixture _fixture;

    public RdsDataTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync() => Task.CompletedTask;

    private async Task<(HttpStatusCode StatusCode, JsonElement Body)> RawPost(string path, object body)
    {
        var json = JsonSerializer.Serialize(body);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        var response = await _fixture.HttpClient.PostAsync(path, content);
        var responseBody = await response.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(responseBody);
        return (response.StatusCode, doc.RootElement.Clone());
    }

    private async Task<(HttpStatusCode StatusCode, JsonElement Body)> RawPostRaw(string path, string rawBody)
    {
        using var content = new StringContent(rawBody, Encoding.UTF8, "application/json");
        var response = await _fixture.HttpClient.PostAsync(path, content);
        var responseBody = await response.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(responseBody);
        return (response.StatusCode, doc.RootElement.Clone());
    }

    // ── Routing tests ─────────────────────────────────────────────────────────

    [Fact]
    public async Task ExecuteRouteExists()
    {
        var (status, body) = await RawPost("/Execute", new { });
        Assert.Equal(HttpStatusCode.BadRequest, status);
        var message = GetMessage(body);
        Assert.Contains("resourceArn", message);
    }

    [Fact]
    public async Task BeginTransactionRouteExists()
    {
        var (status, _) = await RawPost("/BeginTransaction", new { });
        Assert.Equal(HttpStatusCode.BadRequest, status);
    }

    [Fact]
    public async Task CommitTransactionRouteExists()
    {
        var (status, _) = await RawPost("/CommitTransaction", new { });
        Assert.Equal(HttpStatusCode.BadRequest, status);
    }

    [Fact]
    public async Task RollbackTransactionRouteExists()
    {
        var (status, _) = await RawPost("/RollbackTransaction", new { });
        Assert.Equal(HttpStatusCode.BadRequest, status);
    }

    [Fact]
    public async Task BatchExecuteRouteExists()
    {
        var (status, _) = await RawPost("/BatchExecute", new { });
        Assert.Equal(HttpStatusCode.BadRequest, status);
    }

    // ── Parameter validation ──────────────────────────────────────────────────

    [Fact]
    public async Task ExecuteMissingResourceArn()
    {
        var (status, body) = await RawPost("/Execute", new
        {
            secretArn = FakeSecretArn,
            sql = "SELECT 1",
        });
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("resourceArn", GetMessage(body));
    }

    [Fact]
    public async Task ExecuteMissingSecretArn()
    {
        var (status, body) = await RawPost("/Execute", new
        {
            resourceArn = FakeClusterArn,
            sql = "SELECT 1",
        });
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("secretArn", GetMessage(body));
    }

    [Fact]
    public async Task ExecuteMissingSql()
    {
        var (status, body) = await RawPost("/Execute", new
        {
            resourceArn = FakeClusterArn,
            secretArn = FakeSecretArn,
        });
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("sql", GetMessage(body));
    }

    [Fact]
    public async Task BatchExecuteMissingSql()
    {
        var (status, body) = await RawPost("/BatchExecute", new
        {
            resourceArn = FakeClusterArn,
            secretArn = FakeSecretArn,
        });
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("sql", GetMessage(body));
    }

    // ── Invalid ARN ───────────────────────────────────────────────────────────

    [Fact]
    public async Task ExecuteNonexistentCluster()
    {
        var (status, body) = await RawPost("/Execute", new
        {
            resourceArn = FakeClusterArn,
            secretArn = FakeSecretArn,
            sql = "SELECT 1",
        });
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("not found", GetMessage(body), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task BeginTransactionNonexistentCluster()
    {
        var (status, body) = await RawPost("/BeginTransaction", new
        {
            resourceArn = FakeClusterArn,
            secretArn = FakeSecretArn,
        });
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("not found", GetMessage(body), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task BatchExecuteNonexistentCluster()
    {
        var (status, body) = await RawPost("/BatchExecute", new
        {
            resourceArn = FakeClusterArn,
            secretArn = FakeSecretArn,
            sql = "INSERT INTO t VALUES (1)",
        });
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("not found", GetMessage(body), StringComparison.OrdinalIgnoreCase);
    }

    // ── Transaction lifecycle (error paths) ───────────────────────────────────

    [Fact]
    public async Task CommitMissingTransactionId()
    {
        var (status, body) = await RawPost("/CommitTransaction", new { });
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("transactionId", GetMessage(body));
    }

    [Fact]
    public async Task RollbackMissingTransactionId()
    {
        var (status, body) = await RawPost("/RollbackTransaction", new { });
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("transactionId", GetMessage(body));
    }

    [Fact]
    public async Task CommitNonexistentTransaction()
    {
        var (status, body) = await RawPost("/CommitTransaction", new
        {
            transactionId = "nonexistent-txn-id",
        });
        Assert.Equal(HttpStatusCode.NotFound, status);
        Assert.Contains("not found", GetMessage(body), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task RollbackNonexistentTransaction()
    {
        var (status, body) = await RawPost("/RollbackTransaction", new
        {
            transactionId = "nonexistent-txn-id",
        });
        Assert.Equal(HttpStatusCode.NotFound, status);
        Assert.Contains("not found", GetMessage(body), StringComparison.OrdinalIgnoreCase);
    }

    // ── Invalid JSON ──────────────────────────────────────────────────────────

    [Fact]
    public async Task ExecuteInvalidJson()
    {
        var (status, body) = await RawPostRaw("/Execute", "not-json{{{");
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("Invalid JSON", GetMessage(body));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static string GetMessage(JsonElement body)
    {
        if (body.TryGetProperty("message", out var msg))
            return msg.GetString() ?? "";
        if (body.TryGetProperty("Message", out var msg2))
            return msg2.GetString() ?? "";
        return "";
    }
}
