using Amazon;
using Amazon.RDS;
using Amazon.RDS.Model;
using Amazon.Runtime;
using System.Text;
using Task = System.Threading.Tasks.Task;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the RDS Data API service handler.
/// Provisions clusters through the RDS API and uses raw HTTP requests for
/// rds-data's REST paths (/Execute, etc.).
///
/// Mirrors coverage from ministack/tests/test_rds_data.py.
/// </summary>
public sealed class RdsDataTests(MicroStackFixture fixture) : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private const string Region = "us-east-1";
    private const string AccountId = "000000000000";
    private const string FakeClusterArn = $"arn:aws:rds:{Region}:{AccountId}:cluster:nonexistent-cluster";
    private const string FakeSecretArn = $"arn:aws:secretsmanager:{Region}:{AccountId}:secret:nonexistent-secret";

    public async ValueTask InitializeAsync()
    {
        await fixture.HttpClient.PostAsync("/_microstack/reset", null);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private async Task<(HttpStatusCode StatusCode, JsonElement Body)> RawPost(string path, object body)
    {
        var json = JsonSerializer.Serialize(body);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        var response = await fixture.HttpClient.PostAsync(path, content);
        var responseBody = await response.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(responseBody);
        return (response.StatusCode, doc.RootElement.Clone());
    }

    private async Task<(HttpStatusCode StatusCode, JsonElement Body)> RawPostRaw(string path, string rawBody)
    {
        using var content = new StringContent(rawBody, Encoding.UTF8, "application/json");
        var response = await fixture.HttpClient.PostAsync(path, content);
        var responseBody = await response.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(responseBody);
        return (response.StatusCode, doc.RootElement.Clone());
    }

    private async Task<string> CreateEmulatedCluster(string clusterIdentifier)
    {
        var (rdsClient, sdkHttpClient) = CreateRdsClient();
        using (sdkHttpClient)
        using (rdsClient)
        {
            var response = await rdsClient.CreateDBClusterAsync(new CreateDBClusterRequest
            {
                DBClusterIdentifier = clusterIdentifier,
                Engine = "aurora-postgresql",
                MasterUsername = "admin",
                MasterUserPassword = "password123",
            });
            return response.DBCluster.DBClusterArn;
        }
    }

    private (AmazonRDSClient Client, HttpClient HttpClient) CreateRdsClient()
    {
        var sdkHttpClient = new HttpClient(new CanonicalizeUriHandler(fixture.CreateHandler()))
        {
            BaseAddress = new Uri("http://localhost/"),
        };
        var config = new AmazonRDSConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(sdkHttpClient),
        };
        return (new AmazonRDSClient(new BasicAWSCredentials("test", "test"), config), sdkHttpClient);
    }

    private Task<(HttpStatusCode StatusCode, JsonElement Body)> ExecuteStatement(
        string resourceArn,
        string sql,
        object[]? parameters = null,
        string? transactionId = null)
    {
        var request = new Dictionary<string, object?>
        {
            ["resourceArn"] = resourceArn,
            ["secretArn"] = FakeSecretArn,
            ["sql"] = sql,
        };
        if (parameters is not null)
            request["parameters"] = parameters;
        if (transactionId is not null)
            request["transactionId"] = transactionId;
        return RawPost("/Execute", request);
    }

    // ── Routing tests ─────────────────────────────────────────────────────────

    [Fact]
    public async Task ExecuteRouteExists()
    {
        var (status, body) = await RawPost("/Execute", new { });
        status.ShouldBe(HttpStatusCode.BadRequest);
        var message = GetMessage(body);
        message.ShouldContain("resourceArn");
    }

    [Fact]
    public async Task BeginTransactionRouteExists()
    {
        var (status, _) = await RawPost("/BeginTransaction", new { });
        status.ShouldBe(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task CommitTransactionRouteExists()
    {
        var (status, _) = await RawPost("/CommitTransaction", new { });
        status.ShouldBe(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task RollbackTransactionRouteExists()
    {
        var (status, _) = await RawPost("/RollbackTransaction", new { });
        status.ShouldBe(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task BatchExecuteRouteExists()
    {
        var (status, _) = await RawPost("/BatchExecute", new { });
        status.ShouldBe(HttpStatusCode.BadRequest);
    }

    // ── Executing SQL against an emulated cluster ────────────────────────────

    [Fact]
    public async Task ExecuteStatement_ReturnsRowsAndAffectedRowCount()
    {
        var resourceArn = await CreateEmulatedCluster("rds-data-execute");

        var (createStatus, _) = await ExecuteStatement(
            resourceArn,
            "CREATE TABLE ledger (entry_id INTEGER PRIMARY KEY, label TEXT NOT NULL, quantity INTEGER NOT NULL)");
        createStatus.ShouldBe(HttpStatusCode.OK);

        var (insertStatus, insertBody) = await ExecuteStatement(
            resourceArn,
            "INSERT INTO ledger (entry_id, label, quantity) VALUES (1, 'sample', 3)");
        insertStatus.ShouldBe(HttpStatusCode.OK);
        insertBody.GetProperty("numberOfRecordsUpdated").GetInt32().ShouldBe(1);

        var (selectStatus, selectBody) = await ExecuteStatement(
            resourceArn,
            "SELECT label, quantity FROM ledger WHERE entry_id = 1");
        selectStatus.ShouldBe(HttpStatusCode.OK);
        var records = selectBody.GetProperty("records");
        records.GetArrayLength().ShouldBe(1);
        records[0][0].GetProperty("stringValue").GetString().ShouldBe("sample");
        records[0][1].GetProperty("longValue").GetInt64().ShouldBe(3);
    }

    [Fact]
    public async Task ExecuteStatement_BindsNamedParameters()
    {
        var resourceArn = await CreateEmulatedCluster("rds-data-parameters");

        var (createStatus, _) = await ExecuteStatement(
            resourceArn,
            "CREATE TABLE parameterized_entries (entry_id INTEGER PRIMARY KEY, label TEXT NOT NULL, quantity INTEGER NOT NULL)");
        createStatus.ShouldBe(HttpStatusCode.OK);

        // The quote and placeholder-looking text must be treated as data, not SQL syntax.
        const string label = "O'Reilly :quantity";
        var (insertStatus, insertBody) = await ExecuteStatement(
            resourceArn,
            "INSERT INTO parameterized_entries (entry_id, label, quantity) VALUES (1, :label, :quantity)",
            parameters:
            [
                new { name = "label", value = new { stringValue = label } },
                new { name = "quantity", value = new { longValue = 7L } },
            ]);
        insertStatus.ShouldBe(HttpStatusCode.OK);
        insertBody.GetProperty("numberOfRecordsUpdated").GetInt32().ShouldBe(1);

        var (selectStatus, selectBody) = await ExecuteStatement(
            resourceArn,
            "SELECT label, quantity FROM parameterized_entries WHERE label = :label",
            parameters: [new { name = "label", value = new { stringValue = label } }]);
        selectStatus.ShouldBe(HttpStatusCode.OK);
        var records = selectBody.GetProperty("records");
        records.GetArrayLength().ShouldBe(1);
        records[0][0].GetProperty("stringValue").GetString().ShouldBe(label);
        records[0][1].GetProperty("longValue").GetInt64().ShouldBe(7);
    }

    [Fact]
    public async Task ExecuteStatement_MapsSupportedParameterAndResultTypes()
    {
        var resourceArn = await CreateEmulatedCluster("rds-data-parameter-types");
        var blob = Convert.ToBase64String([1, 2, 3]);

        var (status, body) = await ExecuteStatement(
            resourceArn,
            "SELECT :realValue, :booleanValue, :nullValue, :blobValue",
            parameters:
            [
                new { name = "realValue", value = new { doubleValue = 1.5 } },
                new { name = "booleanValue", value = new { booleanValue = true } },
                new { name = "nullValue", value = new { isNull = true } },
                new { name = "blobValue", value = new { blobValue = blob } },
            ]);

        status.ShouldBe(HttpStatusCode.OK);
        var row = body.GetProperty("records")[0];
        row[0].GetProperty("doubleValue").GetDouble().ShouldBe(1.5);
        row[1].GetProperty("longValue").GetInt64().ShouldBe(1);
        row[2].GetProperty("isNull").GetBoolean().ShouldBeTrue();
        row[3].GetProperty("blobValue").GetString().ShouldBe(blob);
    }

    [Fact]
    public async Task ExecuteStatement_InvalidSqlReturnsBadRequest()
    {
        var resourceArn = await CreateEmulatedCluster("rds-data-invalid-sql");

        var (status, body) = await ExecuteStatement(resourceArn, "SELEC 1");

        status.ShouldBe(HttpStatusCode.BadRequest);
        GetMessage(body).ShouldContain("syntax", Case.Insensitive);
    }

    [Fact]
    public async Task ExecuteStatement_UnsupportedParameterValueReturnsBadRequest()
    {
        var resourceArn = await CreateEmulatedCluster("rds-data-unsupported-parameter");

        var (status, body) = await ExecuteStatement(
            resourceArn,
            "SELECT :value",
            parameters:
            [
                new
                {
                    name = "value",
                    value = new { arrayValue = new { stringValues = new[] { "unsupported" } } },
                },
            ]);

        status.ShouldBe(HttpStatusCode.BadRequest);
        GetMessage(body).ShouldContain("unsupported value field");
    }

    [Fact]
    public async Task BatchExecuteStatement_AppliesEachParameterSet()
    {
        var resourceArn = await CreateEmulatedCluster("rds-data-batch");

        var (createStatus, _) = await ExecuteStatement(
            resourceArn,
            "CREATE TABLE batch_entries (entry_id INTEGER PRIMARY KEY, label TEXT NOT NULL)");
        createStatus.ShouldBe(HttpStatusCode.OK);

        var (batchStatus, batchBody) = await RawPost("/BatchExecute", new
        {
            resourceArn,
            secretArn = FakeSecretArn,
            sql = "INSERT INTO batch_entries (entry_id, label) VALUES (:entry_id, :label)",
            parameterSets = new[]
            {
                new object[]
                {
                    new { name = "entry_id", value = new { longValue = 1L } },
                    new { name = "label", value = new { stringValue = "first" } },
                },
                new object[]
                {
                    new { name = "entry_id", value = new { longValue = 2L } },
                    new { name = "label", value = new { stringValue = "second" } },
                },
            },
        });
        batchStatus.ShouldBe(HttpStatusCode.OK);
        batchBody.GetProperty("updateResults").GetArrayLength().ShouldBe(2);

        var (selectStatus, selectBody) = await ExecuteStatement(
            resourceArn,
            "SELECT entry_id, label FROM batch_entries ORDER BY entry_id");
        selectStatus.ShouldBe(HttpStatusCode.OK);
        var records = selectBody.GetProperty("records");
        records.GetArrayLength().ShouldBe(2);
        records[0][0].GetProperty("longValue").GetInt64().ShouldBe(1);
        records[0][1].GetProperty("stringValue").GetString().ShouldBe("first");
        records[1][0].GetProperty("longValue").GetInt64().ShouldBe(2);
        records[1][1].GetProperty("stringValue").GetString().ShouldBe("second");
    }

    [Fact]
    public async Task BatchExecuteStatement_RollsBackAllSetsWhenOneFails()
    {
        var resourceArn = await CreateEmulatedCluster("rds-data-batch-rollback");

        var (createStatus, _) = await ExecuteStatement(
            resourceArn,
            "CREATE TABLE unique_entries (entry_id INTEGER PRIMARY KEY, label TEXT NOT NULL)");
        createStatus.ShouldBe(HttpStatusCode.OK);

        var (batchStatus, batchBody) = await RawPost("/BatchExecute", new
        {
            resourceArn,
            secretArn = FakeSecretArn,
            sql = "INSERT INTO unique_entries (entry_id, label) VALUES (:entry_id, :label)",
            parameterSets = new[]
            {
                new object[]
                {
                    new { name = "entry_id", value = new { longValue = 1L } },
                    new { name = "label", value = new { stringValue = "first" } },
                },
                new object[]
                {
                    new { name = "entry_id", value = new { longValue = 1L } },
                    new { name = "label", value = new { stringValue = "duplicate" } },
                },
            },
        });
        batchStatus.ShouldBe(HttpStatusCode.BadRequest);
        GetMessage(batchBody).ShouldContain("UNIQUE", Case.Insensitive);

        var (selectStatus, selectBody) = await ExecuteStatement(
            resourceArn,
            "SELECT entry_id, label FROM unique_entries");
        selectStatus.ShouldBe(HttpStatusCode.OK);
        selectBody.GetProperty("records").GetArrayLength().ShouldBe(0);
    }

    [Fact]
    public async Task CommitTransaction_PersistsExecutedWrite()
    {
        var resourceArn = await CreateEmulatedCluster("rds-data-commit");
        var (createStatus, _) = await ExecuteStatement(
            resourceArn,
            "CREATE TABLE transaction_entries (entry_id INTEGER PRIMARY KEY, label TEXT NOT NULL)");
        createStatus.ShouldBe(HttpStatusCode.OK);

        var (beginStatus, beginBody) = await RawPost("/BeginTransaction", new
        {
            resourceArn,
            secretArn = FakeSecretArn,
        });
        beginStatus.ShouldBe(HttpStatusCode.OK);
        var transactionId = beginBody.GetProperty("transactionId").GetString();
        transactionId.ShouldNotBeNullOrWhiteSpace();

        var (insertStatus, insertBody) = await ExecuteStatement(
            resourceArn,
            "INSERT INTO transaction_entries (entry_id, label) VALUES (1, 'committed')",
            transactionId: transactionId);
        insertStatus.ShouldBe(HttpStatusCode.OK);
        insertBody.GetProperty("numberOfRecordsUpdated").GetInt32().ShouldBe(1);

        var (commitStatus, commitBody) = await RawPost("/CommitTransaction", new { transactionId });
        commitStatus.ShouldBe(HttpStatusCode.OK);
        commitBody.GetProperty("transactionStatus").GetString().ShouldBe("Transaction Committed");

        var (selectStatus, selectBody) = await ExecuteStatement(
            resourceArn,
            "SELECT label FROM transaction_entries WHERE entry_id = 1");
        selectStatus.ShouldBe(HttpStatusCode.OK);
        var records = selectBody.GetProperty("records");
        records.GetArrayLength().ShouldBe(1);
        records[0][0].GetProperty("stringValue").GetString().ShouldBe("committed");
    }

    [Fact]
    public async Task ExecuteStatement_RejectsTransactionFromAnotherCluster()
    {
        var transactionClusterArn = await CreateEmulatedCluster("rds-data-transaction-cluster");
        var otherClusterArn = await CreateEmulatedCluster("rds-data-other-cluster");
        var (beginStatus, beginBody) = await RawPost("/BeginTransaction", new
        {
            resourceArn = transactionClusterArn,
            secretArn = FakeSecretArn,
        });
        beginStatus.ShouldBe(HttpStatusCode.OK);
        var transactionId = beginBody.GetProperty("transactionId").GetString();
        transactionId.ShouldNotBeNullOrWhiteSpace();

        var (status, body) = await ExecuteStatement(
            otherClusterArn,
            "SELECT 1",
            transactionId: transactionId);

        status.ShouldBe(HttpStatusCode.NotFound);
        GetMessage(body).ShouldContain("not found", Case.Insensitive);
    }

    [Fact]
    public async Task RollbackTransaction_DiscardsExecutedWrite()
    {
        var resourceArn = await CreateEmulatedCluster("rds-data-rollback");
        var (createStatus, _) = await ExecuteStatement(
            resourceArn,
            "CREATE TABLE transaction_entries (entry_id INTEGER PRIMARY KEY, label TEXT NOT NULL)");
        createStatus.ShouldBe(HttpStatusCode.OK);

        var (beginStatus, beginBody) = await RawPost("/BeginTransaction", new
        {
            resourceArn,
            secretArn = FakeSecretArn,
        });
        beginStatus.ShouldBe(HttpStatusCode.OK);
        var transactionId = beginBody.GetProperty("transactionId").GetString();
        transactionId.ShouldNotBeNullOrWhiteSpace();

        var (insertStatus, insertBody) = await ExecuteStatement(
            resourceArn,
            "INSERT INTO transaction_entries (entry_id, label) VALUES (1, 'rolled back')",
            transactionId: transactionId);
        insertStatus.ShouldBe(HttpStatusCode.OK);
        insertBody.GetProperty("numberOfRecordsUpdated").GetInt32().ShouldBe(1);

        var (rollbackStatus, rollbackBody) = await RawPost("/RollbackTransaction", new { transactionId });
        rollbackStatus.ShouldBe(HttpStatusCode.OK);
        rollbackBody.GetProperty("transactionStatus").GetString().ShouldBe("Transaction Rolled Back");

        var (selectStatus, selectBody) = await ExecuteStatement(
            resourceArn,
            "SELECT label FROM transaction_entries WHERE entry_id = 1");
        selectStatus.ShouldBe(HttpStatusCode.OK);
        selectBody.GetProperty("records").GetArrayLength().ShouldBe(0);
    }

    [Fact]
    public async Task ClusterDatabases_AreIsolatedPerCluster()
    {
        var firstClusterArn = await CreateEmulatedCluster("rds-data-isolated-first");
        var secondClusterArn = await CreateEmulatedCluster("rds-data-isolated-second");

        var (firstCreateStatus, _) = await ExecuteStatement(
            firstClusterArn,
            "CREATE TABLE cluster_entries (entry_id INTEGER PRIMARY KEY, label TEXT NOT NULL)");
        firstCreateStatus.ShouldBe(HttpStatusCode.OK);
        var (secondCreateStatus, _) = await ExecuteStatement(
            secondClusterArn,
            "CREATE TABLE cluster_entries (entry_id INTEGER PRIMARY KEY, label TEXT NOT NULL)");
        secondCreateStatus.ShouldBe(HttpStatusCode.OK);

        var (insertStatus, insertBody) = await ExecuteStatement(
            firstClusterArn,
            "INSERT INTO cluster_entries (entry_id, label) VALUES (1, 'first cluster')");
        insertStatus.ShouldBe(HttpStatusCode.OK);
        insertBody.GetProperty("numberOfRecordsUpdated").GetInt32().ShouldBe(1);

        var (firstSelectStatus, firstSelectBody) = await ExecuteStatement(
            firstClusterArn,
            "SELECT label FROM cluster_entries ORDER BY entry_id");
        firstSelectStatus.ShouldBe(HttpStatusCode.OK);
        var firstRecords = firstSelectBody.GetProperty("records");
        firstRecords.GetArrayLength().ShouldBe(1);
        firstRecords[0][0].GetProperty("stringValue").GetString().ShouldBe("first cluster");

        var (secondSelectStatus, secondSelectBody) = await ExecuteStatement(
            secondClusterArn,
            "SELECT label FROM cluster_entries ORDER BY entry_id");
        secondSelectStatus.ShouldBe(HttpStatusCode.OK);
        secondSelectBody.GetProperty("records").GetArrayLength().ShouldBe(0);
    }

    [Fact]
    public async Task Reset_ClearsClusterDatabase()
    {
        const string clusterIdentifier = "rds-data-reset";
        var resourceArn = await CreateEmulatedCluster(clusterIdentifier);
        var (createStatus, _) = await ExecuteStatement(
            resourceArn,
            "CREATE TABLE reset_entries (entry_id INTEGER PRIMARY KEY, label TEXT NOT NULL)");
        createStatus.ShouldBe(HttpStatusCode.OK);
        var (insertStatus, insertBody) = await ExecuteStatement(
            resourceArn,
            "INSERT INTO reset_entries (entry_id, label) VALUES (1, 'before reset')");
        insertStatus.ShouldBe(HttpStatusCode.OK);
        insertBody.GetProperty("numberOfRecordsUpdated").GetInt32().ShouldBe(1);

        var resetResponse = await fixture.HttpClient.PostAsync("/_microstack/reset", null);
        resetResponse.StatusCode.ShouldBe(HttpStatusCode.OK);

        var recreatedArn = await CreateEmulatedCluster(clusterIdentifier);
        var (recreateTableStatus, _) = await ExecuteStatement(
            recreatedArn,
            "CREATE TABLE IF NOT EXISTS reset_entries (entry_id INTEGER PRIMARY KEY, label TEXT NOT NULL)");
        recreateTableStatus.ShouldBe(HttpStatusCode.OK);
        var (selectStatus, selectBody) = await ExecuteStatement(
            recreatedArn,
            "SELECT label FROM reset_entries");
        selectStatus.ShouldBe(HttpStatusCode.OK);
        selectBody.GetProperty("records").GetArrayLength().ShouldBe(0);
    }

    [Fact]
    public async Task DeleteCluster_ClearsClusterDatabase()
    {
        const string clusterIdentifier = "rds-data-delete";
        var resourceArn = await CreateEmulatedCluster(clusterIdentifier);
        var (createStatus, _) = await ExecuteStatement(
            resourceArn,
            "CREATE TABLE deleted_entries (entry_id INTEGER PRIMARY KEY, label TEXT NOT NULL)");
        createStatus.ShouldBe(HttpStatusCode.OK);
        var (insertStatus, insertBody) = await ExecuteStatement(
            resourceArn,
            "INSERT INTO deleted_entries (entry_id, label) VALUES (1, 'before delete')");
        insertStatus.ShouldBe(HttpStatusCode.OK);
        insertBody.GetProperty("numberOfRecordsUpdated").GetInt32().ShouldBe(1);

        var (rdsClient, sdkHttpClient) = CreateRdsClient();
        using (sdkHttpClient)
        using (rdsClient)
        {
            await rdsClient.DeleteDBClusterAsync(new DeleteDBClusterRequest
            {
                DBClusterIdentifier = clusterIdentifier,
                SkipFinalSnapshot = true,
            });
        }

        var recreatedArn = await CreateEmulatedCluster(clusterIdentifier);
        var (recreateTableStatus, _) = await ExecuteStatement(
            recreatedArn,
            "CREATE TABLE IF NOT EXISTS deleted_entries (entry_id INTEGER PRIMARY KEY, label TEXT NOT NULL)");
        recreateTableStatus.ShouldBe(HttpStatusCode.OK);
        var (selectStatus, selectBody) = await ExecuteStatement(
            recreatedArn,
            "SELECT label FROM deleted_entries");
        selectStatus.ShouldBe(HttpStatusCode.OK);
        selectBody.GetProperty("records").GetArrayLength().ShouldBe(0);
    }

    [Fact]
    public async Task StopAndStartCluster_PreservesClusterDatabase()
    {
        const string clusterIdentifier = "rds-data-restart";
        var resourceArn = await CreateEmulatedCluster(clusterIdentifier);
        var (createStatus, _) = await ExecuteStatement(
            resourceArn,
            "CREATE TABLE restarted_entries (entry_id INTEGER PRIMARY KEY, label TEXT NOT NULL)");
        createStatus.ShouldBe(HttpStatusCode.OK);
        var (insertStatus, insertBody) = await ExecuteStatement(
            resourceArn,
            "INSERT INTO restarted_entries (entry_id, label) VALUES (1, 'before restart')");
        insertStatus.ShouldBe(HttpStatusCode.OK);
        insertBody.GetProperty("numberOfRecordsUpdated").GetInt32().ShouldBe(1);

        var (rdsClient, sdkHttpClient) = CreateRdsClient();
        using (sdkHttpClient)
        using (rdsClient)
        {
            await rdsClient.StopDBClusterAsync(new StopDBClusterRequest
            {
                DBClusterIdentifier = clusterIdentifier,
            });
            await rdsClient.StartDBClusterAsync(new StartDBClusterRequest
            {
                DBClusterIdentifier = clusterIdentifier,
            });
        }

        var (selectStatus, selectBody) = await ExecuteStatement(
            resourceArn,
            "SELECT label FROM restarted_entries");
        selectStatus.ShouldBe(HttpStatusCode.OK);
        var records = selectBody.GetProperty("records");
        records.GetArrayLength().ShouldBe(1);
        records[0][0].GetProperty("stringValue").GetString().ShouldBe("before restart");
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
        status.ShouldBe(HttpStatusCode.BadRequest);
        GetMessage(body).ShouldContain("resourceArn");
    }

    [Fact]
    public async Task ExecuteMissingSecretArn()
    {
        var (status, body) = await RawPost("/Execute", new
        {
            resourceArn = FakeClusterArn,
            sql = "SELECT 1",
        });
        status.ShouldBe(HttpStatusCode.BadRequest);
        GetMessage(body).ShouldContain("secretArn");
    }

    [Fact]
    public async Task ExecuteMissingSql()
    {
        var (status, body) = await RawPost("/Execute", new
        {
            resourceArn = FakeClusterArn,
            secretArn = FakeSecretArn,
        });
        status.ShouldBe(HttpStatusCode.BadRequest);
        GetMessage(body).ShouldContain("sql");
    }

    [Fact]
    public async Task BatchExecuteMissingSql()
    {
        var (status, body) = await RawPost("/BatchExecute", new
        {
            resourceArn = FakeClusterArn,
            secretArn = FakeSecretArn,
        });
        status.ShouldBe(HttpStatusCode.BadRequest);
        GetMessage(body).ShouldContain("sql");
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
        status.ShouldBe(HttpStatusCode.BadRequest);
        GetMessage(body).ShouldContain("not found", Case.Insensitive);
    }

    [Fact]
    public async Task BeginTransactionNonexistentCluster()
    {
        var (status, body) = await RawPost("/BeginTransaction", new
        {
            resourceArn = FakeClusterArn,
            secretArn = FakeSecretArn,
        });
        status.ShouldBe(HttpStatusCode.BadRequest);
        GetMessage(body).ShouldContain("not found", Case.Insensitive);
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
        status.ShouldBe(HttpStatusCode.BadRequest);
        GetMessage(body).ShouldContain("not found", Case.Insensitive);
    }

    // ── Transaction lifecycle (error paths) ───────────────────────────────────

    [Fact]
    public async Task CommitMissingTransactionId()
    {
        var (status, body) = await RawPost("/CommitTransaction", new { });
        status.ShouldBe(HttpStatusCode.BadRequest);
        GetMessage(body).ShouldContain("transactionId");
    }

    [Fact]
    public async Task RollbackMissingTransactionId()
    {
        var (status, body) = await RawPost("/RollbackTransaction", new { });
        status.ShouldBe(HttpStatusCode.BadRequest);
        GetMessage(body).ShouldContain("transactionId");
    }

    [Fact]
    public async Task CommitNonexistentTransaction()
    {
        var (status, body) = await RawPost("/CommitTransaction", new
        {
            transactionId = "nonexistent-txn-id",
        });
        status.ShouldBe(HttpStatusCode.NotFound);
        GetMessage(body).ShouldContain("not found", Case.Insensitive);
    }

    [Fact]
    public async Task RollbackNonexistentTransaction()
    {
        var (status, body) = await RawPost("/RollbackTransaction", new
        {
            transactionId = "nonexistent-txn-id",
        });
        status.ShouldBe(HttpStatusCode.NotFound);
        GetMessage(body).ShouldContain("not found", Case.Insensitive);
    }

    // ── Invalid JSON ──────────────────────────────────────────────────────────

    [Fact]
    public async Task ExecuteInvalidJson()
    {
        var (status, body) = await RawPostRaw("/Execute", "not-json{{{");
        status.ShouldBe(HttpStatusCode.BadRequest);
        GetMessage(body).ShouldContain("Invalid JSON");
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
