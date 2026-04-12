using Amazon;
using Amazon.Athena;
using Amazon.Athena.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the Athena service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_athena.py.
/// </summary>
public sealed class AthenaTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonAthenaClient _athena;

    public AthenaTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _athena = CreateClient(fixture);
    }

    private static AmazonAthenaClient CreateClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonAthenaConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonAthenaClient(new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _athena.Dispose();
        return Task.CompletedTask;
    }

    // -- Query Execution -------------------------------------------------------

    [Fact]
    public async Task StartQueryExecutionAndGetResults()
    {
        var resp = await _athena.StartQueryExecutionAsync(new StartQueryExecutionRequest
        {
            QueryString = "SELECT 1 AS num, 'hello' AS greeting",
            QueryExecutionContext = new QueryExecutionContext { Database = "default" },
            ResultConfiguration = new ResultConfiguration { OutputLocation = "s3://athena-results/" },
        });

        var queryId = resp.QueryExecutionId;
        Assert.NotEmpty(queryId);

        var status = await _athena.GetQueryExecutionAsync(new GetQueryExecutionRequest
        {
            QueryExecutionId = queryId,
        });

        Assert.Equal(QueryExecutionState.SUCCEEDED, status.QueryExecution.Status.State);

        var results = await _athena.GetQueryResultsAsync(new GetQueryResultsRequest
        {
            QueryExecutionId = queryId,
        });

        Assert.True(results.ResultSet.Rows.Count >= 2);
        Assert.Equal("num", results.ResultSet.Rows[0].Data[0].VarCharValue);
        Assert.Equal("1", results.ResultSet.Rows[1].Data[0].VarCharValue);
    }

    [Fact]
    public async Task StopQueryExecution()
    {
        var resp = await _athena.StartQueryExecutionAsync(new StartQueryExecutionRequest
        {
            QueryString = "SELECT 1",
            ResultConfiguration = new ResultConfiguration { OutputLocation = "s3://athena-results/" },
        });

        await _athena.StopQueryExecutionAsync(new StopQueryExecutionRequest
        {
            QueryExecutionId = resp.QueryExecutionId,
        });

        var desc = await _athena.GetQueryExecutionAsync(new GetQueryExecutionRequest
        {
            QueryExecutionId = resp.QueryExecutionId,
        });

        // Query may have already completed, so accept both states
        Assert.Contains(desc.QueryExecution.Status.State.Value,
            new[] { QueryExecutionState.CANCELLED.Value, QueryExecutionState.SUCCEEDED.Value });
    }

    [Fact]
    public async Task ListQueryExecutions()
    {
        await _athena.StartQueryExecutionAsync(new StartQueryExecutionRequest
        {
            QueryString = "SELECT 1",
            ResultConfiguration = new ResultConfiguration { OutputLocation = "s3://athena-results/" },
        });

        var resp = await _athena.ListQueryExecutionsAsync(new ListQueryExecutionsRequest
        {
            WorkGroup = "primary",
        });

        Assert.NotEmpty(resp.QueryExecutionIds);
    }

    [Fact]
    public async Task BatchGetQueryExecution()
    {
        var q1 = await _athena.StartQueryExecutionAsync(new StartQueryExecutionRequest
        {
            QueryString = "SELECT 42",
            ResultConfiguration = new ResultConfiguration { OutputLocation = "s3://athena-results/" },
        });

        var q2 = await _athena.StartQueryExecutionAsync(new StartQueryExecutionRequest
        {
            QueryString = "SELECT 99",
            ResultConfiguration = new ResultConfiguration { OutputLocation = "s3://athena-results/" },
        });

        var resp = await _athena.BatchGetQueryExecutionAsync(new BatchGetQueryExecutionRequest
        {
            QueryExecutionIds = [q1.QueryExecutionId, q2.QueryExecutionId, "nonexistent-id"],
        });

        Assert.Equal(2, resp.QueryExecutions.Count);
        Assert.Single(resp.UnprocessedQueryExecutionIds);
    }

    [Fact]
    public async Task QueryNotFoundReturnsError()
    {
        await Assert.ThrowsAsync<InvalidRequestException>(() =>
            _athena.GetQueryExecutionAsync(new GetQueryExecutionRequest
            {
                QueryExecutionId = "nonexistent",
            }));
    }

    // -- WorkGroup CRUD --------------------------------------------------------

    [Fact]
    public async Task CreateWorkGroupAndGet()
    {
        await _athena.CreateWorkGroupAsync(new CreateWorkGroupRequest
        {
            Name = "ath-wg-v2",
            Description = "V2 workgroup",
            Configuration = new WorkGroupConfiguration
            {
                ResultConfiguration = new ResultConfiguration { OutputLocation = "s3://ath-out/v2/" },
            },
        });

        var resp = await _athena.GetWorkGroupAsync(new GetWorkGroupRequest { WorkGroup = "ath-wg-v2" });
        Assert.Equal("ath-wg-v2", resp.WorkGroup.Name);
        Assert.Equal("V2 workgroup", resp.WorkGroup.Description);
        Assert.Equal(WorkGroupState.ENABLED, resp.WorkGroup.State);
    }

    [Fact]
    public async Task ListWorkGroupsIncludesPrimary()
    {
        var resp = await _athena.ListWorkGroupsAsync(new ListWorkGroupsRequest());
        Assert.Contains(resp.WorkGroups, wg => wg.Name == "primary");
    }

    [Fact]
    public async Task UpdateWorkGroupDescription()
    {
        var wgName = $"upd-wg-{Guid.NewGuid():N}";
        await _athena.CreateWorkGroupAsync(new CreateWorkGroupRequest
        {
            Name = wgName,
            Description = "before",
        });

        await _athena.UpdateWorkGroupAsync(new UpdateWorkGroupRequest
        {
            WorkGroup = wgName,
            Description = "after",
        });

        var resp = await _athena.GetWorkGroupAsync(new GetWorkGroupRequest { WorkGroup = wgName });
        Assert.Equal("after", resp.WorkGroup.Description);
    }

    [Fact]
    public async Task UpdateWorkGroupResultConfiguration()
    {
        await _athena.CreateWorkGroupAsync(new CreateWorkGroupRequest
        {
            Name = "upd-cfg-wg",
            Configuration = new WorkGroupConfiguration
            {
                ResultConfiguration = new ResultConfiguration { OutputLocation = "s3://old/" },
            },
        });

        await _athena.UpdateWorkGroupAsync(new UpdateWorkGroupRequest
        {
            WorkGroup = "upd-cfg-wg",
            ConfigurationUpdates = new WorkGroupConfigurationUpdates
            {
                ResultConfigurationUpdates = new ResultConfigurationUpdates
                {
                    OutputLocation = "s3://new/",
                },
            },
        });

        var resp = await _athena.GetWorkGroupAsync(new GetWorkGroupRequest { WorkGroup = "upd-cfg-wg" });
        var config = resp.WorkGroup.Configuration;
        Assert.Contains("new", config.ResultConfiguration.OutputLocation);
    }

    [Fact]
    public async Task DeleteWorkGroupAndVerifyGone()
    {
        await _athena.CreateWorkGroupAsync(new CreateWorkGroupRequest
        {
            Name = "del-wg",
        });

        await _athena.DeleteWorkGroupAsync(new DeleteWorkGroupRequest
        {
            WorkGroup = "del-wg",
            RecursiveDeleteOption = true,
        });

        await Assert.ThrowsAsync<InvalidRequestException>(() =>
            _athena.GetWorkGroupAsync(new GetWorkGroupRequest { WorkGroup = "del-wg" }));
    }

    [Fact]
    public async Task CannotDeletePrimaryWorkGroup()
    {
        await Assert.ThrowsAsync<InvalidRequestException>(() =>
            _athena.DeleteWorkGroupAsync(new DeleteWorkGroupRequest
            {
                WorkGroup = "primary",
            }));
    }

    // -- Named Query CRUD ------------------------------------------------------

    [Fact]
    public async Task CreateNamedQueryAndGet()
    {
        var resp = await _athena.CreateNamedQueryAsync(new CreateNamedQueryRequest
        {
            Name = "ath-nq-v2",
            Database = "default",
            QueryString = "SELECT * FROM t LIMIT 10",
            WorkGroup = "primary",
            Description = "Named query v2",
        });

        var nqId = resp.NamedQueryId;
        var nq = await _athena.GetNamedQueryAsync(new GetNamedQueryRequest { NamedQueryId = nqId });
        Assert.Equal("ath-nq-v2", nq.NamedQuery.Name);
        Assert.Equal("default", nq.NamedQuery.Database);
        Assert.Equal("SELECT * FROM t LIMIT 10", nq.NamedQuery.QueryString);
    }

    [Fact]
    public async Task ListNamedQueriesReturnsIds()
    {
        var resp = await _athena.CreateNamedQueryAsync(new CreateNamedQueryRequest
        {
            Name = "lnq-test",
            Database = "default",
            QueryString = "SELECT 1",
            WorkGroup = "primary",
        });

        var listed = await _athena.ListNamedQueriesAsync(new ListNamedQueriesRequest());
        Assert.Contains(resp.NamedQueryId, listed.NamedQueryIds);
    }

    [Fact]
    public async Task DeleteNamedQueryAndVerifyGone()
    {
        var resp = await _athena.CreateNamedQueryAsync(new CreateNamedQueryRequest
        {
            Name = "del-nq",
            Database = "default",
            QueryString = "SELECT 1",
        });

        await _athena.DeleteNamedQueryAsync(new DeleteNamedQueryRequest { NamedQueryId = resp.NamedQueryId });

        await Assert.ThrowsAsync<InvalidRequestException>(() =>
            _athena.GetNamedQueryAsync(new GetNamedQueryRequest { NamedQueryId = resp.NamedQueryId }));
    }

    [Fact]
    public async Task BatchGetNamedQuery()
    {
        var nq1 = await _athena.CreateNamedQueryAsync(new CreateNamedQueryRequest
        {
            Name = "bgnq-1",
            Database = "default",
            QueryString = "SELECT 1",
        });

        var nq2 = await _athena.CreateNamedQueryAsync(new CreateNamedQueryRequest
        {
            Name = "bgnq-2",
            Database = "default",
            QueryString = "SELECT 2",
        });

        var resp = await _athena.BatchGetNamedQueryAsync(new BatchGetNamedQueryRequest
        {
            NamedQueryIds = [nq1.NamedQueryId, nq2.NamedQueryId, "nonexistent-id"],
        });

        Assert.Equal(2, resp.NamedQueries.Count);
        Assert.Single(resp.UnprocessedNamedQueryIds);
    }

    // -- Data Catalog CRUD -----------------------------------------------------

    [Fact]
    public async Task CreateDataCatalogAndGet()
    {
        await _athena.CreateDataCatalogAsync(new CreateDataCatalogRequest
        {
            Name = "ath-cat-v2",
            Type = DataCatalogType.HIVE,
            Description = "V2 catalog",
            Parameters = new Dictionary<string, string> { ["metadata-function"] = "arn:aws:lambda:us-east-1:000000000000:function:f" },
        });

        var resp = await _athena.GetDataCatalogAsync(new GetDataCatalogRequest { Name = "ath-cat-v2" });
        Assert.Equal("ath-cat-v2", resp.DataCatalog.Name);
        Assert.Equal(DataCatalogType.HIVE, resp.DataCatalog.Type);
    }

    [Fact]
    public async Task ListDataCatalogs()
    {
        await _athena.CreateDataCatalogAsync(new CreateDataCatalogRequest
        {
            Name = "ldc-cat",
            Type = DataCatalogType.HIVE,
        });

        var resp = await _athena.ListDataCatalogsAsync(new ListDataCatalogsRequest());
        Assert.Contains(resp.DataCatalogsSummary, c => c.CatalogName == "ldc-cat");
    }

    [Fact]
    public async Task UpdateDataCatalog()
    {
        await _athena.CreateDataCatalogAsync(new CreateDataCatalogRequest
        {
            Name = "udc-cat",
            Type = DataCatalogType.HIVE,
            Description = "original",
        });

        await _athena.UpdateDataCatalogAsync(new UpdateDataCatalogRequest
        {
            Name = "udc-cat",
            Type = DataCatalogType.HIVE,
            Description = "Updated v2",
        });

        var resp = await _athena.GetDataCatalogAsync(new GetDataCatalogRequest { Name = "udc-cat" });
        Assert.Equal("Updated v2", resp.DataCatalog.Description);
    }

    [Fact]
    public async Task DeleteDataCatalogAndVerifyGone()
    {
        await _athena.CreateDataCatalogAsync(new CreateDataCatalogRequest
        {
            Name = "del-cat",
            Type = DataCatalogType.HIVE,
        });

        await _athena.DeleteDataCatalogAsync(new DeleteDataCatalogRequest { Name = "del-cat" });

        await Assert.ThrowsAsync<InvalidRequestException>(() =>
            _athena.GetDataCatalogAsync(new GetDataCatalogRequest { Name = "del-cat" }));
    }

    [Fact]
    public async Task CannotDeleteDefaultDataCatalog()
    {
        await Assert.ThrowsAsync<InvalidRequestException>(() =>
            _athena.DeleteDataCatalogAsync(new DeleteDataCatalogRequest { Name = "AwsDataCatalog" }));
    }

    // -- Prepared Statement CRUD -----------------------------------------------

    [Fact]
    public async Task CreatePreparedStatementAndGet()
    {
        await _athena.CreatePreparedStatementAsync(new CreatePreparedStatementRequest
        {
            StatementName = "ath_ps_v2",
            WorkGroup = "primary",
            QueryStatement = "SELECT ? AS val",
            Description = "Prepared v2",
        });

        var resp = await _athena.GetPreparedStatementAsync(new GetPreparedStatementRequest
        {
            StatementName = "ath_ps_v2",
            WorkGroup = "primary",
        });

        Assert.Equal("ath_ps_v2", resp.PreparedStatement.StatementName);
        Assert.Equal("SELECT ? AS val", resp.PreparedStatement.QueryStatement);
    }

    [Fact]
    public async Task ListPreparedStatements()
    {
        await _athena.CreatePreparedStatementAsync(new CreatePreparedStatementRequest
        {
            StatementName = "lps_stmt",
            WorkGroup = "primary",
            QueryStatement = "SELECT 1",
        });

        var resp = await _athena.ListPreparedStatementsAsync(new ListPreparedStatementsRequest
        {
            WorkGroup = "primary",
        });

        Assert.Contains(resp.PreparedStatements, s => s.StatementName == "lps_stmt");
    }

    [Fact]
    public async Task DeletePreparedStatementAndVerifyGone()
    {
        await _athena.CreatePreparedStatementAsync(new CreatePreparedStatementRequest
        {
            StatementName = "del_ps",
            WorkGroup = "primary",
            QueryStatement = "SELECT 1",
        });

        await _athena.DeletePreparedStatementAsync(new DeletePreparedStatementRequest
        {
            StatementName = "del_ps",
            WorkGroup = "primary",
        });

        await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            _athena.GetPreparedStatementAsync(new GetPreparedStatementRequest
            {
                StatementName = "del_ps",
                WorkGroup = "primary",
            }));
    }

    // -- Tags ------------------------------------------------------------------

    [Fact]
    public async Task TagAndUntagResource()
    {
        await _athena.CreateWorkGroupAsync(new CreateWorkGroupRequest
        {
            Name = "tag-wg",
            Tags = [new Tag { Key = "init", Value = "yes" }],
        });

        var arn = "arn:aws:athena:us-east-1:000000000000:workgroup/tag-wg";

        await _athena.TagResourceAsync(new TagResourceRequest
        {
            ResourceARN = arn,
            Tags = [new Tag { Key = "env", Value = "dev" }],
        });

        var resp = await _athena.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceARN = arn,
        });

        var tagMap = resp.Tags.ToDictionary(t => t.Key, t => t.Value);
        Assert.Equal("dev", tagMap["env"]);

        await _athena.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceARN = arn,
            TagKeys = ["env"],
        });

        var resp2 = await _athena.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceARN = arn,
        });

        Assert.DoesNotContain(resp2.Tags, t => t.Key == "env");
    }

    // -- Table Metadata (stubs) ------------------------------------------------

    [Fact]
    public async Task GetTableMetadata()
    {
        var resp = await _athena.GetTableMetadataAsync(new GetTableMetadataRequest
        {
            CatalogName = "AwsDataCatalog",
            DatabaseName = "default",
            TableName = "test_table",
        });

        Assert.Equal("test_table", resp.TableMetadata.Name);
    }

    [Fact]
    public async Task ListTableMetadata()
    {
        var resp = await _athena.ListTableMetadataAsync(new ListTableMetadataRequest
        {
            CatalogName = "AwsDataCatalog",
            DatabaseName = "default",
        });

        Assert.NotNull(resp.TableMetadataList);
    }

    // -- Query results validation -----------------------------------------------

    [Fact]
    public async Task QueryResultsContainColumnInfo()
    {
        var qResp = await _athena.StartQueryExecutionAsync(new StartQueryExecutionRequest
        {
            QueryString = "SELECT 42 AS answer, 'world' AS hello",
            QueryExecutionContext = new QueryExecutionContext { Database = "default" },
            ResultConfiguration = new ResultConfiguration { OutputLocation = "s3://athena-out/" },
        });

        var results = await _athena.GetQueryResultsAsync(new GetQueryResultsRequest
        {
            QueryExecutionId = qResp.QueryExecutionId,
        });

        var rows = results.ResultSet.Rows;
        Assert.True(rows.Count >= 2);
        Assert.Equal("answer", rows[0].Data[0].VarCharValue);
        Assert.Equal("42", rows[1].Data[0].VarCharValue);

        var colInfo = results.ResultSet.ResultSetMetadata.ColumnInfo;
        Assert.Equal(2, colInfo.Count);
    }

    // -- Duplicate workgroup ---------------------------------------------------

    [Fact]
    public async Task CreateDuplicateWorkGroupFails()
    {
        await _athena.CreateWorkGroupAsync(new CreateWorkGroupRequest { Name = "dup-wg" });

        await Assert.ThrowsAsync<InvalidRequestException>(() =>
            _athena.CreateWorkGroupAsync(new CreateWorkGroupRequest { Name = "dup-wg" }));
    }

    // -- Duplicate data catalog ------------------------------------------------

    [Fact]
    public async Task CreateDuplicateDataCatalogFails()
    {
        await _athena.CreateDataCatalogAsync(new CreateDataCatalogRequest
        {
            Name = "dup-cat",
            Type = DataCatalogType.HIVE,
        });

        await Assert.ThrowsAsync<InvalidRequestException>(() =>
            _athena.CreateDataCatalogAsync(new CreateDataCatalogRequest
            {
                Name = "dup-cat",
                Type = DataCatalogType.HIVE,
            }));
    }

    // -- Duplicate prepared statement ------------------------------------------

    [Fact]
    public async Task CreateDuplicatePreparedStatementFails()
    {
        await _athena.CreatePreparedStatementAsync(new CreatePreparedStatementRequest
        {
            StatementName = "dup_ps",
            WorkGroup = "primary",
            QueryStatement = "SELECT 1",
        });

        await Assert.ThrowsAsync<InvalidRequestException>(() =>
            _athena.CreatePreparedStatementAsync(new CreatePreparedStatementRequest
            {
                StatementName = "dup_ps",
                WorkGroup = "primary",
                QueryStatement = "SELECT 2",
            }));
    }
}
