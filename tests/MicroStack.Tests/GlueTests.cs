using Amazon;
using Amazon.Glue;
using Amazon.Glue.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the Glue service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_glue.py.
/// </summary>
public sealed class GlueTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonGlueClient _glue;

    public GlueTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _glue = CreateClient(fixture);
    }

    private static AmazonGlueClient CreateClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonGlueConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonGlueClient(new BasicAWSCredentials("test", "test"), config);
    }

    public async ValueTask InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _glue.Dispose();
        return ValueTask.CompletedTask;
    }

    // -- Database CRUD ---------------------------------------------------------

    [Fact]
    public async Task CreateDatabaseAndGet()
    {
        await _glue.CreateDatabaseAsync(new CreateDatabaseRequest
        {
            DatabaseInput = new DatabaseInput { Name = "test_db", Description = "Test database" },
        });

        var resp = await _glue.GetDatabaseAsync(new GetDatabaseRequest { Name = "test_db" });
        resp.Database.Name.ShouldBe("test_db");
        resp.Database.Description.ShouldBe("Test database");
    }

    [Fact]
    public async Task CreateDatabaseDuplicateFails()
    {
        await _glue.CreateDatabaseAsync(new CreateDatabaseRequest
        {
            DatabaseInput = new DatabaseInput { Name = "dup_db" },
        });

        var ex = await Should.ThrowAsync<AlreadyExistsException>(() =>
            _glue.CreateDatabaseAsync(new CreateDatabaseRequest
            {
                DatabaseInput = new DatabaseInput { Name = "dup_db" },
            }));

        ex.Message.ShouldContain("already exists");
    }

    [Fact]
    public async Task UpdateDatabaseDescription()
    {
        await _glue.CreateDatabaseAsync(new CreateDatabaseRequest
        {
            DatabaseInput = new DatabaseInput { Name = "upd_db", Description = "original" },
        });

        await _glue.UpdateDatabaseAsync(new UpdateDatabaseRequest
        {
            Name = "upd_db",
            DatabaseInput = new DatabaseInput { Name = "upd_db", Description = "updated" },
        });

        var resp = await _glue.GetDatabaseAsync(new GetDatabaseRequest { Name = "upd_db" });
        resp.Database.Description.ShouldBe("updated");
    }

    [Fact]
    public async Task DeleteDatabaseAndVerifyGone()
    {
        await _glue.CreateDatabaseAsync(new CreateDatabaseRequest
        {
            DatabaseInput = new DatabaseInput { Name = "del_db" },
        });

        await _glue.DeleteDatabaseAsync(new DeleteDatabaseRequest { Name = "del_db" });

        var ex = await Should.ThrowAsync<EntityNotFoundException>(() =>
            _glue.GetDatabaseAsync(new GetDatabaseRequest { Name = "del_db" }));

        ex.Message.ShouldContain("not found");
    }

    [Fact]
    public async Task GetDatabasesReturnsList()
    {
        await _glue.CreateDatabaseAsync(new CreateDatabaseRequest
        {
            DatabaseInput = new DatabaseInput { Name = "list_db_a" },
        });

        await _glue.CreateDatabaseAsync(new CreateDatabaseRequest
        {
            DatabaseInput = new DatabaseInput { Name = "list_db_b" },
        });

        var resp = await _glue.GetDatabasesAsync(new GetDatabasesRequest());
        (resp.DatabaseList.Count >= 2).ShouldBe(true);
        resp.DatabaseList.ShouldContain(d => d.Name == "list_db_a");
        resp.DatabaseList.ShouldContain(d => d.Name == "list_db_b");
    }

    // -- Table CRUD ------------------------------------------------------------

    [Fact]
    public async Task CreateTableAndGet()
    {
        await _glue.CreateDatabaseAsync(new CreateDatabaseRequest
        {
            DatabaseInput = new DatabaseInput { Name = "tbl_db" },
        });

        await _glue.CreateTableAsync(new CreateTableRequest
        {
            DatabaseName = "tbl_db",
            TableInput = new TableInput
            {
                Name = "tbl_v2",
                StorageDescriptor = new StorageDescriptor
                {
                    Columns = [new Column { Name = "id", Type = "int" }, new Column { Name = "name", Type = "string" }],
                    Location = "s3://bucket/tbl_v2/",
                    InputFormat = "TIF",
                    OutputFormat = "TOF",
                    SerdeInfo = new SerDeInfo { SerializationLibrary = "SL" },
                },
                TableType = "EXTERNAL_TABLE",
            },
        });

        var resp = await _glue.GetTableAsync(new GetTableRequest { DatabaseName = "tbl_db", Name = "tbl_v2" });
        resp.Table.Name.ShouldBe("tbl_v2");
        resp.Table.StorageDescriptor.Columns.Count.ShouldBe(2);
    }

    [Fact]
    public async Task UpdateTableDescription()
    {
        await _glue.CreateDatabaseAsync(new CreateDatabaseRequest
        {
            DatabaseInput = new DatabaseInput { Name = "upd_tbl_db" },
        });

        await _glue.CreateTableAsync(new CreateTableRequest
        {
            DatabaseName = "upd_tbl_db",
            TableInput = new TableInput
            {
                Name = "upd_tbl",
                StorageDescriptor = new StorageDescriptor
                {
                    Columns = [new Column { Name = "c", Type = "string" }],
                    Location = "s3://b/t/",
                    InputFormat = "TIF",
                    OutputFormat = "TOF",
                    SerdeInfo = new SerDeInfo { SerializationLibrary = "SL" },
                },
            },
        });

        await _glue.UpdateTableAsync(new UpdateTableRequest
        {
            DatabaseName = "upd_tbl_db",
            TableInput = new TableInput { Name = "upd_tbl", Description = "updated table" },
        });

        var resp = await _glue.GetTableAsync(new GetTableRequest { DatabaseName = "upd_tbl_db", Name = "upd_tbl" });
        resp.Table.Description.ShouldBe("updated table");
    }

    [Fact]
    public async Task DeleteTableAndVerifyGone()
    {
        await _glue.CreateDatabaseAsync(new CreateDatabaseRequest
        {
            DatabaseInput = new DatabaseInput { Name = "del_tbl_db" },
        });

        await _glue.CreateTableAsync(new CreateTableRequest
        {
            DatabaseName = "del_tbl_db",
            TableInput = new TableInput
            {
                Name = "del_tbl",
                StorageDescriptor = new StorageDescriptor
                {
                    Columns = [],
                    Location = "s3://b/t/",
                    InputFormat = "TIF",
                    OutputFormat = "TOF",
                    SerdeInfo = new SerDeInfo { SerializationLibrary = "SL" },
                },
            },
        });

        await _glue.DeleteTableAsync(new DeleteTableRequest { DatabaseName = "del_tbl_db", Name = "del_tbl" });

        await Should.ThrowAsync<EntityNotFoundException>(() =>
            _glue.GetTableAsync(new GetTableRequest { DatabaseName = "del_tbl_db", Name = "del_tbl" }));
    }

    [Fact]
    public async Task GetTablesReturnsList()
    {
        await _glue.CreateDatabaseAsync(new CreateDatabaseRequest
        {
            DatabaseInput = new DatabaseInput { Name = "gt_db" },
        });

        await _glue.CreateTableAsync(new CreateTableRequest
        {
            DatabaseName = "gt_db",
            TableInput = new TableInput
            {
                Name = "lt_a",
                StorageDescriptor = new StorageDescriptor { Columns = [], Location = "s3://b/a/", InputFormat = "TIF", OutputFormat = "TOF", SerdeInfo = new SerDeInfo { SerializationLibrary = "SL" } },
            },
        });

        await _glue.CreateTableAsync(new CreateTableRequest
        {
            DatabaseName = "gt_db",
            TableInput = new TableInput
            {
                Name = "lt_b",
                StorageDescriptor = new StorageDescriptor { Columns = [], Location = "s3://b/b/", InputFormat = "TIF", OutputFormat = "TOF", SerdeInfo = new SerDeInfo { SerializationLibrary = "SL" } },
            },
        });

        var resp = await _glue.GetTablesAsync(new GetTablesRequest { DatabaseName = "gt_db" });
        var names = resp.TableList.Select(t => t.Name).ToList();
        names.ShouldContain("lt_a");
        names.ShouldContain("lt_b");
    }

    // -- Partition CRUD --------------------------------------------------------

    [Fact]
    public async Task CreatePartitionAndGet()
    {
        await _glue.CreateDatabaseAsync(new CreateDatabaseRequest
        {
            DatabaseInput = new DatabaseInput { Name = "part_db" },
        });

        await _glue.CreateTableAsync(new CreateTableRequest
        {
            DatabaseName = "part_db",
            TableInput = new TableInput
            {
                Name = "part_tbl",
                StorageDescriptor = new StorageDescriptor { Columns = [], Location = "s3://b/pt/", InputFormat = "TIF", OutputFormat = "TOF", SerdeInfo = new SerDeInfo { SerializationLibrary = "SL" } },
                PartitionKeys = [new Column { Name = "dt", Type = "string" }],
            },
        });

        await _glue.CreatePartitionAsync(new CreatePartitionRequest
        {
            DatabaseName = "part_db",
            TableName = "part_tbl",
            PartitionInput = new PartitionInput
            {
                Values = ["2024-01-01"],
                StorageDescriptor = new StorageDescriptor { Columns = [], Location = "s3://b/pt/dt=2024-01-01", InputFormat = "", OutputFormat = "", SerdeInfo = new SerDeInfo() },
            },
        });

        var resp = await _glue.GetPartitionAsync(new GetPartitionRequest
        {
            DatabaseName = "part_db",
            TableName = "part_tbl",
            PartitionValues = ["2024-01-01"],
        });

        resp.Partition.Values.ShouldBe(["2024-01-01"]);
    }

    [Fact]
    public async Task GetPartitionsReturnsList()
    {
        await _glue.CreateDatabaseAsync(new CreateDatabaseRequest
        {
            DatabaseInput = new DatabaseInput { Name = "gp_db" },
        });

        await _glue.CreateTableAsync(new CreateTableRequest
        {
            DatabaseName = "gp_db",
            TableInput = new TableInput
            {
                Name = "gp_tbl",
                StorageDescriptor = new StorageDescriptor { Columns = [], Location = "s3://b/pt/", InputFormat = "", OutputFormat = "", SerdeInfo = new SerDeInfo() },
                PartitionKeys = [new Column { Name = "year", Type = "string" }, new Column { Name = "month", Type = "string" }],
            },
        });

        await _glue.CreatePartitionAsync(new CreatePartitionRequest
        {
            DatabaseName = "gp_db",
            TableName = "gp_tbl",
            PartitionInput = new PartitionInput
            {
                Values = ["2024", "01"],
                StorageDescriptor = new StorageDescriptor { Columns = [], Location = "s3://b/pt/y=2024/m=01/", InputFormat = "", OutputFormat = "", SerdeInfo = new SerDeInfo() },
            },
        });

        await _glue.CreatePartitionAsync(new CreatePartitionRequest
        {
            DatabaseName = "gp_db",
            TableName = "gp_tbl",
            PartitionInput = new PartitionInput
            {
                Values = ["2024", "02"],
                StorageDescriptor = new StorageDescriptor { Columns = [], Location = "s3://b/pt/y=2024/m=02/", InputFormat = "", OutputFormat = "", SerdeInfo = new SerDeInfo() },
            },
        });

        var resp = await _glue.GetPartitionsAsync(new GetPartitionsRequest { DatabaseName = "gp_db", TableName = "gp_tbl" });
        resp.Partitions.Count.ShouldBe(2);
    }

    [Fact]
    public async Task DeletePartitionAndVerifyGone()
    {
        await _glue.CreateDatabaseAsync(new CreateDatabaseRequest
        {
            DatabaseInput = new DatabaseInput { Name = "dp_db" },
        });

        await _glue.CreateTableAsync(new CreateTableRequest
        {
            DatabaseName = "dp_db",
            TableInput = new TableInput
            {
                Name = "dp_tbl",
                StorageDescriptor = new StorageDescriptor { Columns = [], Location = "s3://b/k", InputFormat = "", OutputFormat = "", SerdeInfo = new SerDeInfo() },
                PartitionKeys = [new Column { Name = "dt", Type = "string" }],
            },
        });

        await _glue.CreatePartitionAsync(new CreatePartitionRequest
        {
            DatabaseName = "dp_db",
            TableName = "dp_tbl",
            PartitionInput = new PartitionInput
            {
                Values = ["2024-01-01"],
                StorageDescriptor = new StorageDescriptor { Columns = [], Location = "s3://b/k/dt=2024-01-01", InputFormat = "", OutputFormat = "", SerdeInfo = new SerDeInfo() },
            },
        });

        await _glue.DeletePartitionAsync(new DeletePartitionRequest
        {
            DatabaseName = "dp_db",
            TableName = "dp_tbl",
            PartitionValues = ["2024-01-01"],
        });

        var parts = await _glue.GetPartitionsAsync(new GetPartitionsRequest { DatabaseName = "dp_db", TableName = "dp_tbl" });
        parts.Partitions.ShouldBeEmpty();
    }

    [Fact]
    public async Task DuplicatePartitionFails()
    {
        await _glue.CreateDatabaseAsync(new CreateDatabaseRequest
        {
            DatabaseInput = new DatabaseInput { Name = "dup_part_db" },
        });

        await _glue.CreateTableAsync(new CreateTableRequest
        {
            DatabaseName = "dup_part_db",
            TableInput = new TableInput
            {
                Name = "dup_part_tbl",
                StorageDescriptor = new StorageDescriptor { Columns = [], Location = "s3://b/k", InputFormat = "", OutputFormat = "", SerdeInfo = new SerDeInfo() },
                PartitionKeys = [new Column { Name = "dt", Type = "string" }],
            },
        });

        var partInput = new PartitionInput
        {
            Values = ["2024-01-01"],
            StorageDescriptor = new StorageDescriptor { Columns = [], Location = "s3://b/k/dt=2024-01-01", InputFormat = "", OutputFormat = "", SerdeInfo = new SerDeInfo() },
        };

        await _glue.CreatePartitionAsync(new CreatePartitionRequest
        {
            DatabaseName = "dup_part_db",
            TableName = "dup_part_tbl",
            PartitionInput = partInput,
        });

        await Should.ThrowAsync<AlreadyExistsException>(() =>
            _glue.CreatePartitionAsync(new CreatePartitionRequest
            {
                DatabaseName = "dup_part_db",
                TableName = "dup_part_tbl",
                PartitionInput = partInput,
            }));
    }

    [Fact]
    public async Task BatchCreatePartition()
    {
        await _glue.CreateDatabaseAsync(new CreateDatabaseRequest
        {
            DatabaseInput = new DatabaseInput { Name = "bcp_db" },
        });

        await _glue.CreateTableAsync(new CreateTableRequest
        {
            DatabaseName = "bcp_db",
            TableInput = new TableInput
            {
                Name = "bcp_tbl",
                StorageDescriptor = new StorageDescriptor { Columns = [], Location = "s3://b/k", InputFormat = "", OutputFormat = "", SerdeInfo = new SerDeInfo() },
                PartitionKeys = [new Column { Name = "dt", Type = "string" }],
            },
        });

        var resp = await _glue.BatchCreatePartitionAsync(new BatchCreatePartitionRequest
        {
            DatabaseName = "bcp_db",
            TableName = "bcp_tbl",
            PartitionInputList =
            [
                new PartitionInput
                {
                    Values = ["2024-01"],
                    StorageDescriptor = new StorageDescriptor { Columns = [], Location = "s3://b/k/1", InputFormat = "", OutputFormat = "", SerdeInfo = new SerDeInfo() },
                },
                new PartitionInput
                {
                    Values = ["2024-02"],
                    StorageDescriptor = new StorageDescriptor { Columns = [], Location = "s3://b/k/2", InputFormat = "", OutputFormat = "", SerdeInfo = new SerDeInfo() },
                },
            ],
        });

        resp.Errors.ShouldBeEmpty();

        var parts = await _glue.GetPartitionsAsync(new GetPartitionsRequest { DatabaseName = "bcp_db", TableName = "bcp_tbl" });
        parts.Partitions.Count.ShouldBe(2);
    }

    // -- Crawler CRUD ----------------------------------------------------------

    [Fact]
    public async Task CreateCrawlerAndGet()
    {
        await _glue.CreateCrawlerAsync(new CreateCrawlerRequest
        {
            Name = "test-crawler",
            Role = "arn:aws:iam::000000000000:role/GlueRole",
            DatabaseName = "default",
            Targets = new CrawlerTargets { S3Targets = [new S3Target { Path = "s3://bucket/data/" }] },
        });

        var resp = await _glue.GetCrawlerAsync(new GetCrawlerRequest { Name = "test-crawler" });
        resp.Crawler.Name.ShouldBe("test-crawler");
        resp.Crawler.State.ShouldBe(CrawlerState.READY);
    }

    [Fact]
    public async Task StartCrawlerSetsRunning()
    {
        await _glue.CreateCrawlerAsync(new CreateCrawlerRequest
        {
            Name = "start-crawler",
            Role = "role",
            Targets = new CrawlerTargets { S3Targets = [new S3Target { Path = "s3://b/d/" }] },
        });

        await _glue.StartCrawlerAsync(new StartCrawlerRequest { Name = "start-crawler" });

        var resp = await _glue.GetCrawlerAsync(new GetCrawlerRequest { Name = "start-crawler" });
        resp.Crawler.State.ShouldBe(CrawlerState.RUNNING);
    }

    [Fact]
    public async Task StopCrawlerResetsToReady()
    {
        await _glue.CreateCrawlerAsync(new CreateCrawlerRequest
        {
            Name = "stop-crawler",
            Role = "role",
            Targets = new CrawlerTargets { S3Targets = [new S3Target { Path = "s3://b/d/" }] },
        });

        await _glue.StartCrawlerAsync(new StartCrawlerRequest { Name = "stop-crawler" });
        await _glue.StopCrawlerAsync(new StopCrawlerRequest { Name = "stop-crawler" });

        var resp = await _glue.GetCrawlerAsync(new GetCrawlerRequest { Name = "stop-crawler" });
        resp.Crawler.State.ShouldBe(CrawlerState.READY);
    }

    [Fact]
    public async Task DeleteCrawlerAndVerifyGone()
    {
        await _glue.CreateCrawlerAsync(new CreateCrawlerRequest
        {
            Name = "del-crawler",
            Role = "role",
            Targets = new CrawlerTargets { S3Targets = [new S3Target { Path = "s3://b/d/" }] },
        });

        await _glue.DeleteCrawlerAsync(new DeleteCrawlerRequest { Name = "del-crawler" });

        await Should.ThrowAsync<EntityNotFoundException>(() =>
            _glue.GetCrawlerAsync(new GetCrawlerRequest { Name = "del-crawler" }));
    }

    [Fact]
    public async Task GetCrawlerMetricsReturnsMetrics()
    {
        await _glue.CreateCrawlerAsync(new CreateCrawlerRequest
        {
            Name = "metrics-crawler",
            Role = "role",
            Targets = new CrawlerTargets { S3Targets = [new S3Target { Path = "s3://b/d/" }] },
        });

        var resp = await _glue.GetCrawlerMetricsAsync(new GetCrawlerMetricsRequest
        {
            CrawlerNameList = ["metrics-crawler"],
        });

        resp.CrawlerMetricsList.ShouldHaveSingleItem();
        resp.CrawlerMetricsList[0].CrawlerName.ShouldBe("metrics-crawler");
    }

    [Fact]
    public async Task UpdateCrawlerChangesProperties()
    {
        await _glue.CreateCrawlerAsync(new CreateCrawlerRequest
        {
            Name = "upd-crawler",
            Role = "role-old",
            Description = "old",
            Targets = new CrawlerTargets { S3Targets = [new S3Target { Path = "s3://b/d/" }] },
        });

        await _glue.UpdateCrawlerAsync(new UpdateCrawlerRequest
        {
            Name = "upd-crawler",
            Role = "role-new",
            Description = "new",
        });

        var resp = await _glue.GetCrawlerAsync(new GetCrawlerRequest { Name = "upd-crawler" });
        resp.Crawler.Role.ShouldBe("role-new");
        resp.Crawler.Description.ShouldBe("new");
    }

    // -- Job CRUD --------------------------------------------------------------

    [Fact]
    public async Task CreateJobAndGet()
    {
        await _glue.CreateJobAsync(new CreateJobRequest
        {
            Name = "test-job",
            Role = "arn:aws:iam::000000000000:role/GlueRole",
            Command = new JobCommand { Name = "glueetl", ScriptLocation = "s3://b/s.py" },
            GlueVersion = "3.0",
        });

        var resp = await _glue.GetJobAsync(new GetJobRequest { JobName = "test-job" });
        resp.Job.Name.ShouldBe("test-job");
    }

    [Fact]
    public async Task StartJobRunAndGetRun()
    {
        await _glue.CreateJobAsync(new CreateJobRequest
        {
            Name = "run-job",
            Role = "role",
            Command = new JobCommand { Name = "glueetl", ScriptLocation = "s3://b/s.py" },
        });

        var runResp = await _glue.StartJobRunAsync(new StartJobRunRequest { JobName = "run-job" });
        runResp.JobRunId.ShouldNotBeEmpty();

        var run = await _glue.GetJobRunAsync(new GetJobRunRequest { JobName = "run-job", RunId = runResp.JobRunId });
        run.JobRun.Id.ShouldBe(runResp.JobRunId);
        run.JobRun.JobName.ShouldBe("run-job");
    }

    [Fact]
    public async Task GetJobRunsReturnsList()
    {
        await _glue.CreateJobAsync(new CreateJobRequest
        {
            Name = "runs-job",
            Role = "role",
            Command = new JobCommand { Name = "glueetl", ScriptLocation = "s3://b/s.py" },
        });

        await _glue.StartJobRunAsync(new StartJobRunRequest { JobName = "runs-job" });
        var resp = await _glue.GetJobRunsAsync(new GetJobRunsRequest { JobName = "runs-job" });
        resp.JobRuns.ShouldHaveSingleItem();
    }

    [Fact]
    public async Task DeleteJobAndVerifyGone()
    {
        await _glue.CreateJobAsync(new CreateJobRequest
        {
            Name = "del-job",
            Role = "role",
            Command = new JobCommand { Name = "glueetl", ScriptLocation = "s3://b/s.py" },
        });

        await _glue.DeleteJobAsync(new DeleteJobRequest { JobName = "del-job" });

        await Should.ThrowAsync<EntityNotFoundException>(() =>
            _glue.GetJobAsync(new GetJobRequest { JobName = "del-job" }));
    }

    [Fact]
    public async Task UpdateJobChangesProperties()
    {
        await _glue.CreateJobAsync(new CreateJobRequest
        {
            Name = "upd-job",
            Role = "old-role",
            Description = "old",
            Command = new JobCommand { Name = "glueetl", ScriptLocation = "s3://b/s.py" },
        });

        await _glue.UpdateJobAsync(new UpdateJobRequest
        {
            JobName = "upd-job",
            JobUpdate = new JobUpdate { Description = "new", Role = "new-role" },
        });

        var resp = await _glue.GetJobAsync(new GetJobRequest { JobName = "upd-job" });
        resp.Job.Description.ShouldBe("new");
        resp.Job.Role.ShouldBe("new-role");
    }

    // -- Tags ------------------------------------------------------------------

    [Fact]
    public async Task TagAndUntagResource()
    {
        var arn = "arn:aws:glue:us-east-1:000000000000:database/tag-test-db";
        await _glue.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = arn,
            TagsToAdd = new Dictionary<string, string> { ["env"] = "test", ["team"] = "data" },
        });

        var resp = await _glue.GetTagsAsync(new GetTagsRequest { ResourceArn = arn });
        resp.Tags["env"].ShouldBe("test");
        resp.Tags["team"].ShouldBe("data");

        await _glue.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceArn = arn,
            TagsToRemove = ["team"],
        });

        var resp2 = await _glue.GetTagsAsync(new GetTagsRequest { ResourceArn = arn });
        resp2.Tags["env"].ShouldBe("test");
        resp2.Tags.ContainsKey("team").ShouldBe(false);
    }

    // -- Registry CRUD ---------------------------------------------------------

    [Fact]
    public async Task CreateRegistryAndGet()
    {
        var resp = await _glue.CreateRegistryAsync(new CreateRegistryRequest
        {
            RegistryName = "test-registry",
            Description = "Test registry",
        });

        resp.RegistryName.ShouldBe("test-registry");
        resp.RegistryArn.ShouldContain("test-registry");

        var get = await _glue.GetRegistryAsync(new GetRegistryRequest
        {
            RegistryId = new RegistryId { RegistryName = "test-registry" },
        });

        get.RegistryName.ShouldBe("test-registry");
    }

    [Fact]
    public async Task ListRegistries()
    {
        await _glue.CreateRegistryAsync(new CreateRegistryRequest { RegistryName = "lr-reg-a" });
        await _glue.CreateRegistryAsync(new CreateRegistryRequest { RegistryName = "lr-reg-b" });

        var resp = await _glue.ListRegistriesAsync(new ListRegistriesRequest());
        (resp.Registries.Count >= 2).ShouldBe(true);
    }

    [Fact]
    public async Task DeleteRegistryAndVerifyGone()
    {
        await _glue.CreateRegistryAsync(new CreateRegistryRequest { RegistryName = "del-registry" });
        await _glue.DeleteRegistryAsync(new DeleteRegistryRequest
        {
            RegistryId = new RegistryId { RegistryName = "del-registry" },
        });

        await Should.ThrowAsync<EntityNotFoundException>(() =>
            _glue.GetRegistryAsync(new GetRegistryRequest
            {
                RegistryId = new RegistryId { RegistryName = "del-registry" },
            }));
    }

    // -- Schema CRUD -----------------------------------------------------------

    [Fact]
    public async Task CreateSchemaAndGet()
    {
        await _glue.CreateRegistryAsync(new CreateRegistryRequest { RegistryName = "schema-reg" });

        var resp = await _glue.CreateSchemaAsync(new CreateSchemaRequest
        {
            RegistryId = new RegistryId { RegistryName = "schema-reg" },
            SchemaName = "test-schema",
            DataFormat = DataFormat.AVRO,
            SchemaDefinition = "{\"type\":\"record\",\"name\":\"test\",\"fields\":[]}",
        });

        resp.SchemaName.ShouldBe("test-schema");

        var get = await _glue.GetSchemaAsync(new GetSchemaRequest
        {
            SchemaId = new SchemaId { RegistryName = "schema-reg", SchemaName = "test-schema" },
        });

        get.SchemaName.ShouldBe("test-schema");
    }

    [Fact]
    public async Task RegisterSchemaVersionAndGet()
    {
        await _glue.CreateRegistryAsync(new CreateRegistryRequest { RegistryName = "ver-reg" });
        await _glue.CreateSchemaAsync(new CreateSchemaRequest
        {
            RegistryId = new RegistryId { RegistryName = "ver-reg" },
            SchemaName = "ver-schema",
            DataFormat = DataFormat.AVRO,
            SchemaDefinition = "{\"version\":1}",
        });

        var resp = await _glue.RegisterSchemaVersionAsync(new RegisterSchemaVersionRequest
        {
            SchemaId = new SchemaId { RegistryName = "ver-reg", SchemaName = "ver-schema" },
            SchemaDefinition = "{\"version\":2}",
        });

        resp.SchemaVersionId.ShouldNotBeEmpty();
        resp.VersionNumber.ShouldBe(2);

        var versions = await _glue.ListSchemaVersionsAsync(new ListSchemaVersionsRequest
        {
            SchemaId = new SchemaId { RegistryName = "ver-reg", SchemaName = "ver-schema" },
        });

        versions.Schemas.Count.ShouldBe(2);
    }

    [Fact]
    public async Task DeleteSchemaAndVerifyGone()
    {
        await _glue.CreateRegistryAsync(new CreateRegistryRequest { RegistryName = "del-schema-reg" });
        await _glue.CreateSchemaAsync(new CreateSchemaRequest
        {
            RegistryId = new RegistryId { RegistryName = "del-schema-reg" },
            SchemaName = "del-schema",
            DataFormat = DataFormat.AVRO,
            SchemaDefinition = "{}",
        });

        await _glue.DeleteSchemaAsync(new DeleteSchemaRequest
        {
            SchemaId = new SchemaId { RegistryName = "del-schema-reg", SchemaName = "del-schema" },
        });

        await Should.ThrowAsync<EntityNotFoundException>(() =>
            _glue.GetSchemaAsync(new GetSchemaRequest
            {
                SchemaId = new SchemaId { RegistryName = "del-schema-reg", SchemaName = "del-schema" },
            }));
    }

    // -- GetCrawlers (list all) ------------------------------------------------

    [Fact]
    public async Task GetCrawlersReturnsList()
    {
        await _glue.CreateCrawlerAsync(new CreateCrawlerRequest
        {
            Name = "gc-a",
            Role = "role",
            Targets = new CrawlerTargets { S3Targets = [new S3Target { Path = "s3://b/a/" }] },
        });

        await _glue.CreateCrawlerAsync(new CreateCrawlerRequest
        {
            Name = "gc-b",
            Role = "role",
            Targets = new CrawlerTargets { S3Targets = [new S3Target { Path = "s3://b/b/" }] },
        });

        var resp = await _glue.GetCrawlersAsync(new GetCrawlersRequest());
        (resp.Crawlers.Count >= 2).ShouldBe(true);
    }

    // -- GetJobs (list all) ----------------------------------------------------

    [Fact]
    public async Task GetJobsReturnsList()
    {
        await _glue.CreateJobAsync(new CreateJobRequest
        {
            Name = "gj-a",
            Role = "role",
            Command = new JobCommand { Name = "glueetl", ScriptLocation = "s3://b/s.py" },
        });

        await _glue.CreateJobAsync(new CreateJobRequest
        {
            Name = "gj-b",
            Role = "role",
            Command = new JobCommand { Name = "glueetl", ScriptLocation = "s3://b/s.py" },
        });

        var resp = await _glue.GetJobsAsync(new GetJobsRequest());
        (resp.Jobs.Count >= 2).ShouldBe(true);
    }

    // -- BatchStopJobRun -------------------------------------------------------

    [Fact]
    public async Task BatchStopJobRunNotFoundReturnsErrors()
    {
        await _glue.CreateJobAsync(new CreateJobRequest
        {
            Name = "bsjr-job",
            Role = "role",
            Command = new JobCommand { Name = "glueetl", ScriptLocation = "s3://b/s.py" },
        });

        var resp = await _glue.BatchStopJobRunAsync(new BatchStopJobRunRequest
        {
            JobName = "bsjr-job",
            JobRunIds = ["nonexistent-run-id"],
        });

        resp.Errors.ShouldHaveSingleItem();
    }
}
