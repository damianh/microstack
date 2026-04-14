---
title: Glue
description: Glue emulation — data catalog with databases, tables, partitions, crawlers, ETL jobs, and schema registry.
order: 30
section: Services
---

# Glue

MicroStack emulates AWS Glue's data catalog and job management plane, supporting databases, tables, partitions, crawlers, ETL job runs, and the schema registry. All state is stored in memory; no actual ETL execution or crawling occurs.

## Supported Operations

**Databases:** CreateDatabase, GetDatabase, GetDatabases, UpdateDatabase, DeleteDatabase

**Tables:** CreateTable, GetTable, GetTables, UpdateTable, DeleteTable, BatchDeleteTable, BatchGetTable

**Partitions:** CreatePartition, GetPartition, GetPartitions, UpdatePartition, DeletePartition, BatchCreatePartition, BatchDeletePartition

**Crawlers:** CreateCrawler, GetCrawler, GetCrawlers, UpdateCrawler, DeleteCrawler, StartCrawler, StopCrawler, GetCrawlerMetrics

**Jobs:** CreateJob, GetJob, GetJobs, UpdateJob, DeleteJob, StartJobRun, GetJobRun, GetJobRuns, BatchStopJobRun

**Schema Registry:** CreateRegistry, GetRegistry, ListRegistries, DeleteRegistry, CreateSchema, GetSchema, ListSchemas, DeleteSchema, RegisterSchemaVersion, GetSchemaVersion, ListSchemaVersions

**Tags:** TagResource, UntagResource, GetTags

## Usage

```csharp
using Amazon.Glue;
using Amazon.Glue.Model;
using Amazon.Runtime;

var config = new AmazonGlueConfig
{
    ServiceURL = "http://localhost:4566",
};
using var client = new AmazonGlueClient(
    new BasicAWSCredentials("test", "test"), config);

// Create a database
await client.CreateDatabaseAsync(new CreateDatabaseRequest
{
    DatabaseInput = new DatabaseInput
    {
        Name = "my_database",
        Description = "Data lake catalog",
    },
});

// Create a table in the database
await client.CreateTableAsync(new CreateTableRequest
{
    DatabaseName = "my_database",
    TableInput = new TableInput
    {
        Name = "events",
        TableType = "EXTERNAL_TABLE",
        StorageDescriptor = new StorageDescriptor
        {
            Columns =
            [
                new Column { Name = "id", Type = "int" },
                new Column { Name = "event_type", Type = "string" },
                new Column { Name = "timestamp", Type = "timestamp" },
            ],
            Location = "s3://my-data-lake/events/",
            InputFormat = "org.apache.hadoop.mapred.TextInputFormat",
            OutputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            SerdeInfo = new SerDeInfo { SerializationLibrary = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" },
        },
    },
});

var table = await client.GetTableAsync(new GetTableRequest
{
    DatabaseName = "my_database",
    Name = "events",
});
Console.WriteLine($"Table: {table.Table.Name}, columns: {table.Table.StorageDescriptor.Columns.Count}");
```

## Crawlers and ETL Jobs

```csharp
// Create and start a crawler
await client.CreateCrawlerAsync(new CreateCrawlerRequest
{
    Name = "s3-crawler",
    Role = "arn:aws:iam::000000000000:role/GlueRole",
    DatabaseName = "my_database",
    Targets = new CrawlerTargets
    {
        S3Targets = [new S3Target { Path = "s3://my-data-lake/events/" }],
    },
});

await client.StartCrawlerAsync(new StartCrawlerRequest { Name = "s3-crawler" });

var crawlerState = await client.GetCrawlerAsync(new GetCrawlerRequest { Name = "s3-crawler" });
Console.WriteLine($"Crawler state: {crawlerState.Crawler.State}");
// => RUNNING

// Create and start an ETL job
await client.CreateJobAsync(new CreateJobRequest
{
    Name = "transform-job",
    Role = "arn:aws:iam::000000000000:role/GlueRole",
    Command = new JobCommand
    {
        Name = "glueetl",
        ScriptLocation = "s3://my-scripts/transform.py",
    },
    GlueVersion = "3.0",
});

var run = await client.StartJobRunAsync(new StartJobRunRequest { JobName = "transform-job" });
Console.WriteLine($"Job run ID: {run.JobRunId}");
```

## Schema Registry

```csharp
// Create a schema registry
await client.CreateRegistryAsync(new CreateRegistryRequest
{
    RegistryName = "my-registry",
    Description = "Avro schema registry",
});

// Register a schema
await client.CreateSchemaAsync(new CreateSchemaRequest
{
    RegistryId = new RegistryId { RegistryName = "my-registry" },
    SchemaName = "user-event",
    DataFormat = DataFormat.AVRO,
    SchemaDefinition = """{"type":"record","name":"UserEvent","fields":[{"name":"id","type":"string"}]}""",
});

// Register a new version
var version = await client.RegisterSchemaVersionAsync(new RegisterSchemaVersionRequest
{
    SchemaId = new SchemaId { RegistryName = "my-registry", SchemaName = "user-event" },
    SchemaDefinition = """{"type":"record","name":"UserEvent","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"}]}""",
});
Console.WriteLine($"Version: {version.VersionNumber}");
// => 2
```
