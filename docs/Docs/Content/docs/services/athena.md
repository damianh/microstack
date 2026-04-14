---
title: Athena
description: Athena emulation — query execution, work groups, named queries, data catalogs, and prepared statements.
order: 31
section: Services
---

# Athena

MicroStack emulates Amazon Athena's query management plane. Queries complete synchronously and immediately with status `SUCCEEDED`. Simple `SELECT` projections return parsed column names and values in the result set. Work groups, named queries, data catalogs, and prepared statements are fully managed in memory.

## Supported Operations

**Query Execution:** StartQueryExecution, GetQueryExecution, GetQueryResults, StopQueryExecution, ListQueryExecutions, BatchGetQueryExecution

**Work Groups:** CreateWorkGroup, GetWorkGroup, ListWorkGroups, UpdateWorkGroup, DeleteWorkGroup

**Named Queries:** CreateNamedQuery, GetNamedQuery, ListNamedQueries, DeleteNamedQuery, BatchGetNamedQuery

**Data Catalogs:** CreateDataCatalog, GetDataCatalog, ListDataCatalogs, UpdateDataCatalog, DeleteDataCatalog

**Prepared Statements:** CreatePreparedStatement, GetPreparedStatement, ListPreparedStatements, DeletePreparedStatement

**Table Metadata (stubs):** GetTableMetadata, ListTableMetadata, GetDatabase, ListDatabases

**Tags:** TagResource, UntagResource, ListTagsForResource

## Usage

```csharp
using Amazon.Athena;
using Amazon.Athena.Model;
using Amazon.Runtime;

var config = new AmazonAthenaConfig
{
    ServiceURL = "http://localhost:4566",
};
using var client = new AmazonAthenaClient(
    new BasicAWSCredentials("test", "test"), config);

// Execute a query
var resp = await client.StartQueryExecutionAsync(new StartQueryExecutionRequest
{
    QueryString = "SELECT 42 AS answer, 'hello' AS greeting",
    QueryExecutionContext = new QueryExecutionContext { Database = "default" },
    ResultConfiguration = new ResultConfiguration
    {
        OutputLocation = "s3://my-athena-results/",
    },
});

var queryId = resp.QueryExecutionId;

// Check status (completes synchronously — always SUCCEEDED)
var status = await client.GetQueryExecutionAsync(new GetQueryExecutionRequest
{
    QueryExecutionId = queryId,
});
Console.WriteLine($"State: {status.QueryExecution.Status.State}");
// => SUCCEEDED

// Fetch results
var results = await client.GetQueryResultsAsync(new GetQueryResultsRequest
{
    QueryExecutionId = queryId,
});

// Row 0 = column headers, Row 1+ = data rows
foreach (var row in results.ResultSet.Rows)
{
    Console.WriteLine(string.Join(", ", row.Data.Select(d => d.VarCharValue)));
}
// => answer, greeting
// => 42, hello
```

## Work Groups

```csharp
// Create a work group with a result configuration
await client.CreateWorkGroupAsync(new CreateWorkGroupRequest
{
    Name = "analytics",
    Description = "Analytics team queries",
    Configuration = new WorkGroupConfiguration
    {
        ResultConfiguration = new ResultConfiguration
        {
            OutputLocation = "s3://my-athena-results/analytics/",
        },
    },
});

// Run a query scoped to the work group
var q = await client.StartQueryExecutionAsync(new StartQueryExecutionRequest
{
    QueryString = "SELECT * FROM events LIMIT 100",
    WorkGroup = "analytics",
    QueryExecutionContext = new QueryExecutionContext { Database = "my_db" },
});

// List all query executions in the work group
var executions = await client.ListQueryExecutionsAsync(new ListQueryExecutionsRequest
{
    WorkGroup = "analytics",
});
Console.WriteLine($"Executions: {executions.QueryExecutionIds.Count}");
```

## Named Queries and Prepared Statements

```csharp
// Save a named query for reuse
var nq = await client.CreateNamedQueryAsync(new CreateNamedQueryRequest
{
    Name = "top-events",
    Database = "my_db",
    WorkGroup = "primary",
    QueryString = "SELECT event_type, COUNT(*) AS cnt FROM events GROUP BY event_type ORDER BY cnt DESC LIMIT 10",
    Description = "Top event types",
});

Console.WriteLine($"Named query ID: {nq.NamedQueryId}");

// Create a prepared statement (parameterized query)
await client.CreatePreparedStatementAsync(new CreatePreparedStatementRequest
{
    StatementName = "get_by_id",
    WorkGroup = "primary",
    QueryStatement = "SELECT * FROM events WHERE id = ?",
});
```

:::aside{type="note" title="Query Execution Behavior"}
All queries complete immediately with `SUCCEEDED` status — there is no async polling needed. Simple `SELECT` column projections (e.g., `SELECT 42 AS num, 'hello' AS name`) return parsed column names and values. Complex SQL against real tables is not executed. The `primary` work group and `AwsDataCatalog` data catalog exist by default and cannot be deleted.
:::
