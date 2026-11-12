---
title: RDS Data API
description: RDS Data API emulation backed by per-cluster in-memory SQLite for ExecuteStatement, BatchExecuteStatement, and transactions.
order: 38
section: Services
---

# RDS Data API

MicroStack emulates the RDS Data API (Aurora Serverless Data API) with an in-memory SQLite database for each emulated RDS DB cluster. Create a cluster with the RDS API first, then use its `DBClusterArn` as `resourceArn`. Each cluster has an isolated database, and its contents are cleared when the cluster is deleted, MicroStack is reset, or the MicroStack process restarts. Stopping and starting an RDS cluster does not clear its data.

## Supported Operations

`ExecuteStatement`, `BatchExecuteStatement`, `BeginTransaction`, `CommitTransaction`, and `RollbackTransaction` are supported. Use one SQLite SQL statement per `ExecuteStatement` call. Query results are returned in Data API `records`; write statements return `numberOfRecordsUpdated`. Batch execution accepts one non-query SQL statement and a non-empty `parameterSets` array, runs the sets atomically in its own transaction, and returns one `updateResults` entry per set. Batch requests with `transactionId` are not supported.

The local SQL dialect is SQLite regardless of the emulated cluster's AWS `Engine` value. This is not a PostgreSQL or MySQL server: engine-specific syntax and behavior are not emulated. The `database` request field is ignored because each cluster has one SQLite database. `secretArn` is required but is not resolved or used for database authentication.

Named parameters use SQLite `:name` placeholders with alphanumeric or underscore names. `parameters` values support `stringValue`, `longValue`, `doubleValue`, `booleanValue`, `isNull: true`, and base64-encoded `blobValue`. Result values map to Data API string, long, double, null, and blob fields; SQLite booleans are stored as integers and returned as `longValue` 0 or 1. SQLite's own type conversion and SQL semantics apply.

## Usage

The RDS Data API uses a REST/JSON protocol (not the standard AWS Query protocol), so all requests are plain JSON `POST` calls to path-based endpoints. Use `AmazonRDSDataServiceClient` as you would with AWS; MicroStack executes statements locally using the SQLite compatibility described above.

```csharp
using Amazon.Runtime;
using Amazon.RDSDataService;
using Amazon.RDSDataService.Model;

var config = new AmazonRDSDataServiceConfig
{
    ServiceURL = "http://localhost:4566",
};

using var rdsData = new AmazonRDSDataServiceClient(
    new BasicAWSCredentials("test", "test"), config);

// ExecuteStatement — the DB cluster must first be provisioned through the RDS API.
var result = await rdsData.ExecuteStatementAsync(new ExecuteStatementRequest
{
    ResourceArn = "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
    SecretArn   = "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
    Sql         = "SELECT id, name FROM users WHERE active = :active",
    Parameters  =
    [
        new SqlParameter
        {
            Name  = "active",
            Value = new Field { BooleanValue = true },
        },
    ],
    Database = "mydb",
});

Console.WriteLine($"Rows affected: {result.NumberOfRecordsUpdated}");
```

## Transactions

```csharp
// Begin a transaction
var txnResp = await rdsData.BeginTransactionAsync(new BeginTransactionRequest
{
    ResourceArn = "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
    SecretArn   = "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
    Database    = "mydb",
});
var transactionId = txnResp.TransactionId;

// Execute within the transaction
await rdsData.ExecuteStatementAsync(new ExecuteStatementRequest
{
    ResourceArn   = "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
    SecretArn     = "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
    Sql           = "INSERT INTO orders (id, amount) VALUES (:id, :amount)",
    TransactionId = transactionId,
    Parameters    =
    [
        new SqlParameter { Name = "id",     Value = new Field { LongValue = 42 } },
        new SqlParameter { Name = "amount", Value = new Field { DoubleValue = 99.99 } },
    ],
});

// Commit
await rdsData.CommitTransactionAsync(new CommitTransactionRequest
{
    ResourceArn   = "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
    SecretArn     = "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
    TransactionId = transactionId,
});
```

:::aside{type="note" title="SQLite compatibility and limits"}
MicroStack executes SQL locally with SQLite; it does not connect to Aurora or any external database. Statements use SQLite syntax and type behavior, with one database per cluster held in memory. SQL errors and malformed or unsupported parameter values return `400 BadRequest`; a cluster ARN that has not been provisioned returns `400 BadRequest`. `CommitTransaction` and `RollbackTransaction` require a valid transaction ID and return `404 Not Found` if the transaction does not exist. Batch statements must not return rows.
:::
