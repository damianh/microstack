---
title: RDS Data API
description: RDS Data API emulation — validates request parameters and routing for ExecuteStatement, BatchExecuteStatement, BeginTransaction, CommitTransaction, and RollbackTransaction.
order: 38
section: Services
---

# RDS Data API

MicroStack emulates the RDS Data API (Aurora Serverless Data API) for parameter validation and request routing. The handler accepts the five standard REST/JSON endpoints and validates required fields, returning appropriate error responses. Because there is no live database engine behind the stub, statements directed at a cluster ARN that has not been provisioned return a "cluster not found" error.

## Supported Operations

ExecuteStatement, BatchExecuteStatement, BeginTransaction, CommitTransaction, RollbackTransaction

## Usage

The RDS Data API uses a REST/JSON protocol (not the standard AWS Query protocol), so all requests are plain JSON `POST` calls to path-based endpoints. In production you would use `AmazonRDSDataServiceClient`; MicroStack validates the same required fields.

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

// ExecuteStatement — requires resourceArn, secretArn, and sql
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

:::aside{type="note" title="Validation-only stub"}
MicroStack's RDS Data handler validates all required fields (`resourceArn`, `secretArn`, `sql`) and returns `400 BadRequest` for missing parameters. Requests targeting a cluster ARN that does not exist return `400 BadRequest` with a "cluster not found" message — real SQL execution is not performed. `CommitTransaction` and `RollbackTransaction` require a valid `transactionId` and return `404 Not Found` if the transaction does not exist.
:::
