---
title: DynamoDB
description: DynamoDB emulation — tables, items, queries, scans, transactions, GSIs, streams.
order: 4
section: Services
---

# DynamoDB

Full DynamoDB emulation including hash/range keys, global secondary indexes, conditional expressions, transactions, and DynamoDB Streams.

## Supported Operations

- CreateTable, DeleteTable, DescribeTable, ListTables, UpdateTable
- PutItem, GetItem, DeleteItem, UpdateItem
- Query, Scan (with filters, projections, pagination)
- BatchWriteItem, BatchGetItem
- TransactWriteItems, TransactGetItems
- DescribeContinuousBackups, DescribeTimeToLive, UpdateTimeToLive
- TagResource, UntagResource, ListTagsOfResource

## Usage

```csharp
var ddb = new AmazonDynamoDBClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonDynamoDBConfig { ServiceURL = "http://localhost:4566" });

// Create table
await ddb.CreateTableAsync(new CreateTableRequest
{
    TableName = "users",
    KeySchema = [new KeySchemaElement("pk", KeyType.HASH)],
    AttributeDefinitions = [new AttributeDefinition("pk", ScalarAttributeType.S)],
    BillingMode = BillingMode.PAY_PER_REQUEST,
});

// Put item
await ddb.PutItemAsync("users", new Dictionary<string, AttributeValue>
{
    ["pk"] = new() { S = "user-1" },
    ["name"] = new() { S = "Alice" },
    ["age"] = new() { N = "30" },
});

// Get item
var item = await ddb.GetItemAsync("users", new Dictionary<string, AttributeValue>
{
    ["pk"] = new() { S = "user-1" },
});

Console.WriteLine(item.Item["name"].S); // Alice
```

## Transactions

```csharp
await ddb.TransactWriteItemsAsync(new TransactWriteItemsRequest
{
    TransactItems =
    [
        new TransactWriteItem
        {
            Put = new Put
            {
                TableName = "users",
                Item = new Dictionary<string, AttributeValue>
                {
                    ["pk"] = new() { S = "user-2" },
                    ["name"] = new() { S = "Bob" },
                },
            },
        },
    ],
});
```
