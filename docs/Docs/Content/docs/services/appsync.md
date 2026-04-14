---
title: AppSync
description: AppSync emulation — GraphQL API management with types, resolvers, data sources, and API keys.
order: 29
section: Services
---

# AppSync

MicroStack emulates the AWS AppSync management plane, supporting GraphQL API lifecycle management including types, resolvers, data sources, and API keys. The GraphQL data plane (executing queries/mutations) is not emulated — only the control-plane SDK operations are supported.

## Supported Operations

**GraphQL APIs:** CreateGraphqlApi, GetGraphqlApi, ListGraphqlApis, UpdateGraphqlApi, DeleteGraphqlApi

**API Keys:** CreateApiKey, ListApiKeys, DeleteApiKey

**Data Sources:** CreateDataSource, GetDataSource, ListDataSources, DeleteDataSource

**Resolvers:** CreateResolver, GetResolver, ListResolvers, DeleteResolver

**Types:** CreateType, GetType, ListTypes

**Tags:** TagResource, UntagResource, ListTagsForResource

## Usage

```csharp
using Amazon.AppSync;
using Amazon.AppSync.Model;
using Amazon.Runtime;

var config = new AmazonAppSyncConfig
{
    ServiceURL = "http://localhost:4566",
};
using var client = new AmazonAppSyncClient(
    new BasicAWSCredentials("test", "test"), config);

// Create a GraphQL API
var resp = await client.CreateGraphqlApiAsync(new CreateGraphqlApiRequest
{
    Name = "my-api",
    AuthenticationType = AuthenticationType.API_KEY,
});

var apiId = resp.GraphqlApi.ApiId;
Console.WriteLine($"API ID: {apiId}");

// Create an API key
var keyResp = await client.CreateApiKeyAsync(new CreateApiKeyRequest
{
    ApiId = apiId,
});
Console.WriteLine($"API Key: {keyResp.ApiKey.Id}");

// List all GraphQL APIs
var listResp = await client.ListGraphqlApisAsync(new ListGraphqlApisRequest());
foreach (var api in listResp.GraphqlApis)
{
    Console.WriteLine($"  {api.ApiId}: {api.Name}");
}
```

## Data Sources and Resolvers

```csharp
// Create a data source backed by DynamoDB
var dsResp = await client.CreateDataSourceAsync(new CreateDataSourceRequest
{
    ApiId = apiId,
    Name = "ItemsDataSource",
    Type = DataSourceType.AMAZON_DYNAMODB,
    DynamodbConfig = new DynamodbDataSourceConfig
    {
        TableName = "items-table",
        AwsRegion = "us-east-1",
    },
});

// Create a resolver that maps a GraphQL field to the data source
var resResp = await client.CreateResolverAsync(new CreateResolverRequest
{
    ApiId = apiId,
    TypeName = "Query",
    FieldName = "getItem",
    DataSourceName = dsResp.DataSource.Name,
});

Console.WriteLine($"Resolver field: {resResp.Resolver.FieldName}");

// Define a GraphQL type
var typeResp = await client.CreateTypeAsync(new CreateTypeRequest
{
    ApiId = apiId,
    Definition = "type Item { id: ID! name: String }",
    Format = TypeDefinitionFormat.SDL,
});
Console.WriteLine($"Type: {typeResp.Type.Name}");
```

:::aside{type="note" title="Management Plane Only"}
MicroStack emulates the AppSync control plane (API management) only. GraphQL query execution against the `/graphql` endpoint is not supported. Use the SDK operations above to create and configure APIs; test your schema and resolvers against a real AppSync endpoint or a dedicated GraphQL testing tool.
:::
