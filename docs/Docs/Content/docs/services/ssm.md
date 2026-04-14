---
title: SSM Parameter Store
description: SSM Parameter Store emulation — parameters with versioning, hierarchical paths, labels, encryption.
order: 14
section: Services
---

# SSM Parameter Store

MicroStack's SSM handler supports String, SecureString, and StringList parameter types with hierarchical path-based organization, versioning, labels, history, and tagging.

## Supported Operations

- PutParameter, GetParameter, GetParameters, GetParametersByPath
- DeleteParameter, DeleteParameters
- DescribeParameters, GetParameterHistory, LabelParameterVersion
- AddTagsToResource, RemoveTagsFromResource, ListTagsForResource

## Usage

```csharp
var ssm = new AmazonSimpleSystemsManagementClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonSimpleSystemsManagementConfig { ServiceURL = "http://localhost:4566" });

// Store a parameter
await ssm.PutParameterAsync(new PutParameterRequest
{
    Name = "/myapp/database/host",
    Value = "localhost",
    Type = ParameterType.String,
});

// Retrieve a parameter
var param = await ssm.GetParameterAsync(new GetParameterRequest
{
    Name = "/myapp/database/host",
});

Console.WriteLine(param.Parameter.Value); // localhost
```

## Hierarchical Path Retrieval

```csharp
// Store multiple parameters under a common path
await ssm.PutParameterAsync(new PutParameterRequest
{
    Name = "/myapp/config/db-host",
    Value = "db.example.com",
    Type = ParameterType.String,
});

await ssm.PutParameterAsync(new PutParameterRequest
{
    Name = "/myapp/config/db-port",
    Value = "5432",
    Type = ParameterType.String,
});

// Retrieve all parameters under a path
var result = await ssm.GetParametersByPathAsync(new GetParametersByPathRequest
{
    Path = "/myapp/config",
    Recursive = true,
});

foreach (var p in result.Parameters)
{
    Console.WriteLine($"{p.Name} = {p.Value}");
}
```

## SecureString Parameters

```csharp
// Store as SecureString (stored in plain text in MicroStack — no actual encryption)
await ssm.PutParameterAsync(new PutParameterRequest
{
    Name = "/myapp/secrets/api-key",
    Value = "my-secret-api-key",
    Type = ParameterType.SecureString,
});

// WithDecryption=true is accepted but values are never encrypted at rest
var secure = await ssm.GetParameterAsync(new GetParameterRequest
{
    Name = "/myapp/secrets/api-key",
    WithDecryption = true,
});

Console.WriteLine(secure.Parameter.Value); // my-secret-api-key
```

:::aside{type="note" title="SecureString encryption"}
SecureString parameters are stored in plain text in MicroStack. The `WithDecryption` flag is accepted but has no effect — values are always returned as-is.
:::
