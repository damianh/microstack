---
title: Secrets Manager
description: Secrets Manager emulation — secrets with versioning, rotation staging, batch retrieval, resource policies.
order: 13
section: Services
---

# Secrets Manager

MicroStack's Secrets Manager handler supports the full lifecycle of secrets including string and binary values, version staging, batch retrieval, resource policies, and replication metadata.

## Supported Operations

- CreateSecret, GetSecretValue, BatchGetSecretValue, ListSecrets, DescribeSecret
- DeleteSecret, RestoreSecret, UpdateSecret, PutSecretValue
- UpdateSecretVersionStage, ListSecretVersionIds
- RotateSecret, GetRandomPassword
- PutResourcePolicy, GetResourcePolicy, DeleteResourcePolicy, ValidateResourcePolicy
- ReplicateSecretToRegions
- TagResource, UntagResource

## Usage

```csharp
var sm = new AmazonSecretsManagerClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonSecretsManagerConfig { ServiceURL = "http://localhost:4566" });

// Create a secret
var created = await sm.CreateSecretAsync(new CreateSecretRequest
{
    Name = "myapp/database",
    SecretString = """{"username":"admin","password":"hunter2"}""",
});

Console.WriteLine(created.ARN);
Console.WriteLine(created.VersionId);

// Retrieve the secret
var secret = await sm.GetSecretValueAsync(new GetSecretValueRequest
{
    SecretId = "myapp/database",
});

Console.WriteLine(secret.SecretString);
// Output: {"username":"admin","password":"hunter2"}
```

## Versioning and Rotation Staging

```csharp
// Put a new version — old version gets AWSPREVIOUS, new version gets AWSCURRENT
await sm.PutSecretValueAsync(new PutSecretValueRequest
{
    SecretId = "myapp/database",
    SecretString = """{"username":"admin","password":"newpass"}""",
    VersionStages = ["AWSCURRENT"],
});

// Retrieve a specific version stage
var current = await sm.GetSecretValueAsync(new GetSecretValueRequest
{
    SecretId = "myapp/database",
    VersionStage = "AWSCURRENT",
});

// List all versions
var versions = await sm.ListSecretVersionIdsAsync(new ListSecretVersionIdsRequest
{
    SecretId = "myapp/database",
});

foreach (var v in versions.Versions)
{
    Console.WriteLine($"{v.VersionId}: {string.Join(", ", v.VersionStages)}");
}
```

## Batch Retrieval

```csharp
// Retrieve multiple secrets in a single call
var batch = await sm.BatchGetSecretValueAsync(new BatchGetSecretValueRequest
{
    SecretIdList = ["myapp/database", "myapp/api-key"],
});

foreach (var result in batch.SecretValues)
{
    Console.WriteLine($"{result.Name}: {result.SecretString}");
}
```

:::aside{type="note" title="Rotation"}
`RotateSecret` is accepted and updates the rotation configuration, but does not invoke a Lambda function — rotation logic is not executed. Secrets must be rotated manually via `PutSecretValue`.
:::
