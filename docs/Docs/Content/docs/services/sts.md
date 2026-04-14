---
title: STS
description: STS emulation — caller identity, role assumption, session tokens, web identity.
order: 10
section: Services
---

# STS

MicroStack's STS handler supports all common credential-vending operations. All calls return synthetic but structurally valid credentials — access key IDs, secret keys, and session tokens are generated on the fly.

## Supported Operations

- GetCallerIdentity, GetAccessKeyInfo
- AssumeRole, AssumeRoleWithWebIdentity
- GetSessionToken

## Usage

```csharp
var sts = new AmazonSecurityTokenServiceClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonSecurityTokenServiceConfig { ServiceURL = "http://localhost:4566" });

// Get caller identity
var identity = await sts.GetCallerIdentityAsync(new GetCallerIdentityRequest());
Console.WriteLine(identity.Account);   // 000000000000
Console.WriteLine(identity.Arn);
Console.WriteLine(identity.UserId);
```

## Assuming a Role

```csharp
var resp = await sts.AssumeRoleAsync(new AssumeRoleRequest
{
    RoleArn = "arn:aws:iam::000000000000:role/my-role",
    RoleSessionName = "my-session",
    DurationSeconds = 900,
});

// Use the temporary credentials
var tempCredentials = new SessionAWSCredentials(
    resp.Credentials.AccessKeyId,
    resp.Credentials.SecretAccessKey,
    resp.Credentials.SessionToken);

Console.WriteLine(resp.AssumedRoleUser.Arn);
```

## Session Tokens

```csharp
var session = await sts.GetSessionTokenAsync(new GetSessionTokenRequest
{
    DurationSeconds = 900,
});

Console.WriteLine(session.Credentials.AccessKeyId);
Console.WriteLine(session.Credentials.Expiration);
```

:::aside{type="note" title="Synthetic credentials"}
STS does not enforce IAM policies. All `AssumeRole` calls succeed regardless of the role ARN provided — the role does not need to exist in IAM.
:::
