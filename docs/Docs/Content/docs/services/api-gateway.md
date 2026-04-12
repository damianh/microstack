---
title: API Gateway
description: API Gateway v1 and v2 emulation with Lambda proxy and mock integrations.
order: 6
section: Services
---

# API Gateway

MicroStack supports both API Gateway v2 (HTTP APIs) and v1 (REST APIs) with data plane request dispatching.

## v2 — HTTP APIs

```csharp
var apigw = new AmazonApiGatewayV2Client(
    new BasicAWSCredentials("test", "test"),
    new AmazonApiGatewayV2Config { ServiceURL = "http://localhost:4566" });

var api = await apigw.CreateApiAsync(new CreateApiRequest
{
    Name = "my-api",
    ProtocolType = ProtocolType.HTTP,
});
```

## v1 — REST APIs

Supports resources, methods, MOCK integrations with response templates, and AWS_PROXY Lambda integrations.

## Data Plane

Invoke APIs via the execute-api hostname pattern:

```
http://{apiId}.execute-api.localhost:4566/{stage}/{path}
```
