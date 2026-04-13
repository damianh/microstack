---
title: Internal API
description: MicroStack's built-in health, reset, and config endpoints for test automation.
order: 5
section: Guides
---

# Internal API

MicroStack exposes internal endpoints for health checking, state management, and runtime configuration. These are especially useful in CI pipelines and test suites.

## Health Check

```bash
curl http://localhost:4566/_ministack/health
```

Returns JSON with service availability, edition, and version:

```json
{
  "services": {
    "sqs": "available",
    "s3": "available",
    "dynamodb": "available"
  },
  "edition": "light",
  "version": "0.1.0"
}
```

LocalStack-compatible aliases are also available:

```bash
curl http://localhost:4566/_localstack/health
curl http://localhost:4566/health
```

## Reset State

```bash
curl -X POST http://localhost:4566/_ministack/reset
```

Wipes all in-memory state across every service. Returns `200 OK` on success.

This is the recommended way to get a clean environment between test runs without restarting the container. Call it in `setUp` / `beforeEach` / `InitializeAsync`:

```csharp
public async Task InitializeAsync()
{
    using var http = new HttpClient { BaseAddress = new Uri(connectionString) };
    await http.PostAsync("/_ministack/reset", null);
}
```

If state persistence is enabled (`PERSIST_STATE=1`), reset also deletes the persisted state files.

## Runtime Config

```bash
curl -X POST http://localhost:4566/_ministack/config \
  -H "Content-Type: application/json" \
  -d '{"stepfunctions._sfn_mock_config": "{...}"}'
```

Change service-level settings without restarting. Supported keys:

| Key | Description |
|-----|-------------|
| `stepfunctions._sfn_mock_config` | Step Functions mock configuration (AWS SFN Local compatible) |

## Setting Region and Account

Region and account ID are set via environment variables at startup, not via the config endpoint:

```bash
docker run -p 4566:4566 \
  -e MINISTACK_REGION=eu-west-1 \
  -e MINISTACK_ACCOUNT_ID=123456789012 \
  ghcr.io/damianh/microstack:latest
```

Or use the [multi-tenancy](/architecture/multi-tenancy) feature — a 12-digit access key automatically becomes the account ID.
