---
title: Internal API
description: MicroStack's built-in health, resource inspection, reset, and config endpoints.
order: 5
section: Guides
---

# Internal API

MicroStack exposes internal endpoints for health checking, state management, and runtime configuration. These are especially useful in CI pipelines and test suites.

## Browser Access

Admin endpoints allow cross-origin browser requests from
`http://<MICROSTACK_HOST>:<MICROSTACK_UI_PORT>` (default `http://localhost:4567`).
The policy covers health checks and aliases, resources, request logs, reset, and
configuration, including preflight requests for `GET`, `POST`, and `DELETE`.
Set these environment variables on the API server to match the UI's browser origin;
the UI host must use the same `MICROSTACK_UI_PORT`.

Other origins do not receive permission to read admin responses. CORS is not
authentication and does not prevent non-browser access or all cross-origin writes;
keep the emulator on a trusted development network. AWS service endpoints retain
their existing permissive CORS behavior for SDK clients.

## Resource Inspection

The admin UI uses the versioned, read-only resource inspection API under
`/_microstack/admin/v1`. It exposes the live state retained by enabled MicroStack
service handlers; it does not create separate service instances or execute AWS
data-plane operations to discover resources.

```bash
# Configured region, default account, and API capabilities
curl http://localhost:4566/_microstack/admin/v1/context

# Declared service catalog and current availability
curl "http://localhost:4566/_microstack/admin/v1/services?accountId=000000000000"

# First page of S3 resources
curl "http://localhost:4566/_microstack/admin/v1/services/s3/resources?accountId=000000000000&limit=50"
```

The service endpoints are:

| Endpoint | Description |
|----------|-------------|
| `GET /services/{serviceId}/resources` | Page through root resources, optionally by resource kind |
| `GET /services/{serviceId}/resource` | Read resource metadata and available inspection capabilities |
| `GET /services/{serviceId}/children` | Page through a retained child collection |
| `GET /services/{serviceId}/content` | Read bounded text or JSON content, or metadata for binary/oversized content |
| `GET /services/{serviceId}/connections` | Read explicitly configured cross-service relationships |
| `POST /services/{serviceId}/reveal` | Reveal a supported retained sensitive value on demand |
| `GET /services/{serviceId}/activity` | Read service- and account-scoped request activity |

Append these paths to `/_microstack/admin/v1`. Resource and child identities are
opaque JSON paths passed in the `path` query parameter. Clients should use the
paths returned by the API rather than constructing or splitting identifiers.

Lists default to 50 entries and accept `limit` values from 1 through 200.
Continuation tokens are opaque and scoped to the service, account, kind, and
parent path; a token cannot be reused in another scope. The optional `accountId`
must contain exactly 12 ASCII digits and defaults to the configured account.

Text and JSON previews are limited to 1 MiB of UTF-8 data. Binary and larger
values remain metadata-only. Known secrets are masked in ordinary responses.
Only explicitly supported existing values can be revealed; reveal and content
responses use `Cache-Control: no-store`. Private cryptographic key material is
never exposed.

Errors use a structured JSON response with a stable `code`, a human-readable
`message`. A missing resource returns `404`; invalid arguments and continuation
tokens return `400`; disabled services and unavailable inspection capabilities
return `409`.

The API is intended for trusted local development. It has no authentication, and
account selection and secret reveal make exposing it on an untrusted network
especially unsafe.

## Health Check

```bash
curl http://localhost:4566/_microstack/health
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
curl -X POST http://localhost:4566/_microstack/reset
```

Wipes all in-memory state across every service. Returns `200 OK` on success.

This is the recommended way to get a clean environment between test runs without restarting the container. Call it in `setUp` / `beforeEach` / `InitializeAsync`:

```csharp
public async Task InitializeAsync()
{
    using var http = new HttpClient { BaseAddress = new Uri(connectionString) };
    await http.PostAsync("/_microstack/reset", null);
}
```

If state persistence is enabled (`PERSIST_STATE=1`), reset also deletes the persisted state files.

## Runtime Config

```bash
curl -X POST http://localhost:4566/_microstack/config \
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
  -e MICROSTACK_REGION=eu-west-1 \
  -e MICROSTACK_ACCOUNT_ID=123456789012 \
  ghcr.io/damianh/microstack:latest
```

Or use the [multi-tenancy](/architecture/multi-tenancy) feature — a 12-digit access key automatically becomes the account ID.
