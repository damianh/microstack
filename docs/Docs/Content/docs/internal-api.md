---
title: Internal API
description: MicroStack's built-in health, resource inspection, reset, and config endpoints.
order: 5
section: Guides
---

# Internal API

MicroStack exposes internal endpoints for health checking, state management, and runtime configuration. These are especially useful in CI pipelines and test suites.

## Browser Access

The UI and admin API share the gateway origin, so admin endpoints do not grant
cross-origin browser access. CORS is not authentication and does not prevent
non-browser access or all cross-origin writes; keep the emulator on a trusted
development network. AWS service endpoints retain their existing permissive CORS
behavior for SDK clients.

The UI is reserved at `/ui/` on the gateway hostname. An unsigned, query-free
browser navigation to `/` redirects there. Signed, presigned, SDK, non-HTML, and
service-specific-host requests retain their AWS routing behavior.

## Resource Inspection

The admin UI uses the versioned, read-only resource inspection API under
`/_microstack/admin/v1`. It exposes the live state retained by enabled MicroStack
service handlers; it does not create separate service instances or execute AWS
data-plane operations to discover resources.

```bash
# Configured region, default account, and API capabilities
curl http://localhost:4566/_microstack/admin/v1/context

# Known accounts with retained resources, plus the configured default
curl http://localhost:4566/_microstack/admin/v1/accounts

# Declared service catalog and current availability
curl "http://localhost:4566/_microstack/admin/v1/services?accountId=000000000000"

# First page of S3 resources
curl "http://localhost:4566/_microstack/admin/v1/services/s3/resources?accountId=000000000000&pageSize=50"
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

Lists default to 50 entries and accept `pageSize` values from 1 through 200.
Continuation tokens are opaque and scoped to the service, account, kind, and
parent path; a token cannot be reused in another scope. The optional `accountId`
must contain exactly 12 ASCII digits and defaults to the configured account.

### Known accounts

`GET /_microstack/admin/v1/accounts` returns a JSON string array, for example
`["000000000000","111111111111","222222222222"]`. IDs are distinct, sorted
ordinally, and contain exactly 12 ASCII digits. The configured default is always
included, even without resources; invalid `MICROSTACK_ACCOUNT_ID` configuration
is rejected at startup. The response uses `Cache-Control: no-store`.

Discovery reads explicit retained-resource ownership from enabled handlers,
including restored persistent state. It does not inspect request logs, remember
previously selected accounts, create resources, or scan resource payloads.
Reading an empty account does not add it. An account disappears once its last
retained resource is removed, or after reset, unless it is the configured default.
Retained execution/query resources can keep an account present; secondary
indexes, tags left behind after deletion, and empty child containers do not.
Concurrent mutations are not a transactionally simultaneous cross-service snapshot.
EC2's automatically seeded default VPC, subnets, security group, route table, and
internet gateway do not qualify on their own: the handler initializes these even
on read requests. User-created EC2 resources do qualify.

Instance-global resources have no account ownership and do not add accounts.
These include Firehose streams, RDS Data transactions, Athena workgroups/catalogs,
EventBridge buses/partner sources, and SES sent-email history. Other account-scoped
resources in those services still participate. Discovery does not add persistence
support to handlers that do not already implement it. The UI selects known
accounts only; there is no admin account-creation endpoint.

The canonical browser routes are `/ui/accounts/{accountId}/services` and
`/ui/accounts/{accountId}/services/{serviceId}`, with a 12-digit account ID.
Legacy `/ui/`, `/ui/resources`, and `/ui/services/{serviceId}` links redirect to
the account-scoped routes, using the legacy `account` query parameter when
present, otherwise the last explorer account in this tab or the configured default.
Redirects replace the current history entry and preserve the remaining
query-based inspection state. An unknown account in a URL prompts the user to
choose a known account rather than creating an account or querying its resources.
The account selector lives in the application header on explorer pages. With one
known account it displays plain text; with multiple accounts it switches immediately,
retaining the service but clearing resource and payload selections. Only the last
known selected account ID is saved in browser session storage, scoped to this origin
and tab; this is navigation context, not account discovery. Explicit account routes
and legacy account query parameters take precedence over saved context. A deleted
saved account requires an explicit known-account selection, not a silent fallback.

Overview and Request log remain instance-wide and show "Instance-wide" in the header
instead of a selector. Visiting or refreshing either page does not change the
remembered explorer account: Services and the brand link return to that account.
Overview's legacy resource summary still covers only the configured default account,
as labeled; its reset affects every account. Storage failures are shown explicitly;
in-memory account navigation still works when browser storage is unavailable.
These browser routes do not change the admin API's `accountId` query parameter or
AWS request scoping.

### Inspector metadata and filtering

The explorer uses the same inspection shell across all services, with
service-specific summaries where the handler retains the necessary data.
Service `kinds` include an `isRoot` flag: only root kinds belong in the resource
picker. Child kinds remain available through opaque resource paths. A detail's
`childKinds` describe its collections even when they are empty.

Resource summaries may supply a `type` and small `summary` fields for list rows.
Details may supply their own `summary` fields and a known `connectionCount`.
Missing metadata is not a zero count or a healthy status. Fields marked
`secondary` remain available as additional metadata; sensitive-field masking
and reveal restrictions are unchanged.

Resource and child lists accept `filter`, which matches retained names,
identifiers and ARNs, not payload bodies. The UI applies filters after a short
typing pause, or immediately on Enter or clear. Filtering resets the relevant
continuation token; it does not run an AWS Query, Scan, or ReceiveMessage.
Counts labeled as loaded describe the returned page, not the complete resource.
Provider summary counts describe retained state and may differ from a filtered
page. Snapshots can change between requests.

### Service-specific inspection

- **SQS:** queues show their retained type, settings and message-state counts.
  Message inspection is non-consuming: it does not receive messages, change
  visibility, increment receive counts or advance consumers.
- **S3:** prefixes are virtual groupings of object keys. Browsing preserves full
  object and version identities; previews do not automatically download binary
  or oversized objects.
- **DynamoDB:** item keys and JSON retain DynamoDB attribute types. Browsing
  modeled items is not execution of a DynamoDB Query or Scan.
- **SNS:** the topic view inspects subscriptions, not a retained message inbox.
  Subscription endpoints and confirmation/filter settings describe configuration.
- **EventBridge:** the inspector shows retained rules, patterns or schedules and
  targets. It does not manufacture event history or delivery traces.

### Configured connections

Connections describe configuration, not evidence of successful delivery or
processing. Topic and event-bus context can include destinations configured by
their subscriptions or rules. SQS context also includes same-account SNS
subscriptions, EventBridge targets and dead-letter source queues that explicitly
reference the queue. Matching names alone never establish a relationship.

Connections use full identities, preserve source resource paths, and are paged
in a stable order. External or missing destinations are not silently treated as
live inspectable resources. Supported reverse relationships do not constitute a
universal dependency graph across every service. Cross-provider reads are not
transactionally simultaneous. Activity remains a bounded service/account log,
not a resource-specific delivery trace.

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
