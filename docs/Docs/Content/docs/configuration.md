---
title: Configuration
description: Environment variables and configuration options for MicroStack.
order: 2
section: Guides
---

# Configuration

MicroStack is configured via environment variables. Emulator settings are consolidated into a strongly-typed `MicroStackOptions` class internally; the separate UI host also reads its port and API connection settings.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_PORT` | `4566` | Port to listen on |
| `EDGE_PORT` | `4566` | Alias for `GATEWAY_PORT` (LocalStack compatibility) |
| `MICROSTACK_HOST` | `localhost` | Hostname for URL generation |
| `MICROSTACK_REGION` | `us-east-1` | Default AWS region |
| `MICROSTACK_ACCOUNT_ID` | `000000000000` | Default AWS account ID |
| `MICROSTACK_SQS_ENDPOINT_STRATEGY` | `request` | SQS queue URL source: `request` uses the caller's scheme, host, and port; `legacy` uses `MICROSTACK_HOST` and `GATEWAY_PORT` |
| `MICROSTACK_UI_PORT` | `4567` | UI host port and the port of the allowed browser origin |
| `MICROSTACK_API_URL` | *(derived)* | Browser-reachable API URL, supplied by the UI host |
| `PERSIST_STATE` | `0` | Set to `1` to enable JSON state persistence |
| `STATE_DIR` | `<temp>/microstack-state` | Directory for persisted state files |
| `SERVICES` | *(all)* | Comma-separated list of services to enable |
| `S3_PERSIST` | `0` | Set to `1` to enable S3 object persistence |
| `LOCALSTACK_PERSISTENCE` | `0` | Set to `1` to enable persistence (LocalStack compat) |

## Service Filtering

To start MicroStack with only specific services:

```bash
docker run -e SERVICES=sqs,s3,dynamodb -p 4566:4566 ghcr.io/damianh/microstack:latest
```

Service aliases are supported:

| Alias | Canonical Service |
|-------|-------------------|
| `cloudwatch-logs` | `logs` |
| `cloudwatch` | `monitoring` |
| `eventbridge` | `events` |
| `step-functions` / `stepfunctions` | `states` |
| `execute-api` / `apigatewayv2` | `apigateway` |
| `kinesis-firehose` | `firehose` |
| `elbv2` / `elb` | `elasticloadbalancing` |

## Custom Port

```bash
docker run -e GATEWAY_PORT=5000 -p 5000:5000 ghcr.io/damianh/microstack:latest
```

`GATEWAY_PORT` controls the port inside the container. When Docker or Testcontainers
maps that port dynamically, SQS queue URLs use the incoming request authority by
default so SDKs can use returned URLs without rewriting them.

To retain the earlier configured-address behavior:

```bash
docker run \
  -e MICROSTACK_SQS_ENDPOINT_STRATEGY=legacy \
  -e MICROSTACK_HOST=localhost \
  -e GATEWAY_PORT=4566 \
  -p 4566:4566 \
  ghcr.io/damianh/microstack:latest
```

## State Persistence

Enable persistence to survive restarts:

```bash
docker run -e PERSIST_STATE=1 -v ./state:/tmp/microstack-state -p 4566:4566 ghcr.io/damianh/microstack:latest
```

State is saved as JSON files in `STATE_DIR` on shutdown and restored on startup.

## UI Connection

The UI host serves runtime connection settings at `/_microstack/ui-config` on
the UI port. By default, the browser connects to its current scheme/hostname
with `GATEWAY_PORT` (or `EDGE_PORT`, default `4566`). Set `MICROSTACK_API_URL`
when the browser needs a different API address. The URL must be absolute HTTP(S)
and must not contain credentials.

An explicit `ApiBaseUrl` in the client's configuration overrides runtime
discovery. The UI host also accepts `ApiBaseUrl` in its own configuration, with
`MICROSTACK_API_URL` taking precedence there.

For custom local ports, publish the matching ports for both processes:

```bash
docker run \
  -e GATEWAY_PORT=5000 -e MICROSTACK_UI_PORT=5001 \
  -p 127.0.0.1:5000:5000 -p 127.0.0.1:5001:5001 \
  ghcr.io/damianh/microstack:latest
```

Open `http://localhost:5001`. The admin API's allowed browser origin is derived
from `MICROSTACK_HOST` and `MICROSTACK_UI_PORT`; changing only the browser's API
URL does not grant a different UI origin access. CORS is not authentication.
Keep these development endpoints on a trusted local network.
