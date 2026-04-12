---
title: Configuration
description: Environment variables and configuration options for MicroStack.
order: 2
section: Guides
---

# Configuration

MicroStack is configured via environment variables. All settings are consolidated into a strongly-typed `MicroStackOptions` class internally.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_PORT` | `4566` | Port to listen on |
| `EDGE_PORT` | `4566` | Alias for `GATEWAY_PORT` (LocalStack compatibility) |
| `MINISTACK_HOST` | `localhost` | Hostname for URL generation |
| `MINISTACK_REGION` | `us-east-1` | Default AWS region |
| `MINISTACK_ACCOUNT_ID` | `000000000000` | Default AWS account ID |
| `PERSIST_STATE` | `0` | Set to `1` to enable JSON state persistence |
| `STATE_DIR` | `<temp>/ministack-state` | Directory for persisted state files |
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

## State Persistence

Enable persistence to survive restarts:

```bash
docker run -e PERSIST_STATE=1 -v ./state:/tmp/ministack-state -p 4566:4566 ghcr.io/damianh/microstack:latest
```

State is saved as JSON files in `STATE_DIR` on shutdown and restored on startup.
