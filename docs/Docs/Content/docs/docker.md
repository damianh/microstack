---
title: Docker
description: Running MicroStack as a Docker container, building OCI images, and Docker Compose.
order: 3
section: Guides
---

# Docker

MicroStack publishes OCI container images via `dotnet publish` — no Dockerfile required.

## Pre-built Image

```bash
docker run -p 4566:4566 ghcr.io/damianh/microstack:latest
```

## Building Locally

```bash
dotnet publish src/MicroStack/MicroStack.csproj /t:PublishContainer -c Release
docker run -p 4566:4566 microstack:latest
```

This produces a ~237MB image based on `mcr.microsoft.com/dotnet/aspnet:10.0`.

## Docker Compose

```yaml
services:
  microstack:
    image: ghcr.io/damianh/microstack:latest
    ports:
      - "4566:4566"
    environment:
      - PERSIST_STATE=1
    volumes:
      - microstack-state:/tmp/microstack-state

volumes:
  microstack-state:
```

## Health Check

```bash
curl http://localhost:4566/_microstack/health
```

Returns JSON with all available services:

```json
{
  "services": {
    "sqs": "available",
    "dynamodb": "available",
    "s3": "available",
    ...
  },
  "edition": "light",
  "version": "0.1.0"
}
```

## Resetting State

```bash
curl -X POST http://localhost:4566/_microstack/reset
```

Clears all in-memory state across all services.

## Dynamic Port Mapping

Container frameworks such as Testcontainers should map container port `4566` to a
random host port and wait for `/_microstack/health`. SQS automatically returns queue
URLs using the incoming request's mapped authority. See
[Integration Testing](/testing) for runnable examples in five languages.
