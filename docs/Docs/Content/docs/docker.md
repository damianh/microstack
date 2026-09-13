---
title: Docker
description: Running MicroStack as a Docker container, building OCI images, and Docker Compose.
order: 3
section: Guides
---

# Docker

MicroStack supports both the repository Dockerfile and SDK OCI publishing.
The native process serves AWS APIs and the browser UI on one port.

## Pre-built Image

```bash
docker run -p 127.0.0.1:4566:4566 ghcr.io/damianh/microstack:latest
```

Open `http://localhost:4566/ui/` for the UI. AWS clients use the same origin.

## Building Locally

```bash
docker build -t microstack:latest .
docker run -p 127.0.0.1:4566:4566 microstack:latest
```

Alternatively, SDK publishing includes the UI under the API publish directory:

```bash
dotnet publish src/MicroStack/MicroStack.csproj /t:PublishContainer -c Release -r linux-musl-x64
```

Native AOT publishing requires a matching Linux toolchain; use the Dockerfile
or a suitable Linux environment when building from Windows. Both image paths
use the .NET runtime-dependencies Alpine image and include the browser assets
served directly by the native API process.

For an API-only publish, pass `-p:PublishAdminUi=false`. The standalone design
reference in `design/resource-explorer` is never included in production images.

## Docker Compose

```yaml
services:
  microstack:
    image: ghcr.io/damianh/microstack:latest
    ports:
      - "127.0.0.1:4566:4566"
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
