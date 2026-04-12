---
title: Persistence
description: How MicroStack persists and restores service state across restarts.
order: 2
section: Architecture
---

# Persistence

By default, MicroStack stores all state in memory. When the process stops, everything is gone. For development workflows that need state across restarts, MicroStack supports optional JSON file persistence.

## Enabling Persistence

Set the `PERSIST_STATE` environment variable:

```bash
# Docker
docker run -e PERSIST_STATE=1 -v ./state:/tmp/ministack-state -p 4566:4566 ghcr.io/damianh/microstack:latest

# Direct
PERSIST_STATE=1 dotnet run --project src/MicroStack/MicroStack.csproj
```

## How It Works

The `StatePersistence` class (`src/MicroStack/Internal/StatePersistence.cs`) manages the lifecycle:

### Save (on shutdown)

1. The ASP.NET Core `IHostApplicationLifetime.ApplicationStopping` event triggers `SaveAll()`.
2. For each registered service handler, calls `handler.GetState()`.
3. If the handler returns non-null state, serializes it as JSON to `{STATE_DIR}/{serviceName}.json`.
4. Uses atomic writes: writes to a `.tmp` file first, then `File.Move` with overwrite. This prevents corruption if the process is killed mid-write.

### Restore (on startup)

1. After building the app and registering all handlers, calls `persistence.RestoreAll()`.
2. For each handler, checks if `{STATE_DIR}/{serviceName}.json` exists.
3. If found, deserializes the JSON and calls `handler.RestoreState(data)`.
4. Handlers populate their in-memory collections from the deserialized data.

### Reset

The `POST /_ministack/reset` endpoint calls:
- `registry.ResetAll()` — clears all in-memory state.
- `persistence.DeleteAll()` — deletes all `.json` files in `STATE_DIR`.

## State Directory

| Variable | Default | Description |
|----------|---------|-------------|
| `STATE_DIR` | `<temp>/ministack-state` | Directory where state files are written |

Each service gets its own file:

```
ministack-state/
  sqs.json
  dynamodb.json
  s3.json
  sns.json
  ...
```

## Handler State Contract

Every `IServiceHandler` implements two methods:

```csharp
object? GetState();
void RestoreState(object data);
```

- `GetState()` returns a serializable object representing the handler's current state, or `null` if there's nothing to persist.
- `RestoreState(object data)` receives the deserialized JSON (as `System.Text.Json` object trees) and rebuilds in-memory collections.

Handlers use `AccountScopedDictionary<TKey, TValue>.ToRaw()` and `.FromRaw()` to serialize/deserialize their account-scoped collections, preserving account isolation across restarts.

## S3 Object Persistence

S3 has a separate persistence mode via the `S3_PERSIST` or `LOCALSTACK_PERSISTENCE` environment variables. When enabled, S3 object data (blobs) are persisted to disk in addition to metadata. This is independent of the general `PERSIST_STATE` mechanism.

## Error Handling

Persistence is best-effort:

- If saving a service's state fails, the error is logged and other services continue saving.
- If restoring a service's state fails, the error is logged and the service starts with empty state.
- If a temp file can't be deleted after a failed write, the error is silently ignored.

This ensures persistence issues never prevent MicroStack from starting or shutting down.
