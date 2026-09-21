# Architecture

The Registry is the single authoritative source of model metadata.

## Ownership

The Registry owns logical model identity, model metadata, versions, file
records, tags, sources, assets, relationships, validation, deterministic
compatibility, search, revisions, and durable registry events.

Model Manager owns filesystem discovery, downloads, installation, movement and
deletion of physical model files, CivitAI operations, local thumbnail/gallery
storage, and the management UI.

Story Generator owns stories, scenes, image-generation workflows, and ComfyUI
workflow orchestration. ComfyUI owns actual generation execution.

## Boundary

Consumers communicate with the Registry through the versioned HTTP API. They
never open the Registry SQLite database directly.

Model Manager ─┐
Story Generator ├── /api/v1 ──> Registry ──> SQLite
MCP ───────────┘                  │
                                  └── durable event outbox ──> SSE

The Registry does not download models, delete files, run ComfyUI, or implement a
consumer-specific metadata database.

## Security

The server binds to 127.0.0.1 by default and requires a bearer token for API
routes other than health. LAN binding is opt-in and remains authenticated.

## Concurrency

Mutable model and version records use optimistic concurrency through revision.
A write must include expected_revision. A stale write receives HTTP 409.

Every successful mutation writes its registry event in the same SQLite
transaction as the data mutation.

## Events

Events are persisted in registry_events before the transaction commits. The SSE
endpoint reads the durable stream, so a process restart does not silently drop
committed changes.
