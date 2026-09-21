# API

The API is versioned at /api/v1. The normative machine-readable contract is
schema/openapi.yaml.

## Authentication

All routes except GET /health require:

Authorization: Bearer <registry-token>

Consumers may also send X-Raphael-Actor for audit attribution.

## Core operations

Models expose CRUD and search. Versions, files, tags, sources, assets and
relationships are nested below their owning model.

Search filters include query text, model type, tag, creator, base model,
source, SHA-256 hash, limit and offset.

## Events

GET /api/v1/events is an SSE stream. The JSON snapshot endpoint
GET /api/v1/events/snapshot is provided for clients that need bounded polling.


## Network security

The registry uses bearer authentication. The default bind is loopback-only.
For safety, the server refuses plaintext HTTP when bound to a non-loopback
address unless `--allow-insecure-lan` or
`RAPHAEL_REGISTRY_ALLOW_INSECURE_LAN=true` is explicitly configured for a
trusted network. For production/LAN deployments, terminate HTTPS in front of
the registry or otherwise provide TLS before exposing the bearer token to a
network.

## Error responses

Internal database/storage failures are intentionally returned as a generic
`500 request_failed` response; implementation details are kept out of the
client-visible error message.
