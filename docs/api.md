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
