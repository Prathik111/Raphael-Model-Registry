# Raphael Model Registry

Canonical model metadata service for the Raphael ecosystem.

The Registry owns logical model identity, versions, files, tags, sources, assets,
relationships, validation, search, compatibility data, revisions, and durable
change events. Model Manager owns physical files and CivitAI downloads; Story
Generator owns stories and generation workflows; ComfyUI owns execution.

## Architecture

```text
Model Manager ─┐
Story Generator ├── HTTP API v1 ──> Raphael Model Registry ──> SQLite
MCP ───────────┘
                                      │
                                      └── durable outbox / SSE events
```

Consumers never access the Registry SQLite database directly.

## Workspace

- `registry-core` — domain model, validation, repository/service contracts, compatibility.
- `registry-api` — versioned HTTP DTOs, routes, authentication, SSE.
- `registry-server` — SQLite persistence, migrations, executable, CLI and migration tooling.
- `registry-client` — generic application-agnostic Rust client with health/auto-start support.

## Verification

CI runs:

```text
cargo fmt --check
cargo check --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
OpenAPI/schema validation
Linux build
Windows build
```

There is no Python code in the Registry workspace, so Ruff is not applicable;
Rust formatting, Clippy and type checking are the equivalent static quality gates.

## Local development

```bash
cargo run -p registry-server -- server
cargo run -p registry-server -- health
cargo run -p registry-server -- integrity-check
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

The server binds to `127.0.0.1:43217` by default and generates a local bearer
token under its data directory. LAN binding must be explicitly enabled.

See `docs/` and `schema/openapi.yaml` for the complete contract.
