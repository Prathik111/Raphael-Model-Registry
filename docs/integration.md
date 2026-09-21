# Integration

## Model Manager

Phase the migration in this order:

1. Import legacy metadata into the Registry.
2. Switch reads to Registry API.
3. Switch metadata writes to Registry API.
4. Switch scanner upserts/reconciliation to Registry.
5. Keep physical file operations in Model Manager.
6. Retain the old DB as a read-only rollback artifact until verification completes.

No Model Manager consumer should open Registry SQLite directly.

## Story Generator

Story Generator searches the Registry for checkpoints and LoRAs, retrieves
metadata and compatibility data, and passes selected model references to its
ComfyUI bridge.

## MCP

MCP exposes high-level Registry tools by calling the same API or shared service
boundary. It does not maintain a parallel metadata store.

## Registry auto-start

registry-client can health-check the shared endpoint and launch the standard
Registry executable when the service is not running. The server records its
lock, pid and port files in its data directory.
