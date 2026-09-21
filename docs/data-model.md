# Data model

The schema deliberately separates logical identity from physical artifacts.

| Entity | Purpose |
| --- | --- |
| Model | Logical model identity and shared metadata |
| ModelVersion | A specific published/source version |
| ModelFile | Physical file path, size, timestamp and hash |
| Tag / ModelTag | Shared normalized tags |
| ModelSource | Provider-independent external source tracking |
| ModelAsset | Thumbnail, cover, gallery or preview reference |
| ModelRelationship | Explicit graph relationship between models |
| RegistryEvent | Durable outbox/change record |

Model IDs are generated independently of filenames. SHA-256 is tracked on
ModelFile where available and is not used as the logical model ID.

Application-specific fields belong in the extensions / metadata JSON objects.
Shared ecosystem fields remain strongly typed.

## Compatibility

Compatibility is deterministic. The first implementation considers an explicit
compatible_with relationship or matching non-empty base_model. AI/MCP layers may
reason over the returned deterministic data, but they do not replace the
Registry's basic compatibility decision.
