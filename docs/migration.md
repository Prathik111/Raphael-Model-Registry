# Model Manager migration

The repository contains raphael-registry import-model-manager DATABASE for
transforming the current Model Manager SQLite schema into Registry records.

The importer does not copy the SQLite file. It reads the legacy schema and
creates canonical Registry entities.

* models -> Model + ModelVersion + ModelFile
* tags_json -> normalized Tag / ModelTag records
* CivitAI IDs/URLs -> ModelSource
* images local/thumbnail paths -> ModelAsset records with preserved metadata

The command reports discovered/imported/failed counts and legacy columns that
are not directly mapped.

Run the importer against a backup/read-only copy first. A failed item is
reported explicitly; it is never treated as invisible success.

The Model Manager filesystem remains under Model Manager ownership after import.
