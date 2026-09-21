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
are not directly mapped. Invalid model types and malformed JSON are reported as
item failures instead of being silently converted to empty/default data. A
newly-created model is rolled back when one of its required tag/source/version/
file imports fails; an existing model is never deleted during rollback.

Run the importer against a backup/read-only copy first. Gallery asset failures
are reported individually so the rest of the migration can continue. A failed
item is never treated as invisible success.

The Model Manager filesystem remains under Model Manager ownership after import.
