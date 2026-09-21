# Migration report

A migration report must include at least:

* models_discovered
* models_imported
* models_failed
* gallery_assets_imported
* legacy_tables
* unmapped_model_columns
* failures

Consumers should treat any non-zero models_failed count or non-empty failures
array as a migration requiring review.
