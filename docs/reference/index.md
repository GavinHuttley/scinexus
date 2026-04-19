# API reference

Complete API documentation for all public `scinexus` modules.

## App framework

- [App classes](app-classes.md) -- `AppBase`, `ComposableApp`, `WriterApp`, `LoaderApp`, `NonComposableApp`
- [define_app](define-app.md) -- the `define_app` decorator and `AppType` enum
- [NotCompleted](not-completed.md) -- the `NotCompleted` sentinel and `NotCompletedType` enum
- [Source tracking](source-proxy.md) -- `source_proxy` and `propagate_source` for data provenance
- [Data stores](data-stores.md) -- `open_data_store` and data store backends

## Standalone utilities

- [IO utilities](io-util.md) -- file IO with compression, atomic writes, streaming
- [Parallel execution](parallel.md) -- parallel map, imap, as_completed
- [Progress](progress.md) -- progress bar ABCs and backends
- [Deserialisation](deserialise.md) -- JSON deserialisation registry
- [Utilities](utilities.md) -- introspection helpers and type namespace registration
