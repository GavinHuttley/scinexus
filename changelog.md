# Changelog

Changes from the original cogent3 app infrastructure.

## New Features

- Standalone package extracted from `cogent3.app` — no cogent3 dependency required.
- Generic base classes `AppBase[T, R]`, `ComposableApp[T, R]`, and `WriterApp[T, R]` that apps can inherit from directly as an alternative to the `@define_app` decorator. Type checkers can resolve types through class inheritance without a plugin.
- mypy plugin (`scinexus._mypy_plugin`) for correct type inference of `@define_app` decorated classes. Synthesises the `__call__` return type as `R | NotCompleted`.
- `check_data_type` attribute on apps — a settable property to toggle runtime input type checking on or off. Disabling can speed up execution and simplify debugging.
- `NotCompletedType` enum (`ERROR`, `FAIL`, `BUG`) for categorising failure types, replacing bare strings.
- `set_summary_display()` / `get_summary_display()` — a module-level registry allowing downstream packages (e.g. cogent3) to register custom display functions for data store summary methods (`describe`, `summary_logs`, `summary_not_completed`, `summary_citations`, `validate`).
- `citations` and `bib` properties on apps for tracking software citations via the `citeable` library. Citations propagate through composed pipelines.
- Pluggable parallel backends -- choose between stdlib multiprocessing, loky, or MPI via `set_parallel_backend()`.
- Pluggable progress bars -- use `tqdm` or `rich` via the `Progress` protocol and `set_default_progress()`.
- `set_id_from_source()` / `get_id_from_source()` -- register a custom function for extracting storage identifiers from data.
- `apply_to()` accepts `logger=False` to disable log file creation.

## Enhancements

- App composition (`+`) now makes shallow copies of the right-hand operand. Composed pipelines no longer share mutable state.
- Composition-time type compatibility checking via `check_type_compatibility()` — catches type mismatches when apps are composed with `+`, before any data is processed.
- Data store summary methods (`describe`, `summary_logs`, etc.) return `list[dict]` or `dict` instead of cogent3 `Table` objects. Custom display can be restored via `set_summary_display()`.
- All modules pass mypy strict type checking.
- `StrOrBytes` type alias replaced with `str | bytes` throughout.
- Inline `assert` statements replaced with explicit `ValueError` / `TypeError` raises.
- Type-hint-related imports moved under `TYPE_CHECKING` for lighter runtime import overhead.

## Deprecated

- `ComposableApp.disconnect()` — discontinued, will be removed in version 2026.9. No longer required since composition uses shallow copies.
