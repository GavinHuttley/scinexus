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
- `max_workers` below 1 is refused by the local backends with a message naming the value, where it previously reached the executor and surfaced that library's own error. `max_workers=0` was a silent synonym for `None` and is now an error, and a `bool` raises `TypeError` where `True` previously asked for one worker, so `None` is the only way to ask for one worker per CPU. The MPI backend is unchanged and still reads `0` as one worker.
- `chunksize` follows the same rule as `max_workers`: below 1 raises `ValueError` and a `bool` raises `TypeError`, where `0` and `False` previously meant "no preference" and `True` asked for one item per chunk. Every call that accepts the argument now checks it, including the ones that go on to ignore it — only `imap` on the process and MPI backends chunks the work by it, since `as_completed` submits one task per item and a thread pool has no per-item transport cost to amortise.
- `open_()` takes a bare `"r"` or `"w"` as text for every suffix, matching builtin `open` rather than `gzip`, `bz2` and `lzma`, which take a bare mode as binary. Previously `open_(path, "w")` on a `.gz` gave a binary handle and `open_(path, "r")` raised. Use `"rb"` or `"wb"` for bytes. This also applies to `atomic_write`, whose mode defaults to `"w"`.

## Deprecated

- `ComposableApp.disconnect()` — discontinued, will be removed in version 2026.9. No longer required since composition uses shallow copies.
