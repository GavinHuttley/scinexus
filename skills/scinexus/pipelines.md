# Pipelines: stores, apply_to, parallel, progress

## Opening a store

`open_data_store(path, suffix=None, limit=None, mode="r")` picks the backend from the path.

| path | backend | notes |
| --- | --- | --- |
| a directory | `DataStoreDirectory` | `suffix` required, and cannot be just `"*"` |
| a zip archive | `ReadOnlyDataStoreZipped` | `suffix` required here too, and any mode but `"r"` raises `ValueError` |
| `*.sqlitedb` | `DataStoreSqlite` | `suffix` is ignored, `limit` is refused unless `mode="r"` |
| `":memory:"` | `DataStoreSqlite` | needs `mode="w"`, the read-only case raises `NotImplementedError` |

A missing `suffix` on a zip reports `suffix is required for DataStoreDirectory`, naming a class you are not using. It means the zip.

`mode` is `"r"`, `"w"` or `"a"`, and none of them empties an existing store. What happens to a record already there depends on the backend and on which kind of record it is:

| | directory store | SQLite store |
| --- | --- | --- |
| `mode="w"`, completed record | the write is skipped, `write()` returns `None`, old content stays | replaced |
| `mode="w"`, not-completed record | replaced | replaced |
| `mode="a"`, either kind | `OSError` | `OSError` |
| `mode="r"` | `OSError` | `OSError` |

To recompute a store from scratch, delete it or write to a new path.

A directory store keeps completed records at the top level, failures as json under `not_completed/`, logs under `logs/`, checksums under `md5/` and citations in `bibliography.citations`. A SQLite store keeps all of that in one file.

Every store answers `len()`, `store[0]`, `"alpha" in store` and iteration. A member has `.unique_id`, `.read()` and `.md5`. The summaries - `.describe`, `.summary_not_completed`, `.summary_logs`, `.summary_citations`, `.validate()` - return a `dict` or a `list[dict]`, plain data rather than a formatted table. `set_summary_display` changes that, see `extending.md`.

Member identifiers are spelled with `/` on every platform, so a not-completed member is `not_completed/alpha.json` on Windows too. Never build one by joining with `Path`.

## apply_to and as_completed

`apply_to` belongs to a writer and needs a composed input. It returns the output store.

```python { notest }
out = (loader() + process() + writer(out_store)).apply_to(
    in_store, parallel=True, par_kw={"max_workers": 4}, show_progress=True
)
```

By default it also writes a `scitrack` log into the store recording the composed app, the package versions, the output identifiers and their md5 sums. `logger=False` turns that off, and a `CachingLogger` of your own directs it. Citations are separate and unconditional: every app carrying a `cite=` is written to the store whatever `logger` says. Provenance is a property of `apply_to`, so a pipeline driven any other way records none of it.

Failures worth knowing about, all raised before any work is done:

- two inputs reducing to one identifier, or an input yielding none, raise `ValueError`
- a writer with nothing composed in front of it raises `RuntimeError`
- an empty `dstore` raises `ValueError`

`as_completed` is the same traversal without a writer. It yields in completion order only when `parallel=True`; run serially it preserves the order the inputs came in.

```python { notest }
for result in (loader() + process()).as_completed(in_store, parallel=True):
    ...
```

The surprise there: a result that does not carry its own origin comes back wrapped in a `source_proxy`, so the identity of the input is not lost. The proxy forwards attribute access, `len`, `bool`, `repr`, `str` and `==`, and nothing else - a pipeline returning an `int` yields a proxy that raises `TypeError` on `result + 1`. Its `__hash__` is its own, so `proxy == 16` is `True` while `proxy in {16}` is `False`: putting results straight into a set or a dict does not work. Unwrap with `result.obj`, and read the input it came from with `result.source`.

## Resuming

`apply_to` skips any input whose identifier is already in the output store, so an interrupted run resumes by pointing the same pipeline at the same store in `mode="a"`.

What counts as "already there" differs by backend, and it decides whether a failure is retried:

- a directory store names a failure `not_completed/alpha.json`, which does not match the input identifier `alpha`, so the input is retried. If it fails again the writer must drop the stale record before writing the new one, or the append-only check raises `OSError`
- a SQLite store names it `alpha`, which does match, so the input is not retried at all. After fixing whatever caused a failure, `store.drop_not_completed(unique_id="alpha")` or the rerun does nothing

## Parallel

`parallel=True` on `apply_to` or `as_completed` runs everything up to the writer in workers. The writer itself always runs in the calling process, so the *output* store is never touched by a worker. The input store does cross: a `DataMember` carries a reference to the store it came from.

| backend | what crosses to a worker | watch out for |
| --- | --- | --- |
| `"multiprocess"` | app and inputs pickled, spawn start method | needs `if __name__ == "__main__":`, no lambdas or closures |
| `"threads"` | nothing, the app instance is shared | a `main()` that mutates anything shared |
| `"loky"` | as multiprocess, but through cloudpickle | an app defined in `__main__`, which includes a notebook cell, fails to deserialise in the worker. Define apps in an importable module |
| `"mpi"` | pickled, to ranks that already exist | launching under `mpiexec`, see below |

`par_kw` reaches the backend, and two of its keys do less than they look:

- `max_workers` is **clamped**, not refused: `apply_to` and `as_completed` only ever use the pool's `as_completed`, which takes the CPU count as a ceiling. A value of 6000 on a six-core machine runs on six. Only a value below one raises
- `chunksize` is validated and then ignored, for the same reason: only `imap` chunks, and nothing in the app machinery calls it
- `if_serial` reaches the MPI backend only if you also pass `use_mpi=True`. Under `set_parallel_backend("mpi")` it is dropped and the default `"raise"` applies

Anything you pass as `id_from_source` must be picklable under every process backend, so a lambda or a local function raises `PicklingError` before any work starts.

Because an input that yields a source-less result is kept alive by its proxy, a worker pickles that input back with the result. For a small result over a large input that is the difference between tens of bytes and megabytes per item. It does not arise for results carrying their own `.source`, which are returned unwrapped.

## MPI

MPI is not a fourth pool. The ranks exist before Python starts and nothing in the program can change them.

```bash
mpiexec -n 8 python -m mpi4py.futures my_script.py
```

Rank 0 is the master and does no work, so `-n 8` gives seven workers and `-n 3` is the smallest launch with more than one. A single worker is serial execution, which `if_serial` refuses by default with a `RuntimeError` rather than running slowly.

Do not set `max_workers`. The count comes from `mpiexec -n`, and a value that disagrees is warned about and ignored. The main logic must sit behind `if __name__ == "__main__":`, as it must under spawn.

## Progress

`show_progress` takes `True`, `False` or a `Progress` instance.

```python
from scinexus import get_progress, set_progress_backend

set_progress_backend("tqdm")
progress = get_progress(True)
for _ in progress(range(3), msg="working"):
    pass
```

`parent.child()` gives a nested bar. For work that reports a fraction rather than iterating, `progress.context(msg="phase", start=0.0, end=0.5)` is a context manager with `update(progress=0.3)`. Note that it creates a **separate** bar of its own: `start` and `end` map the fraction onto that slice of that bar, not onto the parent.
