---
name: scinexus
description: Build and debug scinexus pipelines and data stores - define_app, NotCompleted, open_data_store, apply_to, as_completed, composable app, loader and writer apps, sqlitedb stores, parallel backends, and the file helpers open_, atomic_write and compression handling. Use whenever code imports scinexus, composes apps with +, reads or writes a scinexus data store, or opens a file through scinexus.
license: BSD-3-Clause
compatibility: Requires Python >=3.11 and the scinexus package.
---

# scinexus

`llms.txt` beside this file is the API summary: what exists and what it is called. This file is for what a first attempt gets wrong.

Read one of these only when the task reaches it:

- `pipelines.md` - data stores, `apply_to` versus `as_completed`, resume, the four parallel backends, progress
- `io.md` - `open_`, compression, `atomic_write`, streaming lines and records
- `extending.md` - the hooks a package built on scinexus registers

## The shape of a pipeline

A loader, a generic app, a writer, applied to a data store. Most wrong-shaped code is a variation on getting this wrong.

```python
from scinexus import NotCompleted, define_app, open_data_store
from scinexus.data_store import DataMember


@define_app(app_type="loader")
class load_text:
    def main(self, member: DataMember) -> str:
        return member.read()


@define_app
def shout(text: str, end: str = "!") -> str:
    return text.upper() + end


@define_app(app_type="writer")
class save_text:
    def __init__(self, data_store):
        self.data_store = data_store

    def main(self, data: str, identifier: str = "") -> DataMember:
        if isinstance(data, NotCompleted):
            self.data_store.drop_not_completed(unique_id=identifier)
            return self.data_store.write_not_completed(
                unique_id=identifier, data=data.to_json()
            )
        return self.data_store.write(unique_id=identifier, data=data)


in_dstore = open_data_store("raw", suffix="txt")
out_dstore = open_data_store("out", suffix="txt", mode="w")
app = load_text() + shout(end="?") + save_text(out_dstore)
app.apply_to(in_dstore)
out_dstore.close()
```

Applied to `raw/alpha.txt` holding `contents of alpha`, that writes `out/alpha.txt` holding `CONTENTS OF ALPHA?`.

## What to get right

1. **The type hints on `main()` are load-bearing.** The first parameter and the return must both be annotated or `define_app` raises. They are not enforced in the same way: `typeguard` checks the incoming value against the first parameter on every call, while the return hint is used only to decide whether `+` is allowed. An app annotated `-> str` that returns `42` returns `42`. `from __future__ import annotations` anywhere in the module turns every hint into a string and `define_app` raises `NotImplementedError`.
2. **A function app takes one data argument, first.** Every later parameter is constructor configuration, so `shout(end="?")` configures and `shout(end="?")("hi")` calls. A class app puts configuration in `__init__` for the same reason.
3. **Return `NotCompleted` for an expected failure rather than raising.** In a loader or a generic app an uncaught exception is caught and becomes a `NotCompleted` of type `ERROR`, so the choice is which kind of record you get rather than whether the run survives. Returning `None` is never right: it becomes a record of type `BUG`. A **writer** has no such net - `apply_to` calls its `main` directly, so an exception there ends the run with no log and no citations written.
4. **A writer's `main` always receives `NotCompleted` values.** `skip_not_completed` is forced off for writers, so the flag cannot turn this off. Handle them the way the skeleton does, which is the shape the `llms.txt` writer example also has. Dropping the stale record first matters on a resume of a directory store, which retries a failed input on the next run and then cannot rewrite its record in `mode="a"`. A SQLite store does the opposite and never retries a failure. See `pipelines.md`.
5. **Nothing already in the store is recomputed, whatever the mode.** `mode="w"` creates the store, it does not empty it, and `apply_to` skips every input whose identifier is already there. So editing an app and re-running it over the same output store writes nothing and reports no error - point it at a new path, or delete the old one. A directory store goes further and refuses even a direct `write()` of a record it holds, returning `None`, which the framework turns into a `BUG` record.
6. **Encoding is chosen at `open_data_store()`, never at `write()`.** The store owns the suffix and the compression, the writer supplies the bare stem. Into a `suffix="fasta"` store, `write(unique_id="brca1.fa")` stores `brca1.fasta`, and `write(unique_id="brca1.fasta.gz")` raises `ValueError` rather than renaming.
7. **A writable SQLite store holds a lock until `close()`.** Opening a second writable handle to the same file is silent, and then the first operation through it - a `write()`, or a read such as `.describe` - raises `OSError` naming the session that holds the lock. There is no context manager: call `close()`.
