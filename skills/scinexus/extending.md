# Extending scinexus from a package built on it

Four registration points and one base class. Each is process-local: a worker process does not inherit a registration, so register at import time of your package rather than inside a function that only the master runs.

## Type names in hints

`define_app` resolves the hints on `main()` against the module the app was defined in. A bare string hint is refused outright with `NotImplementedError`, but a forward reference inside a container or a `TypeVar` bound - `list["Genome"]` - is resolved by name, and a name that module does not import gives `TypeError: cannot resolve type name 'Genome'`.

`register_type_namespace` supplies a fallback for downstream users, who write hints naming your types without importing them:

```python { notest }
from scinexus.typing import register_type_namespace


def _my_types() -> dict[str, type]:
    from mypackage import Genome, Variant

    return {"Genome": Genome, "Variant": Variant}


register_type_namespace(_my_types)
```

The provider is called lazily, each time a name needs resolving, so it can defer a heavy import - and it is responsible for its own caching. Registration is idempotent, providers are consulted in registration order, and the first to supply a name wins. Module globals are always checked first.

For compatibility checking itself: a `Protocol` on either side makes the composition-time check pass and leaves the real check to `typeguard` at run time. `scinexus.typing` ships `HasSource`, `HasInfo` and `SerialisableType` (anything with `to_rich_dict`).

## How a record gets its name

Every identifier in a store comes from this chain, and each step is replaceable:

```
input object -> get_data_source -> get_unique_id -> identifier
```

`get_data_source` is a `singledispatch`. Out of the box: a `str` or `Path` gives its file name, a `DataMember` gives its `unique_id`, a `dict` gives `info["source"]` or `source`, and anything else gives its `.source` attribute, followed recursively. Add a branch for your own type rather than replacing the function:

```python { notest }
from scinexus.data_store import get_data_source


@get_data_source.register
def _(data: Genome) -> str | None:
    return data.accession
```

`get_unique_id` then strips format and compression suffixes, so `alpha.fasta.gz` becomes `alpha`.

`set_id_from_source(func)` replaces the whole chain with `func(obj) -> str | None`. It is what `apply_to` and `as_completed` use by default, and what `NotCompleted` uses to normalise its `source=` argument. The per-call `id_from_source=` keyword still wins over it. `set_id_from_source(None)` restores the default, and `get_id_from_source()` returns whatever is active.

The function travels to every worker of every process backend, so a lambda or a function defined inside another function raises `PicklingError` as soon as a parallel run starts - including under the default backend. Define it at module level. (The docstring on `set_id_from_source` says this is only a concern for `loky` and MPI, which is wrong in both directions.)

## Display of summaries

Store summaries return plain data: `.describe` a `dict`, `.summary_not_completed` and the rest a `list[dict]`. `set_summary_display(func)` intercepts all of them, with `func(data, *, name)` where `name` is `"describe"`, `"summary_logs"`, `"summary_not_completed"`, `"summary_citations"` or `"validate"`.

```python { notest }
from scinexus import set_summary_display


def as_table(data, *, name):
    from cogent3 import make_table

    rows = [data] if isinstance(data, dict) else data
    return make_table(data=rows, title=name)


set_summary_display(as_table)
```

That is how a domain package gives back the rendered tables scinexus deliberately does not depend on. `set_summary_display(None)` clears it.

## Other registration points

`register_datastore_reader("myfmt")(MyStore)` in `scinexus.io` teaches `open_data_store` a new suffix. It dispatches on the suffix of a *file*: a directory called `results.myfmt` still goes to `DataStoreDirectory`, since `open_data_store` asks `is_dir()` first.

`register_deserialiser("mypackage.module.MyClass")(func)` in `scinexus.deserialise` teaches `deserialise_object` how to inflate a `to_rich_dict` payload. Matching is by **substring** against the payload's `"type"` entry, in registration order, first hit wins - so a short key such as `"MyClass"` also captures `"OtherPackage.MyClassExtended"`. Register the fully qualified name.

## A custom parallel backend

Subclass `Parallel` and implement five methods: `imap`, `as_completed`, `is_master_process`, `get_rank` and `get_size`. Pass an instance to `set_parallel_backend`, which accepts an instance as readily as one of the four names.

```python { notest }
import multiprocessing

from scinexus.parallel import Parallel, set_parallel_backend


class RayBackend(Parallel):
    def imap(self, f, s, max_workers=None, **kwargs): ...
    def as_completed(self, f, s, max_workers=None, **kwargs): ...
    def get_rank(self): ...
    def get_size(self): ...

    def is_master_process(self):
        # implement this one first, and never let it return None
        return multiprocessing.parent_process() is None


set_parallel_backend(RayBackend())
```

`is_master_process` is the one with consequences beyond parallelism, and the first to get right. Data stores gate directory and schema creation on it, so a backend that answers falsely in the master creates nothing and the next write fails with a bare `FileNotFoundError` naming no backend at all. One that answers truthfully in a worker has every worker try to create the store.

An instance set this way is never replaced. Left to choose for itself, scinexus re-reads the GIL state on every call, because importing an extension that does not declare free-threading support turns the GIL back on mid-process.
