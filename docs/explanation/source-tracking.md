# Source tracking

## The problem

When you call `apply_to()` or `as_completed()` on a data store, each member is fed through the pipeline independently. The pipeline may transform the data into something completely different — a new object with no reference back to the input that produced it. But the writer at the end of the pipeline needs to know *which input* produced *which output* so it can assign the correct unique ID in the output data store.

For example, if a loader reads `"gene_001.fa"` and the pipeline returns a translated protein sequence, the writer needs to store that result under the key `"gene_001"`. Without a mechanism to carry the input identity forward, this link is lost.

## How `source_proxy` solves it

`source_proxy` is a transparent wrapper that carries two extra pieces of state alongside the wrapped object:

- **`.source`** — the original input (or its identifier), preserved across transformations
- **`.uuid`** — a unique identifier for this proxy instance, used for hashing

When `as_completed()` or `apply_to()` processes a data store, each member is wrapped in a `source_proxy` before entering the pipeline. Because `source_proxy` delegates attribute access to the wrapped object via `__getattr__`, downstream apps see the original object and do not need to know about the proxy.

```python { notest }
from scinexus.composable import source_proxy

proxy = source_proxy(some_data)
proxy.source      # the original input
proxy.uuid        # unique identifier for this proxy
proxy.any_attr    # delegates to some_data.any_attr
```

## How `propagate_source` preserves the link

After each pipeline step, the result needs to be re-associated with the original source. `propagate_source` handles this:

1. If the result already has a `.source` attribute (e.g. it is a `DataMember` or another object that natively tracks its origin), the proxy is **unwrapped** — the result stands on its own.
2. Otherwise, the proxy's wrapped object is **updated** to the new result via `set_obj()`, and the proxy (still carrying the original `.source`) is returned.

This means the source identity survives an arbitrary number of pipeline steps, even when intermediate apps return entirely new objects.

## Why this matters for writers

`WriterApp.apply_to()` uses the source to derive unique IDs for output records. This enables **append-only semantics**: on a subsequent run against the same data store, records that already exist in the output are skipped. The unique ID comes from the original input's identity (via `get_data_source()`), which is only available because `source_proxy` carried it through the pipeline.

Without source tracking, the writer would have no way to determine whether a result corresponds to an input that has already been processed.
