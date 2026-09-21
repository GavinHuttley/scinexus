# Source tracking

!!! abstract ""

    How `source_proxy` preserves the link between input identity and output when data is transformed through a pipeline.

## The problem

When you call `apply_to()` or `as_completed()` on a data store, each member is fed through the pipeline independently. The pipeline may transform the data into something completely different — a new object with no reference back to the input that produced it. But the writer at the end of the pipeline needs to know *which input* produced *which output* so it can assign the correct unique ID in the output data store.

For example, if a loader reads `"gene_001.fa"` and the pipeline returns a translated protein sequence, the writer needs to store that result under the key `"gene_001"`. Without a mechanism to carry the input identity forward, this link is lost.

## How `source_proxy` solves it

`source_proxy` is a transparent wrapper that carries two extra pieces of state alongside the wrapped object:

- **`.source`** — the original input (or its identifier), preserved across transformations
- **`.uuid`** — a unique identifier for this proxy instance, used for hashing

When `as_completed()` or `apply_to()` processes a data store, a member that does not have identifying information[^1] is wrapped in a `source_proxy` before entering the pipeline. Because `source_proxy` delegates attribute access to the wrapped object via `__getattr__`, downstream apps see the original object and do not need to know about the proxy.

```python { notest }
from scinexus.composable import source_proxy

proxy = source_proxy(some_data)
proxy.source  # the original input
proxy.uuid  # unique identifier for this proxy
proxy.any_attr  # delegates to some_data.any_attr
```

## How `propagate_source` preserves the link

`propagate_source` wraps the whole composed pipeline rather than each step. A composed app pulls its input through the chain internally, so this runs once per input record, and when the pipeline returns the result is re-associated with the original source:

1. If the result carries a reference to its own origin[^1], the proxy is **unwrapped** — the result stands on its own.
2. Otherwise[^2] the proxy's wrapped object is **updated** to the new result via `set_obj()`, and the proxy, still carrying the original `.source`, is returned.

[^1]: a `.source` attribute for an object, or a `"source"` key, or `info["source"]`, for a `dict`
[^2]: For example, a `str` or `bytes` have no `.source` and take the second path.

This means the source identity survives an arbitrary number of pipeline steps, even when intermediate apps return entirely new objects.

## Why this matters for writers

`WriterApp.apply_to()` uses the source to derive unique IDs for output records. This enables **append-only semantics**: on a subsequent run against the same data store, records that already exist in the output are skipped. The unique ID comes from the original input's identity, extracted by whatever `get_id_from_source()` returns — by default `get_unique_id()`, which strips format suffixes, so an input `gene_001.fa` is stored under `gene_001`.

Without source tracking, the writer would have no way to determine whether a result corresponds to an input that has already been processed.

## Failures carry the same identity

When an app raises an exception, the framework turns it into a `NotCompleted` recording which input failed. That identity is taken on entry to the app, before the pipeline's intermediate result replaces it, so a failure part way down a chain still names the input rather than whatever value was being processed when it broke.

An app may also return a `NotCompleted` itself. If it supplies no source, the proxy's identity is filled in on the way out, so the failure is stored under the same unique ID a successful result would have had. If it supplies one, that is left alone — an app that knows a different file is at fault can say so.
