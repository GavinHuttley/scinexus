# Type system

!!! abstract ""

    Why types are checked at composition time rather than call time, how `check_type_compatibility` works, handling of Union types and forward references, the role of `register_type_namespace`, and the relationship to `typeguard` for runtime checking.

## Composability rules

There are rules around app composition, starting with app types. Loaders and writers are special cases. If included, a loader must always be first, e.g.

```python { notest }
app = a_loader + a_generic
```

If included, a writer must always be last, e.g.

```python { notest }
app = a_generic + a_writer
```

Changing the order for either of the above will result in a `TypeError`.

The next constraint on app composition are the input and output types of the apps involved. Specifically, apps define the type of input they work on and the type of output they produce. For two apps to be composed, the output (or return) type of app on the left (e.g. `a_loader`) must overlap with the input type of the app on the right (e.g. `a_generic`). If they don't match, a `TypeError` is raised.

## Built-in type protocols and aliases

SciNexus defines two type-level constructs used across the framework:

- `SerialisableType` -- a `Protocol` that any object with a `to_rich_dict()` method satisfies. Writer apps rely on this to serialise results before storing them in a data store.
- `IdentifierType` -- a type alias (`str | Path | DataMemberABC`) representing the accepted ways to identify a member of a data store. Loader apps accept this as input.

See the [API reference](../reference/utilities.md#type-system) for details.

## Runtime type checking

In addition to checking type compatibility when apps are composed, scinexus validates input data at runtime before each call to `main()`. This uses `typeguard.check_type` to verify that the data matches the app's declared input type. On a mismatch, a `NotCompleted` is returned with a message naming the received and expected types.

!!! important "Why this matters"

    Without runtime type checking, passing the wrong data type to an app still fails — but the error occurs inside `main()` and can be confusing. For example, a message like `'NoneType' object has no attribute 'blah'` gives little indication that the real problem is a type mismatch from an upstream app. With runtime checking enabled, scinexus catches this before entering `main()` and reports the mismatch clearly.

### Disabling type checking with `check_data_type`

Runtime type checking is enabled by default. For mature pipelines where type correctness has been established, you can disable it to remove the small overhead of the `typeguard` check:

```python { notest }
app = read_json() + validate(required_field="name")
app.check_data_type = False
```

Setting `check_data_type` on the outermost app propagates the setting to all apps in the pipeline.
