# Type system

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
