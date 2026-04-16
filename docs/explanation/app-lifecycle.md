# The app lifecycle

## Types of apps

### Loaders

These are responsible for loading data and are composable. They inherit from `LoaderApp`.

```python
from scinexus import LoaderApp
```

### Writers

These are responsible for writing data and are composable. They inherit from `WriterApp`.

```python
from scinexus import WriterApp
```

### Generic

Generic apps do other operations on data and are composable. They inherit from `ComposableApp`

```python
from scinexus import ComposableApp
```

### Non-composable

- writers
- generic
- non-composable

As their names imply, loaders load, writers write and generic apps do other operations on data. Non-composable apps cannot be combined with other apps into pipelines.

The apps differ in their properties. 

## From base classes



## From the `define_app` decorator

Using the decorator is the fastest way to turn something you already have into a composable app. Under the hood, the decorator is basically injecting the base classes described above into the inheritance of your own classes.

What `define_app` does step by step (inspect type hints, choose base class, wrap `main`), the `__call__` flow (None check, NotCompleted check, type check, call main, catch exceptions), and how composition chains apps via the `.input` attribute.

