# The app lifecycle

!!! abstract ""

    The different app types, their base classes, and how `define_app` transforms a class or function into a composable app.

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

Non-composable apps cannot be combined with other apps into pipelines.

```python
from scinexus import NonComposableApp
```

!!! info

    You can create your app by inheriting from one of the above base classes. Or you can use the `define_app` decorator. Using the decorator is the fastest way to turn something you already have into a composable app. Under the hood, the decorator is basically injecting the base classes described above into the inheritance of your own classes.
