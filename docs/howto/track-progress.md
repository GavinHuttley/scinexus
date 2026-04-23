# Track progress

!!! abstract ""

    How to choose your preferred progress bar backend and customise progress bars.

`scinexus` defaults to using the [tqdm](https://pypi.org/project/tqdm/) for progress bars. These behave well across terminal and notebook environments. We also support using [rich](https://pypi.org/project/rich/) for its progress bars. A single API for different progress backends.

## Choosing the progress bar backend

Use `set_progress_backend` to switch between backends. The default is `tqdm`.

```python { notest }
import scinexus

scinexus.set_progress_backend("rich")  # switch to rich
scinexus.set_progress_backend("tqdm")  # switch back to tqdm
scinexus.set_progress_backend(None)  # reset to default (tqdm)
```

## Getting a progress bar

Use `get_progress` to obtain a `Progress` instance. Passing `show_progress=True` returns the current default backend.

```python { notest }
import scinexus

pbar = scinexus.get_progress(show_progress=True)

for item in pbar(range(100), msg="Processing"):
    pass  # your work here
```

You can pass keyword arguments to configure the default backend:

```python { notest }
import scinexus

pbar = scinexus.get_progress(show_progress=True, colour="blue", leave=True)
```

You can also pass a `Progress` instance directly:

```python { notest }
from scinexus.progress import RichProgress

pbar = scinexus.get_progress(show_progress=RichProgress())
```

!!! note

    If you call `get_progress(show_progress=False)`, it returns `NoProgress`, which silently passes through the iterable.

## Nesting progress bars

=== "Using `tqdm` (default)"

    Create nested progress bars using `child()`. Each bar can have its own description via the `msg` keyword. Create the child once before the loop — it automatically resets to zero on each subsequent call.

    ```python { notest }
    import scinexus

    pbar = scinexus.get_progress(show_progress=True)
    child = pbar.child()

    for batch in pbar(range(3), msg="Outer loop"):
        for item in child(range(10), msg=f"Inner batch {batch}"):
            pass  # your work here
    ```

=== "Using `rich`"

    The same nesting pattern works with the `rich` backend:

    ```python { notest }
    import scinexus

    scinexus.set_progress_backend("rich")
    pbar = scinexus.get_progress(show_progress=True)
    child = pbar.child()

    for batch in pbar(range(3), msg="Outer loop"):
        for item in child(range(10), msg=f"Inner batch {batch}"):
            pass  # your work here
    ```

    `rich` children share the same `rich.progress.Progress` display instance, so all bars render together in a single live display.

The outer bar tracks the top-level iteration. Each call to `child()` creates a new `Progress` at the next cursor position, so inner bars appear below the outer one. The child bar is reused across iterations — on the second and subsequent calls, the bar resets to zero instead of creating a new one.

#### Push-based sub-contexts

When you need to report fractional progress rather than iterating, use `context()`:

```python { notest }
import scinexus

pbar = scinexus.get_progress(show_progress=True)

child = pbar.child()
for batch in pbar(range(3), msg="Processing"):
    with child.context(msg=f"Batch {batch}") as ctx:
        for i in range(100):
            ctx.update(progress=i / 100, msg=f"Step {i}")
```

The context maps progress values from `[0.0, 1.0]` to the configured `[start, end]` range and is cleaned up automatically when the `with` block exits.

## Cleaning up

Both `Progress` and `ProgressContext` support the context manager protocol. Using a progress bar as a context manager ensures that `close()` is called automatically, which finalises the display and moves the cursor past the bars. Without cleanup, leftover bars can leave the terminal cursor in the wrong position.

=== "Using `tqdm` (default)"

    ```python
    import scinexus

    with scinexus.get_progress(show_progress=True) as pbar:  # (1)!
        child = pbar.child()
        for batch in pbar(range(3), msg="Outer"):
            for item in child(range(10), msg=f"Batch {batch}"):
                pass
    ```

    1. `close()` is called automatically and the cursor position is restored.

=== "Using `rich`"

    ```python
    import scinexus

    scinexus.set_progress_backend("rich")
    with scinexus.get_progress(show_progress=True) as pbar:  # (1)!
        child = pbar.child()
        for batch in pbar(range(3), msg="Outer"):
            for item in child(range(10), msg=f"Batch {batch}"):
                pass
    ```

    1. `close()` is called automatically and the cursor position is restored.

???- tip "No context manager? No problem!"

    ```python
    import scinexus

    pbar = scinexus.get_progress(show_progress=True)
    child = pbar.child()
    for batch in pbar(range(3), msg="Outer"):
        for item in child(range(10), msg=f"Batch {batch}"):
            pass
    pbar.close()  # (1)!
    ```

    1. Call `close()` explicitly when you are done

    !!! note

        Calling `close()` on a `Progress` instance also closes all of its children. For standalone `ProgressContext` objects (from `context()`), use the `with` statement as shown in the [push-based sub-contexts](#push-based-sub-contexts) section.

## Customising appearance

### Persisting bars after completion

By default, `tqdm` keeps the outermost bar visible after completion but clears nested bars. `rich` removes all bars. Use `leave` to control this:

```python { notest }
from scinexus.progress import TqdmProgress, RichProgress

# Keep all tqdm bars visible after completion
pbar = TqdmProgress(leave=True)

# Keep all rich bars visible after completion
pbar = RichProgress(leave=True)
```

You can also set `leave` independently on child bars:

```python { notest }
from scinexus.progress import TqdmProgress

pbar = TqdmProgress(leave=True)
child = pbar.child(leave=False)  # child bars disappear, outer persists

for batch in pbar(range(3), msg="Outer"):
    for item in child(range(10), msg=f"Batch {batch}"):
        pass
```

### Setting bar colour

Both backends support a `colour` parameter. For `tqdm`, this sets the bar colour directly. For `rich`, it styles the bar column when the display is auto-created.

```python { notest }
from scinexus.progress import TqdmProgress, RichProgress

pbar = TqdmProgress(colour="green")
pbar = RichProgress(colour="cyan")
```

Colour is inherited by child bars.
