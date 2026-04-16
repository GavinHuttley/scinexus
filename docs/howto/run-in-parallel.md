# Run in parallel

*How to parallelise any function using `parallel.map`, `parallel.imap`, and `parallel.as_completed` as standalone utilities, and how to enable parallel execution in app pipelines with `parallel=True` and `par_kw`, using `loky` or MPI.*

## Data level parallelism

`scinexus` supports parallel computation for the common case where the same calculation needs to be applied to many independent data items. A master process splits the work among available CPU cores, each worker processes its share, and results are collected.

!!! warning

    Parallelism is not always faster. You should see a performance gain when the computation time per task significantly exceeds the overhead of distributing work. If individual tasks are very fast, the overhead of inter-process communication can dominate.

    If individual output files are small, storing results in a single file (e.g. a `.sqlitedb` database) is more efficient than writing many small files.

## Parallel computation on a single computer

This requires no additional software installs and works with standalone scripts and Jupyter notebooks. Under the hood, `scinexus` uses the [loky](https://loky.readthedocs.io/) library for process-based parallelism.

### Using `app.apply_to()`

If you have a composed app **with** a writer, use `apply_to()` with the `parallel` and `par_kw` keyword arguments:

```python { notest }
result = app.apply_to(dstore, parallel=True, par_kw=dict(max_workers=4))
```

### Using `app.as_completed()`

If you have a composed app **without** a writer, use `as_completed()`. This returns a generator, so wrap it with `list()` or iterate over it:

```python { notest }
results = list(app.as_completed(dstore, parallel=True, par_kw=dict(max_workers=4)))
```

### Using `scinexus.parallel` directly

For parallelising any function (not just apps), use the functions in `scinexus.parallel`.

#### `parallel.as_completed` — results in completion order

Returns results as they finish. The order may differ from the input order. It also tends to balance work better across compute nodes than `imap` or `map`.

```python { notest }
from scinexus import parallel

result = list(parallel.as_completed(is_prime, PRIMES, max_workers=4))
```

The first argument is the function to call, the second is the iterable of inputs. Each input element is passed as a single argument to the function. The data is broken into chunks across workers automatically.

!!! note

    If you don't specify `max_workers`, all available CPUs are used.

#### `parallel.imap` — preserving input order (generator)

Returns results in the same order as the input, yielding one at a time:

```python { notest }
from scinexus import parallel

for result in parallel.imap(process_item, items, max_workers=4):
    handle(result)
```

#### `parallel.map` — preserving input order (list)

Same as `imap` but returns a list:

```python { notest }
from scinexus import parallel

results = parallel.map(process_item, items, max_workers=4)
```

### Complete example

```python { notest }
import math
from scinexus import parallel


def is_prime(n):
    if n % 2 == 0:
        return False
    sqrt_n = int(math.floor(math.sqrt(n)))
    for i in range(3, sqrt_n + 1, 2):
        if n % i == 0:
            return False
    return True


PRIMES = [
    112272535095293,
    112582705942171,
    115280095190773,
    115797848077099,
    117450548693743,
    993960000099397,
]

if __name__ == "__main__":
    results = parallel.map(is_prime, PRIMES, max_workers=4)
    for number, prime in zip(PRIMES, results):
        print(f"{number} is prime: {prime}")
```

## Parallel computation on multiple computers (MPI)

On systems with multiple nodes (e.g. an HPC cluster), use MPI via the [mpi4py](https://mpi4py.readthedocs.io/) library. You need to install an MPI implementation (e.g. [OpenMPI](https://www.open-mpi.org/)) and the `mpi4py` Python package

```bash
pip install mpi4py
```

or installing `scinexus` [with `mpi` extra][optional-extras].

Pass `use_mpi=True` to any of the parallel functions:

```python { notest }
from scinexus import parallel

results = parallel.map(is_prime, PRIMES, use_mpi=True, max_workers=PBS_NCPUS)
```

Or with app pipelines:

```python { notest }
result = app.apply_to(dstore, parallel=True, par_kw=dict(use_mpi=True, max_workers=4))
```

To run an MPI script, invoke it via `mpiexec`:

```bash
mpiexec -n $PBS_NCPUS python3 -m mpi4py.futures my_script.py
```

!!! note

    You can use MPI for parallel execution on a single computer too. This can be useful for testing your code locally before migrating to a larger system.

### MPI script structure

MPI scripts must guard the main logic behind `if __name__ == "__main__":`:

```python { notest }
import os
from scinexus import parallel

PBS_NCPUS = int(os.environ["PBS_NCPUS"])


def process(data): ...


if __name__ == "__main__":
    results = parallel.map(process, my_data, use_mpi=True, max_workers=PBS_NCPUS)
```
