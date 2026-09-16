# How the MPI backend differs

!!! abstract ""

    Why MPI is not simply a fourth process pool: the worker count is decided outside Python by `mpiexec`, rank 0 is the master and does no work, `max_workers` cannot change any of it, and `UNIVERSE_SIZE` counts slots rather than ranks.

The `"multiprocess"`, `"threads"` and `"loky"` backends all share one assumption: the running process decides how much parallelism to use, and it decides by looking at the machine it is on. `max_workers=4` means four workers, and leaving it out means one per CPU.

MPI breaks that assumption. The processes were created before Python started, by `mpiexec` or by a cluster scheduler, and nothing inside the program can add to them or give them back. Almost everything else on this page follows from that.

## The launcher decides, not the program

With a local backend the chain is short. You ask for workers, and the pool starts them:

```python { notest }
from scinexus import parallel

results = parallel.map(process, items, max_workers=4)  # four workers
```

Under MPI that same argument is a request about an allocation that already exists and cannot be changed. The number that decides your parallelism is the one you gave the launcher:

```bash
mpiexec -n 8 python -m mpi4py.futures my_script.py
```

So under MPI, leave `max_workers` out and let `mpiexec -n` be the single place the worker count is set.

## Rank 0 is the master and does no work

Every MPI process is a *rank*. Rank 0 is the master: it holds the data, hands out tasks and collects results, and does not process items itself. A job of 8 ranks therefore has **7 workers**, not 8.

This has no equivalent in the local backends, where a pool of 4 workers on a 4 core machine puts all 4 to work while the parent simply waits. It is also why `max_workers=$PBS_NCPUS` is the wrong instinct on a cluster: it asks for one more worker than the job can ever contain.

## Why the ranks must exist before the program starts

`scinexus` drives every backend through `concurrent.futures`, and for MPI that means the ranks are launched up front and then handed to the executor. This is what `-m mpi4py.futures` does:

```bash
mpiexec -n 4 python -m mpi4py.futures my_script.py
```

Without `-m mpi4py.futures`, `mpi4py` can instead spawn workers on demand while the program runs. `scinexus` is not written for that arrangement and is not tested against it, so treat the invocation above as the supported one.

## `max_workers` cannot change the worker count

Because the pool is fixed before your code runs, `max_workers` has no way to grow or shrink it. Asking for more does not start more, and asking for fewer does not leave any idle. Measured on a 4 rank job, worker counts of 1, 2, 99 and `None` all put work on the same three ranks.

`scinexus` therefore treats the argument as a statement about an allocation rather than a request for one:

- **Left out**, you get the pool the job was launched with. This is the right choice almost always.
- **Equal to the pool size**, it is accepted quietly, since it agrees with reality.
- **Anything else**, you get a warning saying so, in both directions. Asking for fewer used to run on the whole pool in silence, which is the more surprising of the two.
- **Below one, or not a whole number**, it is refused, exactly as the local backends refuse it.

## `UNIVERSE_SIZE` counts slots, not ranks

MPI exposes an attribute called `UNIVERSE_SIZE`, which is easy to mistake for the number of processes. It is the number of *slots* the job may occupy, which usually means cores on the machine or slots granted by the scheduler.

The two numbers are independent, and neither is the worker count. Measured on a six core machine:

| launch | `UNIVERSE_SIZE` | ranks | workers |
|---|---|---|---|
| `mpiexec -n 4 -m mpi4py.futures` | 6 | 4 | 3 |
| `mpiexec --oversubscribe -n 8 -m mpi4py.futures` | 6 | 8 | 7 |
| `mpiexec -n 1 -m mpi4py.futures` | 6 | 1 | 1 |

Running more ranks than there are slots is **oversubscription**, which `mpiexec --oversubscribe` permits deliberately. It is a legitimate thing to do, for testing on a small machine or for work that spends its time waiting rather than computing, and the third row shows the other end: a single rank serves as its own worker rather than leaving none.

!!! note

    `get_size()` reports the last column, the workers you will actually get. It does not report `UNIVERSE_SIZE` and it does not report the rank count.

## What carries over unchanged

Two things behave as they do for the process backends rather than being MPI peculiarities. Arguments and results are pickled, so closures and lambdas are refused just as they are under `"multiprocess"`. And the main logic must sit behind an `if __name__ == "__main__":` guard, for the same reason it must under spawn based multiprocessing.

For the practical steps -- installing mpi4py, setting the backend, writing and launching the script -- see [Run in parallel](../howto/run-in-parallel.md).
