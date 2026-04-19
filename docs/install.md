# Installation

!!! abstract ""

    How to install `scinexus` and its optional extras for parallel execution, progress bars, and MPI support.

## Basic install

```bash
pip install scinexus
```

## Optional extras

- `pip install "scinexus[loky]"` -- uses the [loky](https://loky.readthedocs.io/) library for parallel execution. Loky provides reusable process pools that are more robust than the stdlib `ProcessPoolExecutor`, particularly in Jupyter notebooks where standard multiprocessing can fail. Recommended for interactive and notebook-based workflows.
- `pip install "scinexus[rich]"` -- also installs the `rich` package for its progress bars ([see using rich](howto/track-progress.md))
- `pip install "scinexus[mpi]"` -- MPI parallel execution via `mpi4py`

You can combine extras:

```bash
pip install "scinexus[loky,rich]"
```

## Requirements

- Python 3.11+

## Verify installation

```python
import scinexus

print(scinexus.__version__)
```
