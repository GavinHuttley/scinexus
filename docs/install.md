# Installation

## Basic install

```bash
pip install scinexus
```

## Optional extras

- `pip install scinexus[rich]` -- also installs the `rich` package for its progress bars ([see using rich](howto/track-progress.md))
- `pip install scinexus[mpi]` -- MPI parallel execution via `mpi4py`

## Requirements

- Python 3.11+

## Verify installation

```python
import scinexus

print(scinexus.__version__)
```
