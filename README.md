[![Coverage Status](https://coveralls.io/repos/github/cogent3/scinexus/badge.svg?branch=main)](https://coveralls.io/github/cogent3/scinexus?branch=main)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/fd8810efd4f142069bd84144e14350b4)](https://app.codacy.com/gh/cogent3/scinexus/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![CI](https://github.com/cogent3/scinexus/actions/workflows/ci.yml/badge.svg)](https://github.com/cogent3/scinexus/actions/workflows/ci.yml)

# scinexus

*A composable app infrastructure for scientific computing. What dataclasses are for structured data, `scinexus` apps are for structured algorithms.*

Many scientific problems require repeating calculations across many files or database records. Such tasks suit data-level parallelism, but writing robust, maintainable code for them is often tedious and quickly becomes complex.

As the Unix philosophy articulates, writing algorithms that do one thing well and can be composed together through piping data of known type is a *Very Good Thing*™.

**`scinexus` encourages this design pattern.** We leverage the Python type annotation system to govern the compatibility (composability) of different applications. This enables in-process composition of your applications with validation of the consistency of the pipeline and the consistency of the data being run through it.

**`scinexus` is designed for scientific reproducibility.** Scientific computations should record all conditions needed to reproduce an analysis. `scinexus` reduces the effort by intercepting all arguments (including defaults) used in app construction and logging the resulting app state.

## Examples

Developers can choose inheriting from a base class or use the `scinexus.define_app` decorator to make composable apps. The following examples show simple composition

<details>
<summary>Load files, those missing data don't cause a crash</summary>

```python
from scinexus import define_app

@define_app(app_type="loader")
def read_json(path: str) -> dict:
    import json
    with open(path) as f:
        return json.load(f)

@define_app
def validate(data: dict, required_field: str) -> dict:
    if required_field not in data:
        raise ValueError(f"missing {required_field!r} field")  # becomes NotCompleted, doesn't crash
    return data

app = read_json() + validate(required_field="name")
```

You can apply `app` to a single file path as `app(filepath)`, or operate in parallel (and show a progress bar) on a sequence of file paths as

```python
results = list(app.as_completed(["some_file_path.json", "some_other_file_path.json"], parallel=True, show_progress=True)
```

</details>

<details>
<summary>A contrived numerical example</summary>

```python
from scinexus import define_app


@define_app
def normalise(values: list[float]) -> list[float]:
    lo, hi = min(values), max(values)
    return [(v - lo) / (hi - lo) for v in values]

@define_app
def threshold(values: list[float]) -> list[bool]:
    return [v > 0.5 for v in values]

app = normalise() + threshold()
app([1.0, 5.0, 3.0, 9.0])
```

</details>

<details>
<summary>A configurable app</summary>

```python
from scinexus import define_app


@define_app(app_type="loader")
def load_csv(path: str) -> list[dict]:
    import csv

    with open(path) as f:
        return list(csv.DictReader(f))

@define_app
class summarise:
    def __init__(self, column: str) -> None:
        """column contains the values to produce summary stats for"""
        self.column = column

    def main(self, rows: list[dict]) -> dict[str, float]:
        vals = [float(r[self.column]) for r in rows]
        return {"mean": sum(vals) / len(vals), "min": min(vals), "max": max(vals)}

app = load_csv() + summarise(column="price")
```

</details>

## Features

- Type checking at composition time
- Durable computing -- failures recorded as `NotCompleted` records, not exceptions
- Data-level parallel execution via `loky` or MPI
- Progress bars (`tqdm` or `rich`)
- Automated logging and citation tracking
- Checkpointing via data stores (directory, SQLite)

## Installation

```bash
pip install scinexus
```

## History

The app framework and utility functions in `scinexus` incubated inside [cogent3](https://github.com/cogent3/cogent3) from March 2019, accumulating over five years of development, testing, and real-world use in computational genomics before being extracted into a standalone package. The design is mature and has underpinned analyses in published studies.

The extraction into `scinexus` makes the infrastructure available to any scientific Python project, free of the `cogent3` dependency.

We acknowledge here that many members of the `cogent3` community contributed to the code that now lives here, including [@rmcar17](https://github.com/rmcar17), [@Nick-Foto](https://github.com/Nick-Foto), [@KatherineCaley](https://github.com/KatherineCaley), [@fredjaya](https://github.com/fredjaya), and [@khiron](https://github.com/khiron).
