[![Coverage Status](https://coveralls.io/repos/github/cogent3/scinexus/badge.svg?branch=main)](https://coveralls.io/github/cogent3/scinexus?branch=main)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/fd8810efd4f142069bd84144e14350b4)](https://app.codacy.com/gh/cogent3/scinexus/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![CI](https://github.com/cogent3/scinexus/actions/workflows/ci.yml/badge.svg)](https://github.com/cogent3/scinexus/actions/workflows/ci.yml)

# scinexus

*A composable app infrastructure for scientific computing. What dataclasses are for structured data, `scinexus` apps are for structured algorithms.*

Many scientific problems require repeating calculations across many files or database records. Such tasks suit data-level parallelism on multi-core CPUs, but writing robust, maintainable code for them is often tedious and quickly becomes complex.

As the robustness of POSIX based operating systems (think Linux, Mac OS, Unix) can attest, writing algorithms that can stitched together through piping data of known type is a *Very Good Thing*™.

**`scinexus` encourages this design pattern.** We leverage the Python type annotation system to govern the compatibility (composability) of different applications. This enables in-process composition of your applications with validation of the consistency of the pipeline and the consistency of the data being run through it.

**`scinexus` is designed for scientific reproducibility.** Scientific computations should record all conditions needed to reproduce an analysis. `scinexus` reduces the effort by intercepting all arguments (including defaults) used in app construction and logging the resulting app state.

## Features

- Type checking at composition time
- Durable computing -- failures recorded as `NotCompleted` records, not exceptions
- Data-level parallel execution via `loky` or MPI
- Progress bars (`tqdm` or `rich`)
- Automated logging and citation tracking
- Checkpointing via data stores (directory, zip, SQLite)

## Installation

```bash
pip install scinexus
```

## History

The app framework and utility functions in `scinexus` incubated inside [cogent3](https://github.com/cogent3/cogent3) from March 2019, accumulating over five years of development, testing, and real-world use in computational genomics before being extracted into a standalone package. The design is mature and has underpinned analyses in published studies.

The extraction into `scinexus` makes the infrastructure available to any scientific Python project, free of the `cogent3` dependency.

We acknowledge here that many members of the `cogent3` community contributed to the code that now lives here, including [@rmcar17](https://github.com/rmcar17), [@Nick-Foto](https://github.com/Nick-Foto), [@KatherineCaley](https://github.com/KatherineCaley), [@fredjaya](https://github.com/fredjaya), and [@khiron](https://github.com/khiron).
