# `scinexus`: composable apps for scientific computing

The main goal of `scinexus` is to make it easier to write rigorous scientific algorithms. What dataclasses and attrs are for structured data, `scinexus` apps are for structured algorithms.

With `scinexus` apps, you can adopt a functional programming style within the development of your application. With app composition, you can greatly simplify your programming logic by producing "mini pipelines". 

!!! info
    `scinexus` is not intended to replace tools like [Snakemake](https://snakemake.readthedocs.io/en/stable/).

## What you get

- Type checking at composition time
- Durable computing, with failures automatically recorded as `NotCompleted` records
- Greatly simplified data level parallel execution
- Builtin progress bars (`tqdm` or `rich`)
- Automated logging
- Automated citation tracking
- Checkpointing via data stores

## Standalone utilities

`scinexus` also provides generally useful utilities for developers of data analysis applications.  utilities for file IO, parallel execution, and progress tracking are usable independently of the app framework.

## Get started

- **Install `scinexus`** -- [installing from pypi](install.md)
- **Build algorithms** -- start with [how to write apps](howto/write-a-function-app.md)
- **Build applications for others** -- read [Why composable apps?](explanation/why-composable-apps.md)
- **Use existing apps** -- see [Composing apps](tutorials/composing-apps.md)
