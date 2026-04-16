# `scinexus`: composable apps for scientific computing

The main goal of `scinexus` is to make it easier to write rigorous scientific algorithms. What `dataclasses` and `attrs` are for structured data, `scinexus` apps are for structured algorithms.

Many scientific problems require repeating calculations across many files or database records. Such tasks suit data-level parallelism on multi-core CPUs, but writing robust, maintainable code for them is often tedious and quickly becomes complex.

With `scinexus` apps, you can use a functional programming style when developing your application. Combined with `scinexus` app composition, this greatly simplifies your programming logic making it easier to understand and thus easier to explain. And as we know

!!! quote
	If the implementation is easy to explain, it may be a good idea.
	
	-- Tim Peters

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
