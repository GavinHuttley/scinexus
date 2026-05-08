<div style="text-align: center;">
  <img src="images/logo-text-bw.svg" alt="scinexus logo" style="width: 50%; max-width: 700px;" class="only-light">
  <img src="images/logo-text-wb.svg" alt="scinexus logo" style="width: 50%; max-width: 800px;" class="only-dark">
</div>

# About

!!! abstract "*Just as `attrs` and `dataclasses` use type hints to simplify data type definition, `scinexus` uses them to simplify writing best-practice scientific algorithms.*"

`scinexus` (pronounced 'sigh-nexus') is a Python framework for rapid development of data processing applications. It enables interoperability between apps through defined data types, allowing development of scientific domain app ecosystems (for examples see [cogent3](https://cogent3.org/doc/app/index-app.html) and [piqtree](https://piqtree.readthedocs.io/en/stable/)).

Many scientific problems require repeating calculations across many files or database records. Such tasks suit data-level parallelism on multi-core CPUs, but writing robust, maintainable code for them is often tedious and quickly becomes complex.

With `scinexus` apps, you can use a functional programming style when developing your application. Combined with `scinexus` app composition, this greatly simplifies your programming logic making it easier to understand and thus easier to explain. And as we know

!!! quote
	If the implementation is easy to explain, it may be a good idea.
	
	-- Tim Peters, "Zen of Python"

## What you get

- Type checking at composition time
- Durable computing[^1]
- Greatly simplified data level parallel execution
- Automated logging
- Automated citation tracking
- Checkpointing via data stores
- Customisable experience (progress bars[^2], parallelisation[^3], data store representations etc..)

[^1]: Failures are automatically recorded as `NotCompleted` records which get propagated and stored in [data stores](explanation/not-completed-design.md). These records record salient details that help you identify the cause of the failure.
[^2]: `tqdm` is the default because of its robustness in notebooks, but you can choose `rich`
[^3]: The default is Python’s standard library `multiprocessing` module. If you're using Jupyter Notebooks, however, it's recommended that you use `loky`. This is an [installation option](install.md#optional-extras) and [configuration is easy](howto/run-in-parallel.md#choosing-a-parallel-backend).

## Standalone utilities

`scinexus` also provides generally useful utilities for developers of data analysis applications. Utilities for file IO, parallel execution, and progress tracking are usable independently of the app framework.

## Get started

- **Install `scinexus`** -- see [Installing from pypi](install.md)
- **Build algorithms** -- see [How to write apps](howto/write-a-function-app.md)
- **Build applications for others** -- see [Why composable apps?](explanation/why-composable-apps.md)
- **Use existing apps** -- see [Composing apps](tutorials/composing-apps.md)

## `scinexus` origin

The app infrastructure code was originally developed within [cogent3](https://cogent3.org), where it accumulated over seven years of development, testing, and real-world use in computational genomics before being extracted into `scinexus`. The design is mature and has underpinned analyses in published studies.

We acknowledge here that many members of the `cogent3` community contributed to the code that now lives here, including [@rmcar17](https://github.com/rmcar17), [@Nick-Foto](https://github.com/Nick-Foto), [@KatherineCaley](https://github.com/KatherineCaley), [@fredjaya](https://github.com/fredjaya), and [@khiron](https://github.com/khiron).
