# Why composable apps?

???+ quote

    Write programs that do one thing and do it well. Write programs to work together.

    — Doug McIlroy

## Make your algorithms more robust

As the robustness of POSIX based operating systems (think Linux, Mac OS, Unix) can attest, writing algorithms that stitch together multiple single purpose applications is a *Very Good Thing*™. This is most elegantly expressed as a part of the Unix design philosophy.

**`scinexus` encourages this design pattern.** We leverage the Python type annotation system to govern the compatibility (composability) of different applications. This enables in-process composition of your applications with validation of the consistency of the pipeline and the consistency of the data being run through it.

We can expand on this slightly for the problem of scientific computation by considering the critical benchmark of satisfying the conditions for reproducibile computation, i.e. the obligation to track all of the properties affecting the execution of your algorithm. Examples of this are the operating system, the language version, the seed used for the random number generator, etc.

**`scinexus` does this for you.** For example, we intercept all arguments (including default values) passed to the construction of apps and record them so that the app state is logged. If you, the developer, also leverage the capabilities of the [`scitrack`](https://pypi.org/project/scitrack/) logging package (which `scinexus` has as a dependency), you can facilitate extra information such as versions of packages that your application depends on. We provide an [example][leveraging-scitrack-for-reproducibility] of using `scitrack` for these cases.

## Improve the accessibility of your work for end users

Apps are ready-made "functions" that you can apply to your data without needing to know all the technical details. They are easy to use, even if a user is not an expert programmer. Users do not need to be experts in structural programming logic. Because can be naturally composed into "pipelines", they can be applied to a single, or thousands, of data records without writing loops or conditionals.

