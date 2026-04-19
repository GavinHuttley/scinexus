# Log and cite

!!! abstract ""

    How to use `scitrack` logging in apps, control logging in `apply_to`, and access citation records from composed pipelines.

## Leveraging `scitrack` for reproducibility

We reproduce here one of the examples from [scitrack](https://github.com/HuttleyLab/scitrack).

???- example "Using `scitrack` in a `click` app"

    ```python linenums="1"
    import click

    from scitrack import CachingLogger

    LOGGER = CachingLogger()


    @click.command()
    @click.option("-i", "--infile", type=click.Path(exists=True))
    @click.option("-t", "--test", is_flag=True, help="Run test.")
    def main(infile, test):
        # capture the local variables, at this point just provided arguments
        LOGGER.log_args()  # (1)!
        LOGGER.log_versions("numpy")  # (2)!
        LOGGER.input_file(infile)  # (3)!
        LOGGER.log_file_path = "some_path.log"  # (4)!


    if __name__ == "__main__":
        main()
    ```

    1. :man_raising_hand: A single statement and you have captured all the input arguments and their values, including defaults!
    2. This captures the version numbers of the packages our application depends on.
    3. This logs the path to `infile` and its md5sum.
    4. Until you assign the path where you want the file written, this content has been cached.

## Controlling logging in `apply_to`

By default, `apply_to` creates a `CachingLogger` that records the composable function, package versions, output paths, MD5 checksums of every result, and total elapsed time. The log is then written into the output data store. This is the recommended setting for production analyses because it gives you a complete, self-contained record of what ran and what it produced.

```python
result = process.apply_to(dstore)  # logger=True by default
```

You can also pass your own `CachingLogger` instance if you want to configure it beforehand or reuse one across multiple calls.

```python
from scitrack import CachingLogger

LOGGER = CachingLogger()
LOGGER.log_args()
result = process.apply_to(dstore, logger=LOGGER)
```

### Disabling logging

Set `logger=False` to skip logging entirely.

```python
result = process.apply_to(dstore, logger=False)
```

This is useful when:

- **Your project is small** and a full provenance log is unnecessary.
- **Logging is handled externally**, for example by a workflow manager or your own `CachingLogger` that wraps several `apply_to` calls.
- **You want to avoid the overhead** of computing an MD5 checksum for every result object, which can be noticeable for large or numerous outputs.

## Make it easy for your work to be cited

Correctly attributing the authors of algorithms and software is a requirement of good scientific practice. `scinexus` makes this easy by letting app authors declare citations that are automatically tracked through composed pipelines.

Use the `cite` parameter of `define_app` (or the base classes) to attach a citation. The `citeable` library provides several classes for this purpose.

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
r'''
from citeable import Software
from scinexus import define_app
from cogent3.app.typing import AlignedSeqsType
from cogent3 import get_app

my_cite = Software(
    author=["Doe, J", "Smith, A"],
    title="My Sequence Filter",
    year=2025,
    url="https://example.com/my-filter",
    version="0.1.0",
)

@define_app(cite=my_cite)  # (1)!
def strict_filter(val: AlignedSeqsType) -> AlignedSeqsType:
    """Remove sequences shorter than the alignment."""
    return val.omit_bad_seqs()

app = strict_filter()

loader = get_app("load_aligned", moltype="dna", format_name="fasta")
pipeline = loader + strict_filter()
print(pipeline.citations)  # (2)!
print(f"\n{pipeline.bib}")  # (3)!
'''
exec_codeblock(src=src,
               use_wrap=True,
               max_lines=2,
               annotations=["Use the `cite` parameter of `define_app` to attach a citation",
               "The `.citations` property returns citations as a tuple. When apps are composed into a pipeline, `.citations` collects unique citations from all apps in the chain.",
               "The `.bib` gives the BibTeX string."],
                admonition='???- example "Adding a citation to your app"')
  ]]] -->
???- example "Adding a citation to your app"

    ```python { linenums="1" notest }
    from citeable import Software
    from scinexus import define_app
    from cogent3.app.typing import AlignedSeqsType
    from cogent3 import get_app

    my_cite = Software(
        author=["Doe, J", "Smith, A"],
        title="My Sequence Filter",
        year=2025,
        url="https://example.com/my-filter",
        version="0.1.0",
    )


    @define_app(cite=my_cite)  # (1)!
    def strict_filter(val: AlignedSeqsType) -> AlignedSeqsType:
        """Remove sequences shorter than the alignment."""
        return val.omit_bad_seqs()


    app = strict_filter()

    loader = get_app("load_aligned", moltype="dna", format_name="fasta")
    pipeline = loader + strict_filter()
    print(pipeline.citations)  # (2)!
    print(f"\n{pipeline.bib}")  # (3)!

    # (Software(     author=['Doe, J', 'Smith, A'],     title='My Sequence Filter',
    # year=2025,     version='0.1.0',     url='https://example.com/my-filter', [...]
    ```

    1. Use the `cite` parameter of `define_app` to attach a citation
    2. The `.citations` property returns citations as a tuple. When apps are composed into a pipeline, `.citations` collects unique citations from all apps in the chain.
    3. The `.bib` gives the BibTeX string.
<!-- [[[end]]] -->

## Extracting citations from a data store

When a composed pipeline is run via `apply_to()`, citations are automatically saved in the output data store.

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
from citeable import Software
from scinexus import define_app, open_data_store
from cogent3.app.typing import AlignedSeqsType
from cogent3 import get_app

my_cite = Software(
    author=["Doe, J"],
    title="My Sequence Filter",
    year=2025,
)

@define_app(cite=my_cite)
def strict_filter(val: AlignedSeqsType) -> AlignedSeqsType:
    return val.omit_bad_seqs()

in_dstore = open_data_store("data/raw.zip", suffix="fa", limit=5)
out_dstore = open_data_store("cited_results", suffix="fa", mode="w")

loader = get_app("load_aligned", moltype="dna", format_name="fasta")
writer = get_app("write_seqs", data_store=out_dstore, format_name="fasta")
process = loader + strict_filter() + writer
result = process.apply_to(in_dstore)

result.summary_citations  # (1)!

result.write_bib("my_analysis.bib")  # (2)!
"""
exec_codeblock(src=src, annotations=["Because we are using `cogent3`, the property returns a `cogent3` `Table` of all citations stored in the data store.",
"You can export to a BibTeX file."], admonition='???- example "Citations in data stores"')
  ]]] -->
???- example "Citations in data stores"

    ```python { linenums="1" notest }
    from citeable import Software
    from scinexus import define_app, open_data_store
    from cogent3.app.typing import AlignedSeqsType
    from cogent3 import get_app

    my_cite = Software(
        author=["Doe, J"],
        title="My Sequence Filter",
        year=2025,
    )


    @define_app(cite=my_cite)
    def strict_filter(val: AlignedSeqsType) -> AlignedSeqsType:
        return val.omit_bad_seqs()


    in_dstore = open_data_store("data/raw.zip", suffix="fa", limit=5)
    out_dstore = open_data_store("cited_results", suffix="fa", mode="w")

    loader = get_app("load_aligned", moltype="dna", format_name="fasta")
    writer = get_app("write_seqs", data_store=out_dstore, format_name="fasta")
    process = loader + strict_filter() + writer
    result = process.apply_to(in_dstore)

    result.summary_citations  # (1)!

    result.write_bib("my_analysis.bib")  # (2)!
    ```

    1. Because we are using `cogent3`, the property returns a `cogent3` `Table` of all citations stored in the data store.
    2. You can export to a BibTeX file.
<!-- [[[end]]] -->

!!! note
    `ReadOnlyDataStoreZipped` supports reading stored citations but not writing them.
