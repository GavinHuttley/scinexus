# Read and write files

!!! abstract ""

    How to use `open_()` for reading and writing files with automatic compression detection (gzip, bzip2, lzma, zip), `atomic_write` for safe file writes that clean up on failure, `iter_splitlines` and `iter_line_blocks` for streaming large files, and `is_url`/`open_url` for working with URLs.

## Writing a compressed file

`open_()` detects the compression format from the file suffix and handles it automatically. Writing a gzip-compressed text file is identical to writing a plain text file — just use a `.gz` suffix.

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
from scinexus import open_

with open_("data/sample.txt.gz", "wt") as f:
    f.write("Hello, compressed world!\\n")
    f.write("Line two of the file.\\n")
"""
exec_codeblock(src=src)
  ]]] -->
```python { linenums="1" notest }
from scinexus import open_

with open_("data/sample.txt.gz", "wt") as f:
    f.write("Hello, compressed world!\n")
    f.write("Line two of the file.\n")
```
<!-- [[[end]]] -->

## Reading a compressed file

Reading works the same way — `open_()` detects the `.gz` suffix and decompresses transparently.

<!-- [[[cog
from cog_utils import exec_codeblock, DOCS_DIR

src = \
"""
from scinexus import open_

with open_("data/sample.txt.gz") as f:
    print(f.read())
"""
exec_codeblock(src=src)

import pathlib
pathlib.Path(DOCS_DIR / "data/sample.txt.gz").unlink(missing_ok=True)
  ]]] -->
```python { linenums="1" notest }
from scinexus import open_

with open_("data/sample.txt.gz") as f:
    print(f.read())

# Hello, compressed world! Line two of the file.
```
<!-- [[[end]]] -->

Supported compression formats are gzip (`.gz`), bzip2 (`.bz2`), lzma (`.xz`, `.lzma`), and zip (`.zip`).

## Reading a URL

`open_()` also handles URLs. Use `is_url()` to check whether a path is a URL before opening it.

<!-- [[[cog
import cog
from cog_utils import DATA_URL

cog.outl(f'''???- example "Checking and reading a URL"

    ```python linenums="1"
    from scinexus.io_util import is_url
    from scinexus import open_

    url = "{DATA_URL}"
    print(is_url(url))  # (1)!

    with open_(url, "rb") as f:  # (2)!
        header = f.read(20)
    print(header)
    ```

    1. `is_url()` returns `True` for `http`, `https`, and `file` scheme URLs.
    2. `open_()` detects the URL and delegates to `open_url()`. Only read mode is supported for URLs.
''', dedent=False)
  ]]] -->
???- example "Checking and reading a URL"

    ```python linenums="1"
    from scinexus.io_util import is_url
    from scinexus import open_

    url = "https://github.com/user-attachments/files/26728407/raw.zip"
    print(is_url(url))  # (1)!

    with open_(url, "rb") as f:  # (2)!
        header = f.read(20)
    print(header)
    ```

    1. `is_url()` returns `True` for `http`, `https`, and `file` scheme URLs.
    2. `open_()` detects the URL and delegates to `open_url()`. Only read mode is supported for URLs.

<!-- [[[end]]] -->

## Efficiently reading large files

Reading an entire large file into memory or iterating line by line with Python's built-in `readline()` can be inefficient. The built-in approach makes a system call for every line, which becomes a bottleneck for files with millions of lines. `scinexus` provides two functions that read data in large chunks and then split into lines, greatly reducing I/O overhead.

### `iter_splitlines`

`iter_splitlines(path, chunk_size=1_000_000)` reads a file in chunks (default 1 MB) and yields individual lines. It correctly handles lines that span chunk boundaries.

```python { notest }
from scinexus.io_util import iter_splitlines

for line in iter_splitlines("large_file.txt"):
    process(line)
```

### `iter_line_blocks`

`iter_line_blocks(path, num_lines=1000, chunk_size=5_000_000)` builds on `iter_splitlines` — it accumulates lines into lists of `num_lines` and yields each list. This is useful when downstream processing works on batches (e.g. FASTA records where each record spans a fixed number of lines).

```python { notest }
from scinexus.io_util import iter_line_blocks

for block in iter_line_blocks("large_file.txt", num_lines=1000):
    process_batch(block)  # block is a list of up to 1000 strings
```

Use `iter_splitlines` when you need one line at a time. Use `iter_line_blocks` when your processing naturally operates on batches of lines.
