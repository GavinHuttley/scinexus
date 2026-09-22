# Files: open_, atomic_write, streaming

These are usable on their own, with no app and no data store. Import them from `scinexus.io_util`, except `open_`, which is also on the package.

## open_

`open_(filename, mode="rt", **kwargs)` opens a path or a URL and handles `.gz`, `.bz2`, `.xz`, `.lzma` and `.zip` by suffix.

Two things differ from what the standard library does:

- a bare `"r"` or `"w"` means **text**, for a compressed file as much as for a plain one. `gzip`, `bz2` and `lzma` take a bare mode as binary, and `open_` does not. Use `"rb"` / `"wb"` for bytes. This applies to `"r"` and `"w"` alone: `"a"` is still binary for the compressed formats, and neither `"a"` nor `"x"` works on a zip at all
- on a text read with no `encoding=` given, the encoding is sniffed from the first hundred bytes with `charset_normalizer` rather than assumed to be the locale default

```python
import gzip
import tempfile
from pathlib import Path

from scinexus import open_

path = Path(tempfile.mkdtemp()) / "data.txt.gz"
with gzip.open(path, "wt") as out:
    out.write("one\ntwo\n")

with open_(path) as infile:
    assert infile.read() == "one\ntwo\n"  # str, not bytes
```

A URL is detected by `is_url` (`http`, `https` and `file` schemes) and handed to `open_url`, which decompresses by suffix and takes the charset from the response headers. `get_format_suffixes("data.txt.gz")` returns `("txt", "gz")`, the pair `open_` dispatches on. `path_exists(p)` is false for a URL and for anything that is not there.

`open_zip` handles a single-member archive: a read of one holding more than one record raises `ValueError`, and a text read falls back to `latin-1`, which decodes any byte.

## atomic_write

Writes to a temporary file and renames it into place on success, so a reader never sees a half-written file and a failure leaves nothing behind.

```python
import tempfile
from pathlib import Path

from scinexus.io_util import atomic_write

target = Path(tempfile.mkdtemp()) / "result.txt"
try:
    with atomic_write(target) as out:
        out.write("partial")
        raise RuntimeError("something went wrong")
except RuntimeError:
    pass

assert not target.exists()  # and no temporary left either
```

It takes `mode`, `encoding`, `tmpdir`, and `in_zip` for writing a member into a zip archive. Further arguments for the underlying open go in `open_kwargs=` as a dict, so that a name like `tmpdir` cannot bind to the wrong parameter.

## Streaming a large file

Three functions, all reading in bounded chunks rather than loading the file.

| function | yields | default chunk |
| --- | --- | --- |
| `iter_splitlines(path, chunk_size, *, as_bytes=False)` | one line at a time | 1 MB |
| `iter_line_blocks(path, num_lines=1000, chunk_size, *, as_bytes=False)` | lists of `num_lines` lines | 5 MB |
| `iter_record_chunks(*, path, delimiter, chunk_size)` | bytes between delimiters | 5 MB |

`chunk_size` counts bytes read in one go, in both modes, and for a compressed file it counts bytes coming out of the decompression - the memory the read costs, not the size on disk. A trailing empty line is never yielded, so a file ending in a newline gives the same lines as one that does not.

The bound does not hold for a URL. All three set `chunk_size = None` for one and read the whole response in a single call, so streaming a large remote file this way costs its full size in memory.

The two modes of `iter_splitlines` do not always agree on how many lines a file has. `str.splitlines()` breaks on eleven characters, `bytes.splitlines()` on three. For a file holding `b"a\x0bb\nc\n"`:

```
as_bytes=False  ->  ['a', 'b', 'c']
as_bytes=True   ->  [b'a\x0bb', b'c']
```

If the file might hold a vertical tab, a form feed or a Unicode line separator, read it as bytes and decode yourself.

`iter_record_chunks` is keyword-only, takes a `bytes` delimiter and yields `bytes`. The first item is whatever precedes the first delimiter, which for a file that starts with one is empty: for a FASTA file split on `b">"` the items are `b""`, `b"a\nACGT\n"`, `b"b\nTTTT\n"`. Filter that first item yourself.
