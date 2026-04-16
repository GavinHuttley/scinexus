# Use data stores

*How to use `open_data_store` in read, write, and append modes with directory, zip, and SQLite backends, iterate over members, and inspect `.completed`, `.not_completed`, and `.summary_<methods>`.*

## How do I use a data store?

A data store is just a "container". To open a data store you use the `open_data_store()` function. To load the data for a member of a data store you need an appropriately selected loader type of app.

## Supported operations on a data store

All data store classes can be iterated over, indexed, checked for membership. These operations return a `DataMember` object. In addition to providing access to members, the data store classes have convenience methods for describing their contents and providing summaries of log files that are included and of the `NotCompleted` members (see not completed).

## Opening a data store

Use the `open_data_store()` function, illustrated below. Use the mode argument to identify whether to open as read only (`mode="r"`), write (`mode="w"`) or append(`mode="a"`).

### Opening a read only data store

We open the zipped directory described above, defining the filenames ending in ``.fa`` as the data store members. All files within the directory become members of the data store (unless we use the ``limit`` argument).

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
from scinexus import open_data_store

dstore = open_data_store("data/raw.zip", suffix="fa", mode="r")  # (1)!
print(dstore)

dstore.describe  # (2)!

m = dstore[0]  # (3)!

for m in dstore[:5]:  # (4)!
    print(m)

m.read()[:20]  # (5)!
"""
exec_codeblock(src=src,
annotations=[
"Open a data store.",
"The `.describe` property summarises the contents.",
"You can index like any Python sequence.",
"Or loop over members.",
"And read data from a member."
])
  ]]] -->
```python { linenums="1" notest }
from scinexus import open_data_store

dstore = open_data_store("data/raw.zip", suffix="fa", mode="r")  # (1)!
print(dstore)

dstore.describe  # (2)!

m = dstore[0]  # (3)!

for m in dstore[:5]:  # (4)!
    print(m)

m.read()[:20]  # (5)!

# 1035x member
# ReadOnlyDataStoreZipped(source='/Users/gavin/repos/SciNexus/docs/data/raw.zip',
# members=[DataMember(data_store=/Users/gavin/repos/SciNexus/docs/data/raw.zip,
# unique_id=ENSG00000157184.fa),
# DataMember(data_store=/Users/gavin/repos/SciNexus/docs/data/raw.zip,
# unique_id=ENSG00000131791.fa)]...) ENSG00000157184.fa ENSG00000131791.fa
# ENSG00000127054.fa ENSG00000067704.fa ENSG00000182004.fa
```

1. Open a data store.
2. The `.describe` property summarises the contents.
3. You can index like any Python sequence.
4. Or loop over members.
5. And read data from a member.
<!-- [[[end]]] -->

!!! note
    For a `DataStoreSqlite` member, the default data storage format is bytes. So reading the content of an individual record is best done using the `load_db` app.

### Making a writeable data store

The creation of a writeable data store is specified with `mode="w"`, or (to append) `mode="a"`. In the former case, any existing records are overwritten. In the latter case, existing records are ignored.

## `DataStoreSqlite` stores serialised data

When you specify a Sqlitedb data store as your output (by using `open_data_store()`) you write multiple records into a single file making distribution easier.

One important issue to note is the process which creates a Sqlitedb "locks" the file. If that process exits unnaturally (e.g. the run that was producing it was interrupted) then the file may remain in a locked state. If the db is in this state, `scinexus` will not modify it unless you explicitly unlock it.

This is represented in the display as shown below.

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
from scinexus import open_data_store, set_summary_display

set_summary_display(None)
dstore = open_data_store("data/demo-locked.sqlitedb")
print(dstore.describe)
"""
exec_codeblock(src=src, use_wrap=False, display_src=False)
  ]]] -->
```python { linenums="1" notest }
{"completed": 175, "not_completed": 0, "logs": 1, "title": "Unlocked db store."}
```
<!-- [[[end]]] -->


To unlock, you execute the following:

```python { notest }
dstore.unlock(force=True)
```

## Interrogating run logs

If you use the `apply_to()` method, a `scitrack` logfile will be stored in the data store. This includes useful information regarding the run conditions that produced the contents of the data store.

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
from scinexus import open_data_store, set_summary_display

set_summary_display(None)
dstore = open_data_store("data/demo-locked.sqlitedb")
print(dstore.summary_logs)
"""
exec_codeblock(src=src, display_src=False, max_lines=4)
  ]]] -->
```python { linenums="1" notest }
# [{'time': '2019-07-24 14:42:56', 'name': 'logs/load_unaligned-progressive_align-
# write_db-pid8650.log', 'python_version': '3.7.3', 'who': 'gavin', 'command':
# '/Users/gavin/miniconda3/envs/c3dev/lib/python3.7/site-
# packages/ipykernel_launcher.py -f [...]
```
<!-- [[[end]]] -->


Log files can be accessed via a special attribute.

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
from scinexus import open_data_store, set_summary_display

set_summary_display(None)
dstore = open_data_store("data/demo-locked.sqlitedb")
print(dstore.logs)
"""
exec_codeblock(src=src, display_src=False, max_lines=4)
  ]]] -->
```python { linenums="1" notest }
# [DataMember(data_store=/Users/gavin/repos/SciNexus/docs/data/demo-
# locked.sqlitedb, unique_id=logs/load_unaligned-progressive_align-write_db-
# pid8650.log)]
```
<!-- [[[end]]] -->


Each element in that list is a `DataMember` which you can use to get the data contents. The following

```python  { notest }
print(dstore.logs[0].read()[:225])
```

Produces

<!-- [[[cog
from cog_utils import exec_codeblock

src = \
"""
from scinexus import open_data_store, set_summary_display

set_summary_display(None)
dstore = open_data_store("data/demo-locked.sqlitedb")
print(dstore.logs[0].read()[:225])
"""
exec_codeblock(src=src, display_src=False, max_lines=4)
  ]]] -->
```python { linenums="1" notest }
# 2019-07-24 14:42:56     Eratosthenes.local:8650 INFO    system_details :
# system=Darwin Kernel Version 18.6.0: Thu Apr 25 23:16:27 PDT 2019;
# root:xnu-4903.261.4~2/RELEASE_X86_64 2019-07-24 14:42:56
# Eratosthenes.local:8650 INFO    python
```
<!-- [[[end]]] -->


## Citations – giving credit to package developers

When apps declare citations, those citations are automatically saved alongside your results when you use `apply_to()`.

<!-- [[[cog
import pathlib
import shutil
from cog_utils import exec_codeblock

src = \
"""
import pathlib
import shutil
from citeable import Software
from scinexus import define_app, open_data_store
from cogent3 import get_app
from cogent3.app.typing import AlignedSeqsType

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
result.write_bib("my_analysis.bib")
print(pathlib.Path("my_analysis.bib").read_text())
"""
exec_codeblock(src=src, max_lines=4)
pathlib.Path("docs/my_analysis.bib").unlink()
shutil.rmtree("docs/cited_results")
  ]]] -->
```python { linenums="1" notest }
import pathlib
import shutil
from citeable import Software
from scinexus import define_app, open_data_store
from cogent3 import get_app
from cogent3.app.typing import AlignedSeqsType

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
result.write_bib("my_analysis.bib")
print(pathlib.Path("my_analysis.bib").read_text())

# @software{cogent3,   author    = {Huttley, Gavin and Caley, Katherine and
# Fotovat, Nabi and Ma, Stephen Ka-Wah and Koh, Moses and Morris, Richard and
# McArthur, Robert and McDonald, Daniel and Jaya, Fred and Maxwell, Peter and
# Martini, James and La, Thomas and Lang, Yapeng},   title     = {{cogent3}: [...]
```
<!-- [[[end]]] -->

The `summary_citations` property returns a table of all citations stored in the data store (line 24). Export to BibTeX with `write_bib()` (line 26).

!!! note
    `ReadOnlyDataStoreZipped` supports reading stored citations but not writing them.
