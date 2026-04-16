# Processing a dataset

*Use `open_data_store` with a loader, processor, and writer app to batch-process a directory of files `apply_to`, and enable progress bars and parallel execution. Then inspect results.*

We will translate the DNA sequences in `raw.zip` into amino acid and store them as a sqlite database. We will interrogate the generated data store to get a synopsis of the results.

<!-- [[[cog
import pathlib
from cog_utils import exec_codeblock

pathlib.Path("docs/translated.sqlitedb").unlink(missing_ok=True)

src = \
r"""
from scinexus import open_data_store
from cogent3 import get_app

in_dstore = open_data_store("data/raw.zip", suffix="fa")  # (1)!
out_dstore = open_data_store("translated.sqlitedb", mode="w")  # (2)!

load = get_app("load_unaligned", moltype="dna")
translate = get_app("translate_seqs")
write = get_app("write_db", data_store=out_dstore)
app = load + translate + write  # (3)!

out_dstore = app.apply_to(in_dstore)  # (4)!

out_dstore.describe  # (5)!
out_dstore.validate()  # (6)!
out_dstore.summary_not_completed  # (7)!
"""
exec_codeblock(src=src,
admonition='???+ example "Translating DNA to amino acid"',
annotations=[
"Open the zipped input data store, selecting `.fa` files as members.",
"Create a writable SQLite output data store. Using a single database file is more efficient than writing many small files.",
"Compose loader, translator, and writer into a single pipeline.",
"Apply the pipeline to every member of the input data store. Results are written to `out_dstore`.",
"Summary showing counts of completed records, not-completed records, and log files.",
"Verify the integrity of all records via MD5 checksums.",
"Summary of why some records could not be processed — e.g. sequences not divisible by 3 or containing stop codons.",
])

pathlib.Path("docs/translated.sqlitedb").unlink(missing_ok=True)
  ]]] -->
???+ example "Translating DNA to amino acid"

    ```python { linenums="1" notest }
    from scinexus import open_data_store
    from cogent3 import get_app

    in_dstore = open_data_store("data/raw.zip", suffix="fa")  # (1)!
    out_dstore = open_data_store("translated.sqlitedb", mode="w")  # (2)!

    load = get_app("load_unaligned", moltype="dna")
    translate = get_app("translate_seqs")
    write = get_app("write_db", data_store=out_dstore)
    app = load + translate + write  # (3)!

    out_dstore = app.apply_to(in_dstore)  # (4)!

    out_dstore.describe  # (5)!
    out_dstore.validate()  # (6)!
    out_dstore.summary_not_completed  # (7)!
    ```

    1. Open the zipped input data store, selecting `.fa` files as members.
    2. Create a writable SQLite output data store. Using a single database file is more efficient than writing many small files.
    3. Compose loader, translator, and writer into a single pipeline.
    4. Apply the pipeline to every member of the input data store. Results are written to `out_dstore`.
    5. Summary showing counts of completed records, not-completed records, and log files.
    6. Verify the integrity of all records via MD5 checksums.
    7. Summary of why some records could not be processed — e.g. sequences not divisible by 3 or containing stop codons.
<!-- [[[end]]] -->

!!! note

    The `.completed` and `.not_completed` attributes give access to the different types of members, while `.members` gives them all. For example, `len(out_dstore.not_completed)` returns the count of failed records and each element is a `DataMember`.
