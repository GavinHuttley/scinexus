import os
import pathlib
import re
import sys
import textwrap
import urllib.request
import zipfile
from io import StringIO

import cog

ROOT_DIR = pathlib.Path(__file__).parent.parent
DOCS_DIR = ROOT_DIR
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

DATA_URL = "https://github.com/user-attachments/files/26728407/raw.zip"
DECOMPRESS_URL = (
    "https://github.com/user-attachments/files/26769518/demo-locked.sqlitedb.zip"
)


def count_pattern(text: str) -> int:
    return len(re.findall(r"# \((\d+)\)!", text))


def _register_cogent3_types() -> None:
    try:
        from cogent3.app.typing import _get_resolution_namespace

        from scinexus.typing import register_type_namespace

        register_type_namespace(_get_resolution_namespace)
    except ImportError:
        pass


_register_cogent3_types()


def setup_installed() -> None:
    zip_dest = DATA_DIR / "raw.zip"
    if not zip_dest.exists():
        urllib.request.urlretrieve(DATA_URL, filename=zip_dest)  # noqa: S310
        return

    decompress_dest = DATA_DIR / "demo-locked.sqlitedb.zip"
    if not decompress_dest.exists():
        urllib.request.urlretrieve(DECOMPRESS_URL, filename=decompress_dest)  # noqa: S310
        with zipfile.ZipFile(decompress_dest, "r") as zip_ref:
            zip_ref.extractall(DATA_DIR)
        return


def exec_codeblock(
    *,
    src: str,
    lang: str = "python",
    width: int = 80,
    max_lines: int = 10,
    admonition: str | None = None,
    use_wrap: bool = True,
    display_src: bool = True,
    annotations: list[str] | None = None,
) -> None:
    """Execute code lines, then emit a fenced code block with source + output.

    Parameters
    ----------
    lines
        List of code strings (one per line).
    lang
        Language for the emitted code block (default: "python").
    width
        Wrap width for output lines.
    max_lines
        Maximum number of lines to emit for output (after wrapping).
    use_wrap
        Whether to wrap the output lines.
    admonition
        Optional admonition type to wrap the code block in (e.g. "note", "tip", etc.).
    display_src
        Whether to include the source code in the emitted code block.
    annotations
        Optional list of annotation strings to include as comments in the emitted code block.
    """
    setup_installed()
    cwd = os.getcwd()  # noqa: PTH109
    os.chdir(ROOT_DIR)
    wd = os.getcwd()
    lines = src.splitlines()
    if not lines[0]:
        lines = lines[1:]

    if not lines[-1]:
        lines = lines[:-1]

    src = "\n".join(lines)
    buf = StringIO()
    sys.stdout = buf
    try:
        ns: dict = {}
        exec(src, ns)  # noqa: S102
    except Exception as exc:
        msg = f"Error executing code block: \n{src}\n{exc}\ncwd={wd}\n"
        raise Exception(msg) from exc

    sys.stdout = sys.__stdout__

    output = [f'```{lang} {{ linenums="1" notest }}']
    output_text = buf.getvalue().strip()
    if display_src:
        output.append(src)
    if use_wrap and output_text:
        output.extend(
            [
                "",
                *[
                    f"# {l}"
                    for l in textwrap.wrap(
                        output_text, width=width, max_lines=max_lines
                    )
                ],
            ]
        )
    elif output_text:
        output.extend(["", output_text])

    output.append("```")
    if annotations:
        expect = count_pattern(src)
        if expect != len(annotations):
            msg = f"Number of annotations ({len(annotations)}) does not match expected ({expect}) based on pattern count in source."
            raise ValueError(msg)

        annotation_text = "\n".join(f"{i}. {a}" for i, a in enumerate(annotations, 1))
        output.extend(["", annotation_text])

    txt = "\n".join(output)

    if admonition:
        txt = textwrap.indent(txt, "    ")
        txt = f"{admonition}\n\n{txt}"

    cog.outl(txt, dedent=False)
    os.chdir(cwd)
