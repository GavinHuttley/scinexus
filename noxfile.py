import os
import pathlib
import shutil
import subprocess
import sys

import nox

# on python >= 3.12 this will improve speed of test coverage a lot
if sys.version_info >= (3, 12):
    os.environ["COVERAGE_CORE"] = "sysmon"

# "3.14t" is the free-threaded (no-GIL) build; uv can fetch it as
# cpython-3.14.x+freethreaded. It is exploratory here, not yet a support claim.
#
# 3.14t is the only free-threaded build the dev group installs into. The group
# pins cogent3, which requires numba on every version it supports, and numba
# requires llvmlite. Both publish free-threaded wheels for 3.14t alone, so on
# 3.13t uv falls back to building llvmlite from source and that needs an LLVM
# toolchain. The 3.13t interpreter itself installs fine, and test_types would
# run there because it installs mypy alone. Adding a free-threaded version
# once its wheels exist means this list and the matrices named in the note at
# the top of ci.yml.
_py_versions = [f"3.{v}" for v in range(11, 15)] + ["3.14t"]

nox.options.default_venv_backend = "uv"


@nox.session(python=False)
def fmt(session: nox.Session) -> None:
    session.run("ruff", "check", "--fix-only", ".", external=True)
    session.run("ruff", "format", ".", external=True)


@nox.session(python="3.14")
def cogdocs(session: nox.Session) -> None:
    session.install("-e", ".", "--group", "dev")
    cmnd = (
        'find docs -name "*.md" | xargs '
        "uv run --group dev --group doc cog -r -I docs/scripts"
    )
    subprocess.run(cmnd, check=True, shell=True)  # noqa: S602


def _mypy(session: nox.Session) -> None:
    # each session gets its own cache: they install different packages, and a
    # cache shared with a session where an optional extra was present hides
    # the import errors the other session exists to catch
    session.run("mypy", f"--cache-dir=.mypy_cache/{session.name}", "src/scinexus/")


@nox.session(python=_py_versions)
def type_check(session):
    session.install("-e", ".", "--group", "dev")
    _mypy(session)


@nox.session(python=_py_versions)
def test_types(session):
    # mypy is installed explicitly rather than via the dev group: this session
    # deliberately type checks against runtime dependencies only. Without it
    # nox falls back to whatever mypy is on PATH, on the wrong interpreter.
    session.install("-e", ".", "mypy")
    _mypy(session)


@nox.session(python=_py_versions)
def test(session):
    session.install("-e", ".", "--group", "dev")
    session.run("uv", "pip", "list")
    # doctest modules within scinexus
    session.chdir("src/scinexus")
    session.run(
        "pytest",
        "-s",
        "-x",
        "--doctest-modules",
        ".",
    )

    session.chdir("../../tests")
    session.run(
        "pytest",
        "-s",
        "-x",
        "-m",
        "not slow and not mpi",
        *session.posargs,
    )


@nox.session(python=_py_versions)
def testmpi(session):
    session.install("-e", ".[mpi]", "--group", "dev")
    session.chdir("tests")
    py = pathlib.Path(session.bin_paths[0]) / "python"
    session.run(
        "mpiexec",
        "--oversubscribe",
        "-n",
        "4",
        str(py),
        "-m",
        "mpi4py.futures",
        "-m",
        "pytest",
        "-s",
        "-x",
        "-m",
        "mpi",
        *session.posargs,
        external=True,
    )


@nox.session(python=_py_versions)
def testcov(session):
    session.install("-e", ".", "--group", "dev")
    cover_mpi = shutil.which("mpiexec") is not None
    if cover_mpi:
        session.install("-e", ".[mpi]")

    cov_file = str(pathlib.Path.cwd() / ".coverage")
    session.env["COVERAGE_FILE"] = cov_file
    session.run("coverage", "erase")

    base = ["coverage", "run", "--source=scinexus"]

    # mypy via API wrapper so coverage traces plugin hook execution
    session.run(*base, "scripts/run_mypy_cov.py", "--no-incremental", "src/scinexus/")

    # doctests
    session.chdir("src/scinexus")
    session.run(*base, "--append", "-m", "pytest", "-s", "-x", "--doctest-modules", ".")

    # unit tests
    session.chdir("../../tests")
    session.run(
        *base,
        "--append",
        "-m",
        "pytest",
        "-s",
        "-x",
        "-m",
        "not mpi",
    )

    # MPI tests when mpiexec is available
    if cover_mpi:
        py = pathlib.Path(session.bin_paths[0]) / "python"
        session.run(
            "mpiexec",
            "--oversubscribe",
            "-n",
            "4",
            str(py),
            "-m",
            "mpi4py.futures",
            "-m",
            *base,
            "--append",
            "-m",
            "pytest",
            "-s",
            "-x",
            "-m",
            "mpi",
            external=True,
        )

    session.chdir("..")
    session.run("coverage", "report")
    i = 0
    while i < len(session.posargs):
        fmt = session.posargs[i]
        if fmt == "html":
            session.run("coverage", fmt, external=True)
            i += 1
            continue

        o_name = session.posargs[i + 1]
        session.run("coverage", fmt, o_name, external=True)
        i += 2


@nox.session(python=_py_versions)
def test_docs(session):
    session.install("-e", ".", "--group", "dev", "--group", "doc")
    session.run("uv", "pip", "list")
    # doctest modules within scinexus
    session.chdir("docs")
    session.run(
        "pytest",
        "--markdown-docs",
        "-m",
        "markdown-docs",
        "-x",
        ".",
        "--ignore",
        "scripts",
        *session.posargs,
    )
