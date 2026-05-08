import os
import pathlib
import shutil
import subprocess
import sys

import nox

# on python >= 3.12 this will improve speed of test coverage a lot
if sys.version_info >= (3, 12):
    os.environ["COVERAGE_CORE"] = "sysmon"

_py_versions = range(11, 15)

nox.options.default_venv_backend = "uv"


@nox.session(python=False)
def fmt(session: nox.Session) -> None:
    session.run("ruff", "check", "--fix-only", ".", external=True)
    session.run("ruff", "format", ".", external=True)


@nox.session(python="3.14")
def cogdocs(session: nox.Session) -> None:
    session.install("-e", ".", "--group", "dev")
    cmnd = 'find docs -name "*.md" | xargs uv run --group dev cog -r -I docs/scripts'
    subprocess.run(cmnd, check=True, shell=True)  # noqa: S602


@nox.session(python=[f"3.{v}" for v in _py_versions])
def type_check(session):
    session.install("-e", ".", "--group", "dev")
    session.run("mypy", "src/scinexus/")


@nox.session(python=[f"3.{v}" for v in _py_versions])
def test_types(session):
    session.install("-e", ".")
    session.run("mypy", "src/scinexus/")


@nox.session(python=[f"3.{v}" for v in _py_versions])
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


@nox.session(python=[f"3.{v}" for v in _py_versions])
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


@nox.session(python=[f"3.{v}" for v in _py_versions])
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


@nox.session(python=[f"3.{v}" for v in _py_versions])
def test_docs(session):
    session.install("-e", ".", "--group", "dev")
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
