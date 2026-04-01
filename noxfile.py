import os
import pathlib
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


@nox.session(python=[f"3.{v}" for v in _py_versions])
def type_check(session):
    session.install("-e", ".", "--group", "test")
    session.run("mypy", "src/scinexus/")


@nox.session(python=[f"3.{v}" for v in _py_versions])
def test_types(session):
    session.install("-e", ".", "--group", "test")
    session.run("mypy", "src/scinexus/")


@nox.session(python=[f"3.{v}" for v in _py_versions])
def test(session):
    session.install("-e", ".", "--group", "test")
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
    session.install("-e", ".[mpi]", "--group", "test")
    session.chdir("tests")
    py = pathlib.Path(session.bin_paths[0]) / "python"
    session.run(
        "mpiexec",
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
