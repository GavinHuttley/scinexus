"""setup for running the skill's markdown code blocks under pytest-markdown-docs

Excluded from what the docs site publishes, see scripts/publish_skills.py.
"""

import os
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def skill_scratch_dir(tmp_path: Path) -> Iterator[Path]:
    """run each block in a scratch directory holding the inputs it assumes

    The skeleton in SKILL.md reads a store at ``raw`` and writes one at
    ``out``, which the prose beside it states. Preparing that here keeps
    the block free of setup a reader does not need to see.
    """
    (tmp_path / "raw").mkdir()
    (tmp_path / "raw" / "alpha.txt").write_text("contents of alpha")
    previous = Path.cwd()
    os.chdir(tmp_path)
    try:
        yield tmp_path
    finally:
        os.chdir(previous)
