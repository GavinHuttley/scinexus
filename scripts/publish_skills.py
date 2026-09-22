"""publish the agent skills in skills/ into a built documentation site

Serves them at the paths the great-docs agent-skills standard specifies, so
that ``npx skills add <site url>`` and anything else following that standard
can discover them.

https://posit-dev.github.io/great-docs/user-guide/agent-skills.html

Run after the site is built, since zensical does not copy a hidden directory
out of docs/:

    uv run python scripts/publish_skills.py site
"""

import argparse
import json
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).parent.parent
SOURCE = REPO / "skills"
WELL_KNOWN = Path(".well-known") / "agent-skills"
# the path a consumer predating the agent-skills endpoint probes
LEGACY = Path(".well-known") / "skills" / "default"
# scaffolding for running the examples, which is not part of the skill
EXCLUDED = {"conftest.py", "__pycache__", ".pytest_cache"}


def read_frontmatter(path: Path) -> dict[str, str]:
    """return the YAML frontmatter of a SKILL.md as a dict of strings

    Understands the subset the standard uses: plain scalars, quoted scalars,
    and the folded or literal blocks its own example writes the description
    with. A nested map such as ``metadata`` is reported as a key with an
    empty value rather than being flattened into bogus top-level keys.
    """
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0].strip() != "---":
        msg = f"{path} has no frontmatter, which the standard requires"
        raise ValueError(msg)
    try:
        end = lines.index("---", 1)
    except ValueError:
        msg = f"{path} has no closing '---', so its frontmatter never ends"
        raise ValueError(msg) from None

    fields: dict[str, str] = {}
    key: str | None = None
    for line in lines[1:end]:
        indented = line.startswith((" ", "\t"))
        if indented and key is not None:
            # a continuation of a folded or literal block, or a member of a
            # nested map. Either way it belongs to the key above it
            fields[key] = f"{fields[key]} {line.strip()}".strip()
            continue
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        key = key.strip()
        value = value.strip()
        # "> " and "| " introduce a block whose content is on the lines below
        fields[key] = "" if value in {">", "|", ">-", "|-"} else value.strip("\"'")

    return fields


def _publishable(source: Path) -> list[Path]:
    """the files of a skill, relative to it, in a stable order

    Walked rather than listed: a companion file may sit in a subdirectory,
    which is how the reference implementation ships references and scripts.
    """
    found = []
    for path in sorted(source.rglob("*")):
        relative = path.relative_to(source)
        if EXCLUDED.intersection(relative.parts):
            continue
        if path.is_file():
            found.append(relative)
    return found


def publish_skill(source: Path, site: Path) -> dict[str, object]:
    """copy one skill directory into site and return its manifest entry"""
    front = read_frontmatter(source / "SKILL.md")
    name = front.get("name", source.name)
    if name != source.name:
        msg = f"skill {source.name!r} declares name {name!r}, they must match"
        raise ValueError(msg)

    destination = site / WELL_KNOWN / name
    # removed first, so a file dropped from the skill stops being served
    shutil.rmtree(destination, ignore_errors=True)
    published = _publishable(source)
    for relative in published:
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / relative, target)

    entry: dict[str, object] = {
        "name": name,
        "description": front.get("description", ""),
        # relative to this manifest, so the skill is found wherever the site
        # is mounted. Read the Docs serves a versioned project under a prefix
        "files": [relative.as_posix() for relative in published],
    }
    for optional in ("license", "compatibility"):
        if optional in front:
            entry[optional] = front[optional]
    return entry


def publish(site: Path) -> list[dict[str, object]]:
    """copy every skill into site and write the discovery manifest"""
    if not site.is_dir():
        msg = f"{site} does not exist, build the site before publishing into it"
        raise SystemExit(msg)

    sources = sorted(p for p in SOURCE.iterdir() if (p / "SKILL.md").exists())
    if not sources:
        msg = f"no skills found under {SOURCE}"
        raise SystemExit(msg)

    entries = [publish_skill(source, site) for source in sources]
    manifest = site / WELL_KNOWN / "index.json"
    manifest.write_text(
        json.dumps({"skills": entries}, indent=2) + "\n", encoding="utf-8"
    )

    # the single-skill endpoints the standard also names, so that a site with
    # one skill answers the simplest fetch and the older probe as well
    if len(sources) == 1:
        legacy = site / LEGACY
        legacy.mkdir(parents=True, exist_ok=True)
        shutil.copy2(sources[0] / "SKILL.md", legacy / "SKILL.md")
        shutil.copy2(sources[0] / "SKILL.md", site / "skill.md")

    return entries


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("site", type=Path, help="the built site directory")
    args = parser.parse_args(argv)
    try:
        entries = publish(args.site)
    except ValueError as invalid:
        raise SystemExit(str(invalid)) from None
    for entry in entries:
        files = entry["files"]
        count = len(files) if isinstance(files, list) else 0
        print(f"published {entry['name']}: {count} files")  # noqa: T201
    return 0


if __name__ == "__main__":
    sys.exit(main())
