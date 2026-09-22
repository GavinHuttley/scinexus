"""checks on the agent skill this repository publishes"""

import importlib.util
import json
import re
from pathlib import Path

import pytest

import scinexus

REPO = Path(__file__).parent.parent
MARKETPLACE = REPO / ".claude-plugin" / "marketplace.json"
PLUGIN = REPO / ".claude-plugin" / "plugin.json"
SKILL_DIR = REPO / "skills" / "scinexus"
WORKFLOW = REPO / ".github" / "workflows" / "docs.yml"


def _load_publisher():
    """import the publish script without putting scripts/ on sys.path"""
    path = REPO / "scripts" / "publish_skills.py"
    spec = importlib.util.spec_from_file_location("publish_skills", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


publish_skills = _load_publisher()

# a reference in SKILL.md is a file name in backticks
_REFERENCE = re.compile(r"`([\w.-]+\.(?:md|txt))`")
# the standard: 64 chars at most, lower case and hyphens, matching the
# directory name
_NAME = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")


def _skill_text() -> str:
    return (SKILL_DIR / "SKILL.md").read_text(encoding="utf-8")


def _referenced() -> set[str]:
    return set(_REFERENCE.findall(_skill_text()))


def test_skill_llms_txt_matches_docs():
    """the skill ships llms.txt verbatim, so a drifting copy fails here"""
    assert (SKILL_DIR / "llms.txt").read_bytes() == (
        REPO / "docs" / "llms.txt"
    ).read_bytes()


def test_frontmatter_meets_the_standard():
    front = publish_skills.read_frontmatter(SKILL_DIR / "SKILL.md")
    assert front["name"] == SKILL_DIR.name
    assert _NAME.match(front["name"])
    assert len(front["name"]) <= 64
    description = front["description"]
    assert 0 < len(description) <= 1024
    # the trigger vocabulary an agent matches a task against
    assert "scinexus" in description
    assert "define_app" in description


@pytest.mark.parametrize(
    "text",
    [
        "---\nname: x\ndescription: >\n  folded onto\n  two lines\n---\n",
        "---\nname: x\ndescription: |\n  literal onto\n  two lines\n---\n",
        '---\nname: x\ndescription: "quoted"\n---\n',
    ],
)
def test_frontmatter_reads_every_form_the_standard_uses(tmp_dir, text):
    """the standard's own example writes the description as a folded block"""
    path = tmp_dir / "SKILL.md"
    path.write_text(text)
    front = publish_skills.read_frontmatter(path)
    assert front["name"] == "x"
    assert front["description"]
    assert ">" not in front["description"]
    assert "|" not in front["description"]


@pytest.mark.parametrize(
    "text", ["# no frontmatter\n", "---\nname: x\n\nnever closed\n"]
)
def test_frontmatter_refuses_a_file_it_cannot_read(tmp_dir, text):
    path = tmp_dir / "SKILL.md"
    path.write_text(text, encoding="utf-8")
    # escaped: match takes a pattern, and a Windows path is backslashes, so
    # C:\Users reaches re.compile as the incomplete escape \U
    with pytest.raises(ValueError, match=re.escape(str(path))):
        publish_skills.read_frontmatter(path)


def test_every_file_the_skill_points_at_is_shipped():
    """a pointer added to SKILL.md without the file is a dead end"""
    missing = {name for name in _referenced() if not (SKILL_DIR / name).exists()}
    assert not missing


def test_every_shipped_file_is_pointed_at():
    """a file nothing points at is never read, so it is dead weight"""
    shipped = {path.name for path in SKILL_DIR.iterdir() if path.name != "SKILL.md"}
    assert shipped <= _referenced()


def test_publish_serves_every_source_file(tmp_dir):
    """compared against the source, not against the manifest it just wrote"""
    publish_skills.publish(tmp_dir)

    published = tmp_dir / publish_skills.WELL_KNOWN / "scinexus"
    expected = {path.name for path in SKILL_DIR.iterdir() if path.is_file()}
    assert {path.name for path in published.iterdir()} == expected
    for name in expected:
        assert (published / name).read_bytes() == (SKILL_DIR / name).read_bytes()


def test_publish_writes_the_standard_endpoints(tmp_dir):
    """a standard-following installer asks for these three paths"""
    entries = publish_skills.publish(tmp_dir)

    skill = (SKILL_DIR / "SKILL.md").read_bytes()
    assert (
        tmp_dir / publish_skills.WELL_KNOWN / "scinexus" / "SKILL.md"
    ).read_bytes() == skill
    assert (tmp_dir / publish_skills.LEGACY / "SKILL.md").read_bytes() == skill
    assert (tmp_dir / "skill.md").read_bytes() == skill

    manifest = tmp_dir / publish_skills.WELL_KNOWN / "index.json"
    (entry,) = json.loads(manifest.read_text(encoding="utf-8"))["skills"]
    assert entry == entries[0]
    # relative names, so they resolve wherever the site is mounted
    assert not any(name.startswith("/") for name in entry["files"])


def test_publish_serves_a_companion_in_a_subdirectory(tmp_dir):
    """the reference implementation ships references/ and scripts/"""
    source = tmp_dir / "skills" / "demo"
    (source / "references").mkdir(parents=True)
    (source / "SKILL.md").write_text("---\nname: demo\ndescription: d\n---\n")
    (source / "references" / "deep.md").write_text("deep\n")
    site = tmp_dir / "site"
    site.mkdir()

    publish_skills.SOURCE = tmp_dir / "skills"
    try:
        (entry,) = publish_skills.publish(site)
    finally:
        publish_skills.SOURCE = REPO / "skills"

    assert "references/deep.md" in entry["files"]
    assert (
        site / publish_skills.WELL_KNOWN / "demo" / "references" / "deep.md"
    ).exists()


def test_publish_stops_serving_a_file_removed_from_the_skill(tmp_dir):
    """a stale copy would be served forever, and absent from the manifest"""
    published = tmp_dir / publish_skills.WELL_KNOWN / "scinexus"
    published.mkdir(parents=True)
    (published / "gone.md").write_text("removed from the skill\n")

    publish_skills.publish(tmp_dir)

    assert not (published / "gone.md").exists()


def test_publish_excludes_test_scaffolding(tmp_dir):
    """conftest.py runs the examples, it is not part of the skill"""
    source = tmp_dir / "skills" / "demo"
    source.mkdir(parents=True)
    (source / "SKILL.md").write_text("---\nname: demo\ndescription: d\n---\n")
    (source / "conftest.py").write_text("# scaffolding\n")
    site = tmp_dir / "site"
    site.mkdir()

    publish_skills.SOURCE = tmp_dir / "skills"
    try:
        (entry,) = publish_skills.publish(site)
    finally:
        publish_skills.SOURCE = REPO / "skills"

    assert entry["files"] == ["SKILL.md"]
    assert not (site / publish_skills.WELL_KNOWN / "demo" / "conftest.py").exists()


def test_publish_refuses_an_unbuilt_site(tmp_dir):
    with pytest.raises(SystemExit):
        publish_skills.publish(tmp_dir / "never-built")


def test_publish_rejects_a_name_that_is_not_its_directory(tmp_dir):
    """the standard requires the two to match, and installers rely on it"""
    source = tmp_dir / "skills" / "scinexus"
    source.mkdir(parents=True)
    (source / "SKILL.md").write_text("---\nname: something-else\ndescription: d\n---\n")
    site = tmp_dir / "site"
    site.mkdir()

    publish_skills.SOURCE = tmp_dir / "skills"
    try:
        with pytest.raises(ValueError, match="they must match"):
            publish_skills.publish(site)
    finally:
        publish_skills.SOURCE = REPO / "skills"


def test_main_publishes_and_reports(tmp_dir, capsys):
    assert publish_skills.main([str(tmp_dir)]) == 0
    assert "scinexus" in capsys.readouterr().out


def test_the_docs_workflow_publishes_and_keeps_hidden_files():
    """both are silent failures: no skill on the site, or a 404 for every URL"""
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "scripts/publish_skills.py site" in workflow
    # upload-artifact drops hidden files by default, and .well-known is hidden
    assert "include-hidden-files: true" in workflow
    # a change to the skill has to rebuild the site that serves it
    assert "'skills/**'" in workflow


def test_marketplace_resolves_to_the_plugin_holding_the_skill():
    data = json.loads(MARKETPLACE.read_text(encoding="utf-8"))
    assert data["name"] == "scinexus"
    assert data["owner"]["name"]
    (entry,) = data["plugins"]
    assert entry["name"] == "scinexus"
    # the plugin root is the repository root, so one copy of the skill serves
    # the plugin and the published site
    root = (REPO / entry["source"]).resolve()
    assert root == REPO.resolve()
    plugin = json.loads(PLUGIN.read_text(encoding="utf-8"))
    assert plugin["name"] == entry["name"]
    for declared in plugin["skills"]:
        assert (root / declared).is_dir()
    assert SKILL_DIR.parent == root / "skills"


def test_plugin_version_tracks_the_package():
    """without it an install lands under an 'unknown' version directory"""
    assert (
        json.loads(PLUGIN.read_text(encoding="utf-8"))["version"]
        == scinexus.__version__
    )
