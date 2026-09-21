# Use scinexus with a coding agent

!!! abstract ""

    Install the `scinexus` agent skill, so that a coding assistant knows how the framework fits together before it writes any code for you. Works with any agent that reads skills, not one in particular.

## What the skill is

A directory of markdown: a `SKILL.md` carrying a worked loader, generic app and writer pipeline plus the rules that decide whether a first attempt works, and four files beside it that an agent opens only when a task reaches them -- the [llms.txt](../llms.txt) API summary, the file helpers, pipelines and data stores, and the hooks for a package built on `scinexus`.

It tells an assistant the things an API reference cannot: that a writer's `main` always receives `NotCompleted` values, that `mode="w"` creates a data store rather than emptying it, that `from __future__ import annotations` stops `define_app` working.

## Install it from this site

This site publishes the skill at the location the [agent skills standard](https://posit-dev.github.io/great-docs/user-guide/agent-skills.html) specifies, so a tool that follows that standard needs only the address this page is served from:

```bash
npx skills add https://scinexus.readthedocs.io/en/latest/
```

That reads the manifest at [`.well-known/agent-skills/index.json`](../.well-known/agent-skills/index.json) and installs what it lists. That link is relative, so following it from wherever you are reading this page gives the address to use, whatever prefix this site is served under. Where the files then land is the installing tool's business, and each assistant reads a different place:

| agent | directory |
| --- | --- |
| Claude Code | `.claude/skills/scinexus/` |
| Codex | `.codex/skills/scinexus/` |
| Cursor | `.cursor/skills/scinexus/` |
| GitHub Copilot | `.github/skills/scinexus/` |
| Windsurf | `.windsurf/skills/scinexus/` |
| OpenCode | `.opencode/skills/scinexus/` |

## Install it by hand

Nothing above is required. The files are plain markdown at a fixed address, so fetching them yourself works just as well -- substitute the directory your assistant reads:

```bash
mkdir -p .claude/skills/scinexus
base=https://scinexus.readthedocs.io/en/latest/.well-known/agent-skills/scinexus
for f in SKILL.md io.md pipelines.md extending.md llms.txt; do
  curl -sSfo ".claude/skills/scinexus/$f" "$base/$f"
done
```

An agent that can fetch a URL itself needs even less: point it at `https://scinexus.readthedocs.io/en/latest/skill.md` and ask it to follow the file names inside.

## Install it as a Claude Code plugin

Claude Code can also install the skill from the repository, which keeps it up to date with the rest of the plugin machinery:

```
/plugin marketplace add cogent3/scinexus
/plugin install scinexus@scinexus
```

Worth knowing how that finds anything, since neither command names a skill. The first clones `github.com/cogent3/scinexus` and reads `.claude-plugin/marketplace.json` at its root, which lists one plugin named `scinexus` whose `source` is the repository root. The second reads `<plugin>@<marketplace>`, so `scinexus@scinexus` is the plugin named `scinexus` from the marketplace named `scinexus`. Installing it copies that source, and Claude Code then finds skills where the manifest says they are, at `skills/<name>/SKILL.md`.

Because the source is the repository root, that copies the whole repository -- a couple of megabytes of package, tests and documentation for thirty kilobytes of markdown. The trade buys a single copy of the skill: the same files serve the plugin and this site, so the two cannot disagree. Either of the routes above fetches only the skill.

Remove it with:

```
/plugin uninstall scinexus@scinexus
/plugin marketplace remove scinexus
```

## Keeping it current

A skill is a copy, and copies go stale. The published files are rebuilt from the repository whenever these docs are, so re-running the install is what refreshes them. If your assistant starts asserting something this documentation contradicts, that is the first thing to check.
