<div align="center">

# 🤖 Agent Operations Archive

### A development history from the Codex-assisted creation of this workshop

</div>

> [!NOTE]
> Nothing in this directory is required to build, run, or teach the workshop.
> It preserves one of the development trails used while Codex helped create and
> refine the repository.

---

## What is preserved here

| Path | Historical purpose |
| --- | --- |
| [`AGENTS.md`](AGENTS.md) | Working agreements and project-specific instructions used by Codex during development. It is archived here and is no longer a root-level instruction file. |
| [`MEMORY.md`](MEMORY.md) | Durable decisions and implementation context recorded while the workshop evolved. |
| [`dev-todos/`](dev-todos/) | Completed plans, technical TODOs, lesson designs, and refinement tasks used to develop the Labs incrementally. |

The archive is intentionally not polished into product documentation. It shows
how requirements were broken down, tested, reviewed, refined, and moved through
small sequential changes.

---

## Following the development trail

The repository was developed through a mostly sequential branch and pull
request flow. Curious readers can combine this archive with the
[GitHub pull request history](https://github.com/Gabriel-Philot/workshop-spark-measures/pulls?q=is%3Apr)
to reconstruct the progression from initial platform setup through the Labs,
documentation refinement, and presentation assets.

A practical reading order is:

1. inspect the completed plans and TODOs under `dev-todos/done/`;
2. locate the corresponding pull request and commits;
3. compare the stated acceptance criteria with the resulting Lab or document;
4. use `MEMORY.md` and `AGENTS.md` for the development context that surrounded
   those changes.

> [!TIP]
> This material is primarily for the unusually curious reader who wants to see
> approximately how an AI-assisted workshop repository was developed over many
> small iterations.

---

## Codex workflow tooling

Codex was the coding agent used to assist this development history. One of the
plugins used during the process was
[Superpowers](https://github.com/obra/superpowers), an agentic skills framework
that provides workflows for activities such as brainstorming, planning,
verification, debugging, code review, and branch completion.

The files in this directory record this project's use of those practices. For
the plugin itself, installation guidance, current behavior, and licensing,
consult the official Superpowers repository.

---

## Archive boundary

This directory is historical evidence, not a second source of truth for the
workshop. Current setup instructions, architecture decisions, and classroom
execution remain in the root README, `docs/`, and the individual Lab guides.
