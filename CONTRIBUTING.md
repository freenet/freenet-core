# Contributing to Freenet

We welcome contributions to Freenet! Here's what you need to know.

## What Needs an Issue First

Reviewer attention is the scarcest resource on this project, and code is now cheap to generate. To keep the review queue tractable, contributions fall into two buckets:

**Accepted without prior discussion:**

- Bug fixes (that don't change intended behavior).
- Performance improvements that aren't overreaching and don't change behavior.
- Typo fixes, documentation corrections, and obvious one-line changes.

**Requires an approved issue first:**

- **Any feature change or new feature.** Open an issue describing what you want to do and wait for a maintainer to confirm the approach *before* writing code. A maintainer explicitly saying "yes, a PR for this is welcome" is the green light — silence is not approval.
- **Anything that changes behavior, adds scope, or reshapes an API**, even if it started life as a bug fix or a refactor.

**Feature PRs opened without an approved issue will be auto-closed.** This isn't personal — we cannot commit to reviewing speculative feature work, and unsolicited feature PRs are now the dominant source of review load. Reopening is welcome once the issue discussion happens and a maintainer signs off on the approach.

When in doubt about which bucket you're in, file an issue first. It costs you a round-trip; guessing wrong costs you the whole PR.

### Scope discipline

- **One logical change per PR.** Bundling a feature into a "bug fix," or letting a focused change accrete unrelated cleanup, is scope creep and a reason to close the PR pending an issue-level conversation — regardless of whether the extra change is good on its own merits.
- **The submitter is responsible for the PR.** Whatever tools you used to produce it, you must understand the change well enough to defend the design choices, answer reviewer questions, and revise it. "I'm not sure, the AI wrote it" is grounds for closing the PR.
- **Volume is a signal.** A burst of unrelated PRs from a new contributor will be treated as a single batch and likely closed pending an issue-level conversation about what you'd actually like to work on.

## Before You Start

- **Read the codebase conventions.** See [AGENTS.md](AGENTS.md) for project structure, coding standards, and testing requirements.
- **Ask questions.** If something is unclear, ask on the issue or in our [Matrix channel](https://matrix.to/#/#freenet:matrix.org) before writing code.

## Quality Standards

- PR titles must follow [Conventional Commits](https://www.conventionalcommits.org/) format (`feat:`, `fix:`, `docs:`, etc.) — CI enforces this.
- PRs should explain **why**, not just what. See [AGENTS.md](AGENTS.md) for description structure.
- Bug fixes should include a regression test that fails without the fix.
- Run `cargo fmt`, `cargo clippy --all-targets`, and `cargo test` before pushing.
- Keep PRs focused — one logical change per PR.

## AI-Assisted Contributions

We use AI tools extensively in our own development and welcome AI-assisted contributions, but we hold them to the same quality bar as our internal work. We publish the [agent skills](https://github.com/freenet/freenet-agent-skills) our team uses — specifically:

- **[pr-creation](https://github.com/freenet/freenet-agent-skills/tree/main/skills/pr-creation)** — covers the full PR lifecycle: testing, code simplification, and self-review before submission.
- **[pr-review](https://github.com/freenet/freenet-agent-skills/tree/main/skills/pr-review)** — four-perspective code review (code-first, testing, skeptical, big-picture).

If you're using AI to generate PRs, please use these skills or equivalent tooling that understands the Freenet codebase. PRs that appear AI-generated without adequate quality control (missing tests, unrelated changes bundled together, hardcoded paths, stacked commits across PRs, etc.) will be closed.

**Disclose AI assistance.** PRs and review comments produced with AI assistance should say so — a trailing line like `[AI-assisted - Claude]` or `[AI-assisted - Codex]` on the PR description and on substantive comments is sufficient. This isn't a barrier to contribution; it just lets reviewers calibrate.

**AI tooling requirements:** Freenet's codebase involves async networking, cryptographic protocols, and subtle concurrency — areas where AI models vary dramatically in capability. Our team uses frontier models with project-specific context and still catches significant issues in self-review.

We require the use of a frontier-class model (as of early 2026: Claude Opus 4, GPT-5, Gemini 3, etc.) with an agentic coding tool. Smaller and local models consistently produce plausible-looking code that introduces subtle bugs in this codebase. PRs that show signs of inadequate AI tooling will be closed.

## Getting Help

- [Matrix chat](https://matrix.to/#/#freenet:matrix.org) for questions and discussion
- [Issue tracker](https://github.com/freenet/freenet-core/issues) for bugs and feature requests
- [API docs](https://docs.rs/freenet) for code reference
- [Freenet manual](https://freenet.org/resources/manual/) for architecture overview
