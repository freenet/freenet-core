# Contributing to Freenet

We welcome contributions to Freenet! Here's what you need to know.

## Before You Start

- **Claim the issue first.** Comment on the issue you want to work on and wait for a maintainer to confirm before starting work. This prevents duplicate effort and ensures the approach is aligned with project direction.
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

**AI tooling requirements:** Freenet's codebase involves async networking, cryptographic protocols, and subtle concurrency — areas where AI models vary dramatically in capability. Our team uses frontier models with project-specific context and still catches significant issues in self-review.

We require the use of a frontier-class model (as of early 2026: Claude Opus 4, GPT-5, Gemini 3, etc.) with an agentic coding tool. Smaller and local models consistently produce plausible-looking code that introduces subtle bugs in this codebase. PRs that show signs of inadequate AI tooling will be closed.

## Getting Help

- [Matrix chat](https://matrix.to/#/#freenet:matrix.org) for questions and discussion
- [Issue tracker](https://github.com/freenet/freenet-core/issues) for bugs and feature requests
- [API docs](https://docs.rs/freenet) for code reference
- [Freenet manual](https://freenet.org/resources/manual/) for architecture overview
