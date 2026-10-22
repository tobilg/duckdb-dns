# AGENTS.md

Instructions for AI coding agents working in this repository. See [CLAUDE.md](CLAUDE.md) for build, test and architecture details.

## Pull requests and review

This repository has two maintainers: @tobilg and @onnimonni.

- Never push to `main` and never merge pull requests yourself, even with write access. Merging is left to the maintainers.
- Open a pull request from a branch and request a review from the *other* maintainer:
  - change made by or for @onnimonni → `gh pr edit <number> --add-reviewer tobilg`
  - change made by or for @tobilg → `gh pr edit <number> --add-reviewer onnimonni`
- A pull request needs at least one approving review from a maintainer other than its author before it is merged.
- Mention in the PR description what was tested (e.g. `make test_debug` and the DuckDB version).
