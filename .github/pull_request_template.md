## Summary

<!-- What changed and why. Link the issue: Closes #NNN -->

## Authorship

AI tools are welcome here; unreviewed AI output is not, and neither are pull requests opened by unattended agents. See the [AI usage policy](https://github.com/brbrown25/flink-proto-confluent/blob/main/AI_POLICY.md).

- [ ] This change comes from a person who understands what it does and why, has run it, and can discuss it in review
- AI assistance: <!-- "none", or the tool and what it did, e.g. "Claude Code drafted the converter; I rewrote the null handling and wrote the tests" -->

## Checklist

- [ ] Tests added or updated (unit and/or integration)
- [ ] Commit messages follow Conventional Commits and reference the issue (`feat(scope): summary. #123`)
- [ ] **Docs:** if this adds or changes an option, default, error message or user-visible output, I updated `docs/content/` (see [Contributing](https://brbrown25.github.io/flink-proto-confluent/contributing/))
- [ ] **Casts:** if the change alters output shown in a demo (`docs/content/assets/casts/`), I re-recorded it with `docs/demo/record.sh` — or N/A
- [ ] `make docs` (the strict MkDocs build) passes locally if I touched `docs/` — use the make target, a globally installed `mkdocs` lacks the theme
