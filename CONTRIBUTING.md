# Contributing to flink-proto-confluent

Thanks for your interest in contributing. This page covers how to report problems, propose changes and get a pull request merged. The [contributing guide on the docs site](https://brbrown25.github.io/flink-proto-confluent/contributing/) has the detail on working with the documentation and the recorded demos.

## Before you start

- **Found a bug?** Open a [bug report](https://github.com/brbrown25/flink-proto-confluent/issues/new?template=bug_report.yml). The form asks for versions, the table DDL and the stack trace, because most reports cannot be reproduced without them.
- **Want a feature?** Open a [feature request](https://github.com/brbrown25/flink-proto-confluent/issues/new?template=feature_request.yml) and describe the problem before the solution.
- **Found a vulnerability?** Do not open an issue. Follow [SECURITY.md](SECURITY.md).
- **Planning a change?** Every change is linked to an issue. If none exists for what you want to do, open one first, even for a small fix, and wait for a response on anything non-trivial. It avoids work on something that would be declined or redesigned.

Issues labelled [`good first issue`](https://github.com/brbrown25/flink-proto-confluent/labels/good%20first%20issue) are scoped to be approachable without knowing the codebase.

## AI-assisted contributions

AI tools are welcome; unreviewed AI output is not. Disclose AI use, understand and verify what you submit, and be the person who answers in review. Pull requests opened by autonomous agents are closed without review. The full rules are in [AI_POLICY.md](AI_POLICY.md) and they apply to issues as well as code.

## Building and testing

You need a JDK (17 or 21) and, for the integration tests, a running Docker daemon.

```bash
make check      # unit + integration tests, checkstyle, spotbugs, spotless, coverage gate, JAR checks
make test       # unit tests only
./gradlew spotlessApply   # fix formatting
```

- Tests use JUnit 5 and real components: Testcontainers for Kafka and Schema Registry, a Flink MiniCluster for end-to-end runs. Mockito is not used.
- Cover behaviour changes with tests. A bug fix starts with a test that fails without the fix.
- `make check` enforces at least 80% line coverage overall and for the `serialize` and `deserialize` packages.
- `verifyJars` (part of `check`) fails if the library JAR bundles dependencies or the `-all` JAR contains Flink or unrelocated libraries. If you add a dependency, read the [classpath reference](docs/content/reference/classpath.md) first.

## Documentation

If your change adds or alters a format option, a default, an error message or any other user-visible behaviour, update `docs/content/` in the same pull request.

```bash
make docs-serve   # live preview at http://127.0.0.1:8000
make docs         # strict build, the same check CI runs
```

Use these targets rather than a globally installed `mkdocs`. They run MkDocs from a virtualenv built from `docs/requirements.txt`; a bare install lacks the Material theme and fails with `Unrecognised theme name: 'material'`.

The [docs contributing guide](https://brbrown25.github.io/flink-proto-confluent/contributing/) lists what to update for each kind of change and how to re-record a demo cast.

## Commits and pull requests

- Commits follow [Conventional Commits](https://www.conventionalcommits.org/) with a scope, and reference the issue: `feat(serialize): support enum-as-int. #123`. The commit type drives the changelog and the next version number through release-please, so choose it deliberately (`feat` for user-visible additions, `fix` or `bug` for bug fixes, both listed under "Bug Fixes", `docs`, `test`, `chore`, `deps`).
- Use the body to say why the change is being made. The diff already shows what changed.
- Pull requests are rebase-merged, so each commit lands on `main` as written. Keep them few and meaningful.
- Fill in the [pull request template](.github/pull_request_template.md), including the authorship confirmation.
- A pull request needs passing checks and one approving review. CI is described in [docs/ci.md](docs/ci.md).

## License

By contributing, you agree that your contributions are licensed under the [Apache License 2.0](LICENSE).
