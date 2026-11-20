# Contributing

Issues and pull requests are welcome at [github.com/brbrown25/flink-proto-confluent](https://github.com/brbrown25/flink-proto-confluent). Open an issue first for anything non-trivial. [CONTRIBUTING.md](https://github.com/brbrown25/flink-proto-confluent/blob/main/CONTRIBUTING.md) in the repository covers the workflow, commit format and pull request expectations; this page covers the build and the docs.

AI-assisted contributions are welcome as long as you disclose the assistance, understand and have verified what you submit, and answer review yourself. Read the [AI usage policy](https://github.com/brbrown25/flink-proto-confluent/blob/main/AI_POLICY.md) before opening an issue or pull request. Report vulnerabilities privately as described in the [security policy](https://github.com/brbrown25/flink-proto-confluent/blob/main/SECURITY.md).

```bash
make check      # tests + checkstyle
make coverage   # unit + integration tests + JaCoCo
```

Tests use JUnit 5 and real components (Testcontainers for Kafka and Schema Registry); Mockito is not used. Commits follow [Conventional Commits](https://www.conventionalcommits.org/), which drives release-please.

## Testing against each Flink line

One source tree builds for both supported Flink lines (see [Supported Flink versions](getting-started.md#supported-flink-versions)). `-PflinkVersion` picks the Flink version the build compiles and tests against; without it the build uses `flink1.version` from `gradle.properties` (1.20.5), which is what the `make` targets run. `gradle.properties` is the one place each line's version is set (`flink1.version`, `flink2.version`): CI, snapshots, releases and the docs demo all read it from there. CI runs every suite on Flink 1.20.5 and 2.2.1, so run the same commands for both lines before opening a pull request that touches `src/` or the build. The integration tests need a running Docker daemon.

| What | Flink 1.20 | Flink 2.x |
| --- | --- | --- |
| Unit tests only | `./gradlew test -x integrationTest -PflinkVersion=1.20.5` | `./gradlew test -x integrationTest -PflinkVersion=2.2.1` |
| Integration tests | `./gradlew integrationTest -PflinkVersion=1.20.5` | `./gradlew integrationTest -PflinkVersion=2.2.1` |
| Unit + integration tests and the JaCoCo report | `./gradlew test -PflinkVersion=1.20.5` | `./gradlew test -PflinkVersion=2.2.1` |
| Full gate (unit + integration tests and every check) | `./gradlew check -PflinkVersion=1.20.5` | `./gradlew check -PflinkVersion=2.2.1` |
| JMH benchmarks (not part of `check`; see [Benchmarks](reference/benchmarks.md)) | `./gradlew jmh -PflinkVersion=1.20.5` | `./gradlew jmh -PflinkVersion=2.2.1` |

`test` is finalized by the JaCoCo report, which needs the integration tests too, so a plain `./gradlew test` (and `make test`) also runs the integration suite; `-x integrationTest` skips it. Run a single test class with `--tests`, for example `./gradlew test -x integrationTest -PflinkVersion=2.2.1 --tests '*RegistryProtoFormatFactoryTest'`. Code under `src/` must compile and pass on both lines; the build puts only one line's Flink artifacts on the classpath, so a test that passes on one line says nothing about the other.

## Working on these docs

The site is [MkDocs Material](https://squidfunk.github.io/mkdocs-material/). Sources are in `docs/content/`, config in `docs/mkdocs.yml`, and the nav is the `nav:` list in that file — a new page must be added there.

### Preview locally

```bash
make docs-serve   # http://127.0.0.1:8000, live reload
make docs         # strict build, exactly what CI runs; fails on broken links/nav
```

Both targets create a virtualenv in `.venv` from `docs/requirements.txt` on first use and run the `mkdocs` inside it. Use them rather than a `mkdocs` installed globally (Homebrew, `pip install mkdocs`): the site needs the `mkdocs-material` theme, which a bare install does not have.

!!! failure "Unrecognised theme name: 'material'"
    This error means the wrong `mkdocs` ran, not that the docs are broken. Run `make docs`, or call the virtualenv's binary directly.

The same thing without `make`:

```bash
python3 -m venv .venv && .venv/bin/pip install -r docs/requirements.txt
cd docs
../.venv/bin/mkdocs serve
../.venv/bin/mkdocs build --strict
```

### When to update what

| You changed | Update |
| --- | --- |
| A format option, default or validation rule | `reference/configuration.md` and any how-to that shows it |
| User-visible behaviour or an error message | The relevant how-to; re-record a cast if it appears in one |
| The dependency version | Nothing — release-please bumps the version markers in `getting-started.md` and the README on release |
| Output shown in a demo | Re-record that cast (below) |

Keep SQL snippets in the docs identical to the SQL in `docs/demo/sql/` where both exist.

Where a page shows the same SQL as a demo script, include it rather than copy it: the site enables [`pymdownx.snippets`](https://facelessuser.github.io/pymdown-extensions/extensions/snippets/) with paths relative to the repository root, so a code block whose only line is `--8<-- "docs/demo/sql/quickstart.sql:create-table"` renders the `create-table` section of that script, marked in it by a `-- ;--8<-- [start:create-table]` comment line and a matching `[end:create-table]` one (see [`docs/demo/README.md`](https://github.com/brbrown25/flink-proto-confluent/blob/main/docs/demo/README.md)). A missing file or section fails `make docs`. SQL that intentionally differs from the demo, such as a shorter example, stays written in the page.

The option tables in `reference/configuration.md` and `how-to/catalog.md` are checked by `OptionsDocumentationTest`, part of `./gradlew test`: it fails when an option has no row, a row names a key that is not an option, a Default cell differs from the option's default, or the "Fallback key" a row names differs from the option's fallback keys. An option without a default must not show a bare value such as `` `topic` `` as its Default; write `*(none)*` and describe the effective behaviour.

### Terminal demos (asciinema)

Casts in `docs/content/assets/casts/` are recorded against a real Kafka + Schema Registry + Flink stack defined in `docs/demo/`. The stack runs Flink 1.20 (`flink:1.20-java17`) by default, and the committed casts are recorded on it; `FLINK_VERSION=2.2 ./record.sh ...` records against Flink 2.2 (`flink:2.2-java17`) instead, which is useful for checking a cast still behaves the same on the other line. Requires Docker, `asciinema` 3.x and a JDK.

```bash
cd docs/demo
./record.sh                    # rebuilds the JAR, resets the stack, re-records every cast
asciinema play ../content/assets/casts/quickstart.cast   # preview one in the terminal
```

To add a cast: write `docs/demo/casts/<name>.sh` (copy an existing one; `say` prints a comment, `run` types and executes a command), add any SQL to `docs/demo/sql/`, add `<name>` to the `ALL` list in `record.sh`, then embed it in `demos.md`:

```html
<div class="cast" data-src="../assets/casts/<name>.cast"></div>
```

Record just some casts with `./record.sh name1 name2`, which reuses the demo stack if it is already healthy (about a minute per cast instead of a full rebuild). Casts reuse state from earlier runs, such as topics and schema ids, so use `./record.sh --reset name1` or a full `./record.sh` before committing a recording. `CASTS="name1 name2" ./record.sh` still works and always resets. You can also dry-run a script without the typing delays with `CAST_FAST=1 ./casts/<name>.sh` (against a running stack). Keep casts under ~45 seconds. See [`docs/demo/README.md`](https://github.com/brbrown25/flink-proto-confluent/blob/main/docs/demo/README.md) for the stack itself.

### Publishing

Pushes to `main` that touch `docs/**` build and deploy the site to GitHub Pages via `.github/workflows/docs.yml`. Pull requests run the same strict build without deploying.

Maintainers: the release process is documented in [RELEASING.md](https://github.com/brbrown25/flink-proto-confluent/blob/main/docs/RELEASING.md) and CI ownership in [ci.md](https://github.com/brbrown25/flink-proto-confluent/blob/main/docs/ci.md).
