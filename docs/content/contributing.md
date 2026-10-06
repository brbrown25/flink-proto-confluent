# Contributing

Issues and pull requests are welcome at [github.com/brbrown25/flink-proto-confluent](https://github.com/brbrown25/flink-proto-confluent). Open an issue first for anything non-trivial.

```bash
make check      # tests + checkstyle
make coverage   # unit + integration tests + JaCoCo
```

Tests use JUnit 5 and real components (Testcontainers for Kafka and Schema Registry); Mockito is not used. Commits follow [Conventional Commits](https://www.conventionalcommits.org/), which drives release-please.

## Working on these docs

The site is [MkDocs Material](https://squidfunk.github.io/mkdocs-material/). Sources are in `docs/content/`, config in `docs/mkdocs.yml`, and the nav is the `nav:` list in that file — a new page must be added there.

### Preview locally

```bash
python3 -m venv .venv && .venv/bin/pip install -r docs/requirements.txt
cd docs
../.venv/bin/mkdocs serve      # http://127.0.0.1:8000, live reload
../.venv/bin/mkdocs build --strict   # exactly what CI runs; fails on broken links/nav
```

### When to update what

| You changed | Update |
| --- | --- |
| A format option, default or validation rule | `reference/configuration.md` and any how-to that shows it |
| User-visible behaviour or an error message | The relevant how-to; re-record a cast if it appears in one |
| The dependency version | Nothing — release-please bumps the version markers in `getting-started.md` and the README on release |
| Output shown in a demo | Re-record that cast (below) |

Keep SQL snippets in the docs identical to the SQL in `docs/demo/sql/` where both exist.

### Terminal demos (asciinema)

Casts in `docs/content/assets/casts/` are recorded against a real Kafka + Schema Registry + Flink stack defined in `docs/demo/`. Requires Docker, `asciinema` 3.x and a JDK.

```bash
cd docs/demo
./record.sh                    # rebuilds the JAR, resets the stack, re-records every cast
asciinema play ../content/assets/casts/quickstart.cast   # preview one in the terminal
```

To add a cast: write `docs/demo/casts/<name>.sh` (copy an existing one; `say` prints a comment, `run` types and executes a command), add any SQL to `docs/demo/sql/`, add `<name>` to the loop in `record.sh`, then embed it in `demos.md`:

```html
<div class="cast" data-src="../assets/casts/<name>.cast"></div>
```

Keep casts under ~45 seconds. See [`docs/demo/README.md`](https://github.com/brbrown25/flink-proto-confluent/blob/main/docs/demo/README.md) for the stack itself.

### Publishing

Pushes to `main` that touch `docs/**` build and deploy the site to GitHub Pages via `.github/workflows/docs.yml`. Pull requests run the same strict build without deploying.

Maintainers: the release process is documented in [RELEASING.md](https://github.com/brbrown25/flink-proto-confluent/blob/main/docs/RELEASING.md) and CI ownership in [ci.md](https://github.com/brbrown25/flink-proto-confluent/blob/main/docs/ci.md).
