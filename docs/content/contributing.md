# Contributing

Issues and pull requests are welcome at [github.com/brbrown25/flink-proto-confluent](https://github.com/brbrown25/flink-proto-confluent). Open an issue first for anything non-trivial.

```bash
make check      # tests + checkstyle
make coverage   # unit + integration tests + JaCoCo
```

Tests use JUnit 5 and real components (Testcontainers for Kafka and Schema Registry); Mockito is not used. Commits follow [Conventional Commits](https://www.conventionalcommits.org/), which drives release-please.

## Working on these docs

```bash
pip install -r docs/requirements.txt
cd docs && mkdocs serve        # live preview at http://127.0.0.1:8000
cd docs && mkdocs build        # strict build, same as CI
```

Terminal recordings live in `docs/content/assets/casts/`; see [`docs/demo/README.md`](https://github.com/brbrown25/flink-proto-confluent/blob/main/docs/demo/README.md) for how to re-record them.

Maintainers: the release process is documented in [RELEASING.md](https://github.com/brbrown25/flink-proto-confluent/blob/main/docs/RELEASING.md) and CI ownership in [ci.md](https://github.com/brbrown25/flink-proto-confluent/blob/main/docs/ci.md).
