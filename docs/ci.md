# CI workflow ownership

Each workflow owns a distinct slice of the pipeline so that no Gradle task runs more than once for the same commit on the same JDK and Flink line. Testing on multiple JDKs and Flink lines is intentional and is *not* considered duplication — the rule is one run of each task **per JDK and Flink line** per event.

## Workflows

| Workflow | Trigger | Owns |
| --- | --- | --- |
| `pr-checks.yml` | `pull_request` | All pull-request signal: lint, build/JAR check per Flink line, dependency review, the JDK 17 + 21 × Flink 1.20 + 2.x unit-test matrix, one integration-test run per Flink line on the primary JDK (coverage from the Flink 1.20 leg only), the public API compatibility check per Flink line, and the `ci-ok` aggregate. |
| `ci.yml` | `push` to `main`/`master`, `workflow_dispatch` | The canonical post-merge build: one job per Flink line that builds and runs unit + integration tests (the Flink 1.20 leg also verifies coverage and uploads to Codecov), plus a parallel lint job. |
| `publish-snapshot.yml` | `workflow_run` after a successful **CI** run on `main`/`master`, `workflow_dispatch` | Publishing SHA-qualified snapshots (`X.Y.Z-<shortsha>-SNAPSHOT`). It does not re-test: it is gated on the CI run for the same SHA. Skips release commits. |
| `release-please.yml` | `push` to `main`, `workflow_dispatch` | Maintaining the `chore(release): X.Y.Z` pull request, and on its merge creating the tag and the GitHub Release. |
| `docs.yml` | `pull_request` / `push` to `main`/`master` touching `docs/**`, `workflow_dispatch` | Building the MkDocs site with `--strict` (PRs and pushes) and deploying it to GitHub Pages (pushes only). |
| `release.yml` | tag `v*` | Release verification, publishing to Maven Central, and attaching the library and self-contained `-all` JARs to the GitHub Release. |
| `benchmarks.yml` | `workflow_dispatch` only | Running the JMH benchmarks (`./gradlew jmh`) on demand and uploading `build/results/jmh/` as an artifact. Not a required check; the benchmark source set is only compiled and linted by `build`. See [Benchmarks](content/reference/benchmarks.md). |
| `docs-demo.yml` | `pull_request` touching `src/main/**` or `docs/**`, `workflow_dispatch` | Running the docs demo (`docs/demo`) end to end against the format JAR built from the pull request, so the SQL snippets and casts cannot go stale silently. Not a required check and not part of `ci-ok`. See [Docs demo check](#docs-demo-check). |

`ci.yml` deliberately does **not** trigger on `pull_request`; that is what makes the PR path free of same-JDK repeats.

`release.yml` triggers only on the tag push, never on `release: created`. Both triggers would fire concurrent runs for one version whose publish jobs contend for the same Sonatype staging repository; a `concurrency` group keyed on the tag is the second guard against that. See [RELEASING.md](RELEASING.md) for the full release flow.

## Where each task runs

| Task | On a pull request | On push to `main` |
| --- | --- | --- |
| `build -x test -x integrationTest` | `pr-checks.yml` → `build-check` (per Flink line) | `ci.yml` → `build` (per Flink line) |
| `test` (unit) | `pr-checks.yml` → `test-matrix` (JDK 17 **and** 21, per Flink line) | `ci.yml` → `build` (primary JDK, per Flink line) |
| `integrationTest` | `pr-checks.yml` → `integration-tests` (primary JDK, per Flink line) | `ci.yml` → `build` (per Flink line) |
| `jacocoTestReport`, `jacocoTestCoverageVerification` | `pr-checks.yml` → `integration-tests` (Flink 1.20 leg) | `ci.yml` → `build` (Flink 1.20 leg) |
| Checkstyle / SpotBugs / Spotless | `pr-checks.yml` → `lint` (per Flink line) | `ci.yml` → `checkstyle` (per Flink line) |
| `japicmp` (public API compatibility) | `pr-checks.yml` → `api-compat` (per Flink line) | not run: it guards the change before it lands |
| Docs demo (`docs/demo/bin/verify.sh`) | `docs-demo.yml` → `demo` (Flink 1.20; touching `src/main/**` or `docs/**` only) | not run |

## Flink lines and the `ci-ok` check

The lint, build, unit-test and integration-test jobs run once per supported Flink line (`flink: ['1.20.5', '2.2.1']`, passed as `-PflinkVersion`), with `fail-fast: false` so one line's failure does not hide the other's result. Check names carry the versions, e.g. `lint (flink 2.2.1)`, `build (flink 2.2.1)`, `unit tests (jdk 17, flink 1.20.5)`, `integration tests (flink 2.2.1)`.

Each leg also proves it ran on the line it names. The jobs export the matrix value as `EXPECTED_FLINK_VERSION`, and `FlinkLineGuardTest` (unit) and `FlinkLineGuardIntegrationTest` (integration) fail unless it equals the `flinkVersion` Gradle resolved, `flink-runtime` reports that version, and `flink-core`, `flink-table-common`, the DataStream/Table bridge and the Kafka connector are loaded from that line's JARs. A leg that silently fell back to the default Flink version therefore fails instead of passing against the wrong line. Locally, without the variable, the guards still check the classpath against `-PflinkVersion`.

Because those names change on every version bump, branch protection requires only `ci-ok`. It needs every gated job, runs with `if: always()`, and fails if any of them failed or was cancelled; when they were all skipped for a docs-only change it passes, so the required check is never left pending. Bumping a Flink or JDK version therefore needs no ruleset change.

## Two build wiring details that matter

`tasks.test` is wired with `finalizedBy(jacocoTestReport)`, and `jacocoTestReport` depends on both `test` and `integrationTest`. A plain `./gradlew test` therefore also runs the integration suite. Any job that wants unit tests alone must run `./gradlew test -x integrationTest -x jacocoTestReport` — that is why the matrix legs carry those exclusions.

Because of the same wiring, the PR coverage leg cannot simply run `jacocoTestReport` without re-running `test`. Instead the JDK 21 / Flink 1.20 unit-test leg (marked `coverage: true` in the matrix) uploads its `build/jacoco/test.exec`, the Flink 1.20 `integration-tests` leg downloads it, and then runs `./gradlew integrationTest jacocoTestReport jacocoTestCoverageVerification -x test`. `jacocoTestReport` collects every `build/jacoco/*.exec`, so the report and the 80% gate still see unit **and** integration coverage from a single execution of each suite.

## Gradle version

CI drives every build through the committed wrapper (`./gradlew`), so `gradle/wrapper/gradle-wrapper.properties` is the single source of truth for the Gradle version across CI, local development and releases. `.github/actions/setup-build` pins no `gradle-version`; `gradle/actions/setup-gradle` picks up the wrapper and caches it.

## Caching

Two caches are configured explicitly, and both log a HIT/MISS annotation so effectiveness is visible in the run logs rather than assumed.

### Gradle dependency cache

`gradle/actions/setup-gradle` only writes the cache from the default branch by default, which left cache behaviour on PR branches unverified. `.github/actions/setup-build` now takes an explicit `cache-read-only` input, defaulting to `true`:

| Job | Mode | Why |
| --- | --- | --- |
| `ci.yml` → `build` (push to `main`) | **read-write** (`cache-read-only: false`) | The single writer. It resolves the full dependency set (build + unit + integration + coverage), so it warms the entry every other job reuses. |
| `ci.yml` → `checkstyle` | read-only | Same dependency set as `build`; writing again would only duplicate entries. |
| every `pr-checks.yml` job | read-only | PR branches must not be able to poison the shared cache. |

`setup-build` then prints a `Gradle cache` notice naming the number of restored dependency groups, the cache size on disk and the mode, plus which wrapper distributions were restored. A MISS on a PR branch means the warming run on `main` needs to be investigated — it is not silent any more.

Gradle's own cache key includes the workflow, job and matrix values, so the JDK and Flink matrix legs maintain separate entries instead of evicting one another. In `ci.yml` both Flink legs of `build` write (each to its own entry), so the cache holds the dependency set of both lines. PR jobs have different job ids, so they fall back to the most recent entry for the runner OS; that entry may belong to the other Flink line, in which case the leg downloads only the Flink artifacts it is missing.

When the restored entry lacks something many legs need (a dependency bump, a new Flink version), every leg downloads it from Maven Central at once and Central may answer `429 Too Many Requests`. `gradle.properties` therefore raises Gradle's dependency-download retries (`org.gradle.internal.repository.max.tentatives=6`, `org.gradle.internal.repository.initial.backoff=2000`): a transient failure (429, 408, 5xx, dropped connection) is retried after 2, 4, 8, 16 and 32 seconds before the build fails. The similarly named `org.gradle.internal.network.retry.*` properties only cover publishing, not resolution (#202).

### Testcontainers Docker images

The suites pull `confluentinc/cp-kafka`, `confluentinc/cp-schema-registry` and `clickhouse/clickhouse-server` on every run. These images are deliberately **not** cached: measurement showed an Actions cache restore plus `docker load` is slower than pulling from Docker Hub, and the tarballs would consume roughly 3.7 GB of the 10 GB repository cache budget. See #76 for the numbers.

## Concurrency and path filters

`ci.yml` and `pr-checks.yml` each declare a `concurrency` group with `cancel-in-progress: true` — keyed on the ref for `ci.yml` and on the pull-request number for `pr-checks.yml` — so pushing several commits in quick succession leaves only the newest pipeline running.

Documentation-only changes skip the heavy jobs. `ci.yml` uses `paths-ignore` on its `push` trigger. `pr-checks.yml` cannot use `paths-ignore` without leaving required checks permanently pending, so it instead runs a cheap `changes` job that diffs the PR against its base and exports `code=true|false`; `lint`, `build-check`, `api-compat`, `test-matrix` and `integration-tests` all gate on it. A docs-only PR therefore reports those checks as skipped rather than absent. Both filters treat `docs/`, any `*.md`, `LICENSE`, `NOTICE` and `.gitignore` as documentation; anything else — including `.github/` itself — counts as code.

## API compatibility check

A minor or patch release must not break code written against the previous one. release-please derives the next version from the commit types, so nothing but this check stops a `feat(...)` that removes a public method from shipping as a minor release (#220).

`pr-checks.yml` → `api-compat` runs `./gradlew japicmp` once per Flink line. The task compares the thin JAR built for that line with the last release of the same line on Maven Central: `com.bbrownsound:<artifactId>:<releaseVersion>`, where the artifactId is `flink1.artifactId` or `flink2.artifactId` and `releaseVersion` is the line release-please maintains in `gradle.properties`. The self-contained `-all` JAR is never compared; its relocated packages are not API. It checks binary **and** source compatibility of the public API (below) and fails on any incompatible change: a removed or renamed class, method, field or `ConfigOption` constant, a changed signature, a method that now throws a checked exception, a new abstract method, and so on. Additions never fail it.

### What counts as public API

The public API is listed in one place, `publicApiPackages` and `publicApiClasses` in `build.gradle.kts`. It is what users reference from Java, from SQL option strings or from the documentation:

| Public | Why |
| --- | --- |
| `RegistryProtoFormatFactory`, `ProtoConfluentFormatOptions` | The format's entry point and its `ConfigOption`s. |
| `config.*` (`ProtoConfluentFormatConfig`, `OnDeserializeError`, `DeadLetterDelivery`) | The format configuration and the values its options map to. |
| `catalog.*` (`SchemaRegistryCatalog`, its factory, options and config) | The Schema Registry catalog, created from SQL or Java. |
| `subject.*` (`SubjectNameStrategies`, `FixedSubjectNameStrategy`) | Named by class in subject-name-strategy options. |
| `schema.*` (`SchemaRegistrySchemaResolver`, `ResolvedProtoSchema`, `SchemaRegistryClients`, `SchemaResolutionException`) | Documented Java API for resolving a schema outside a job ([Generate DDL](content/how-to/generate-ddl.md)). |
| `util.ProtoToTableDDL`, `util.ProtoToLogicalType`, `util.ProtoToSqlType`, `util.RecursionStrategy` | Documented Java API for deriving DDL and Flink types ([Type mapping](content/reference/type-mapping.md)). |
| `deserialize.UnexpectedMessageTypeException` | Reaches users as the cause of a deserialize error. |

Everything else is internal and may change in any release: the decoding and encoding formats the factory hands to Flink (`ProtoDecodingFormat`, `ProtoEncodingFormat`), the (de)serialization schemas and converters, `metrics`, the `ddl` command line tool (`cli`; its command line is its interface, not its classes), and the rest of `util`. Only public and protected members of public classes are compared. To make something public API, add it to the lists in the same pull request that documents it.

### Declaring an intentional break

The check passes with incompatible changes only when the pull request declares a breaking change the way Conventional Commits does, which is also what makes release-please bump the **major** version:

- `!` right before the colon, in the pull request title or a commit subject: `feat(api)!: remove printTableSchemaFromProto. #123`;
- or a `BREAKING CHANGE: <what breaks and what to do instead>` footer in a commit message.

`.github/scripts/breaking-change-declared.sh` makes that decision (its tests, `breaking-change-declared.test.sh`, run first in the same job) and the job passes `-Pjapicmp.acceptBreaking=true` to Gradle when a marker is present. The job reads the title from the GitHub API, not from the event that started the run, so after adding `!` to the title, re-run the failed `api compatibility` jobs (and with them `ci-ok`); a new push works too. release-please reads the commits that land on `main`, so the marker must be in what lands: a rebase merge lands the commits as written, while a squash merge takes its subject from the title when the pull request has several commits (and from the commit when it has one). When only the title carries the marker the job prints a warning saying so.

A declared break still has to be recorded. Add each incompatible element to `acceptedBreaks` in `build.gradle.kts`, under the baseline version it breaks, with a comment naming the issue or pull request; the job summary and `build/reports/japicmp/japicmp.md` list the exact entries to paste. Entries for a baseline stop applying once release-please moves `releaseVersion` past it, so they never hide a later change to the same element.

### Reports and skipped runs

The task writes `build/reports/japicmp/japicmp.md` (only the incompatible changes, or a line saying there are none) and `japicmp.html` / `japicmp.xml` (every changed element of the public API, each marked compatible or not). The job appends the Markdown report to its job summary and uploads the directory as the `japicmp-flink<version>` artifact.

When the baseline does not exist on Maven Central, the comparison is skipped with a log line and a report saying why, rather than failing. That is the case for the Flink 2 line (`flink-proto-confluent-flink2`) until its first release, and for both lines in the short window between merging a release pull request (which moves `releaseVersion`) and the release reaching Central. Any other resolution failure, such as no network, fails the task, so CI never passes a comparison it did not make.

`japicmp` is not part of `check`, because it needs Maven Central. Run it locally with `./gradlew japicmp -PflinkVersion=1.20.5` (and `2.2.1`); add `-Pjapicmp.acceptBreaking=true` to see the report of a declared break without the failure.

## Docs demo check

The recorded casts and the SQL in `docs/demo` show option names and output that the code can change under them (#130). `docs-demo.yml` runs that demo on every pull request touching `src/main/**` or `docs/**` (and on demand), so a renamed or removed option, or changed output, fails a check instead of leaving a stale cast.

The job builds the shadow (`-all`) JAR from the pull request, then runs `docs/demo/bin/verify.sh`, which also works locally (Docker and a JDK, nothing else):

```bash
docs/demo/bin/verify.sh                 # build the JAR, start a clean stack, run and check every step, tear down
docs/demo/bin/verify.sh --no-build dead-letter   # reuse ./lib, run one step
docs/demo/bin/verify.sh --keep          # leave the stack up to poke at it (docker compose -p proto-confluent-demo-verify down -v)
FLINK_VERSION=2.2 docs/demo/bin/verify.sh        # the same on the Flink 2.2 stack
```

It starts the `docs/demo` stack (Kafka, Schema Registry, a Flink 1.20 session cluster with the JAR mounted into `lib/`) under its own compose project, runs every cast script with `CAST_FAST=1` and `sql/quickstart.sql` on its own, in the order `verify.sh` lists (`schema-evolution` gets a fresh stack, because it creates the `orders` topic itself). A step fails when it:

- exits non-zero or runs past `STEP_TIMEOUT` (300 seconds);
- prints a Flink SQL client error (`[ERROR]`, `Could not execute SQL statement`), which is what an unknown or renamed option produces;
- does not print, in order, every line of `docs/demo/expected/<step>.txt`. Those files hold the facts the docs rely on: the aggregate rows, the rows that survive `on-deserialize-error = skip`, the dead-letter headers (`error.class`, `error.message`, `source.topic`), the schema `auto-register-schemas` registers, the registry's compatibility verdicts. Each line is a plain substring of an output line; `#` starts a comment.

It also fails when a committed `sql/*.sql` file is not run by any cast or step, or a cast has no step, so new snippets cannot go unchecked. Each step's output is kept in `docs/demo/.verify/` and uploaded as the `docs-demo-output-flink<version>` artifact. When a change to the demo output is intended, update the matching `expected/` file (and re-record the cast, see `docs/demo/README.md`).

Cost: the whole job takes about NN minutes on `ubuntu-latest` (about 3 minutes locally with a warm Gradle cache), most of it image pulls, the shadow JAR build and Flink jobs. The job has a 30-minute timeout and the verify step 20 minutes; `verify.sh` and a final `if: always()` step both run `docker compose down -v`. The Kafka SQL connector JAR is cached with `actions/cache`; the images are pulled, not cached, for the reason given for the Testcontainers images above. `workflow_dispatch` takes a `flink-version` input (`1.20` or `2.2`).

The check is **not** required and is not in `ci-ok`'s `needs`. It is path-filtered at the trigger, so on a pull request that touches neither path it is absent, not skipped, and requiring it would leave such pull requests pending forever, the same trap #117 describes for `codecov/patch`. Making it required needs a decision on #117 first (for example moving it into `pr-checks.yml` behind the `changes` job with a docs-or-code filter, so it reports a skip).

## Coverage gates

Three things report on coverage, and only two of them can fail a build. Knowing which is which matters, because the most visible one is the one that cannot.

| Mechanism | Can it fail? | What it covers |
| --- | --- | --- |
| Gradle `jacocoTestCoverageVerification` | **Yes** — fails the `integration tests (flink 1.20.x)` leg | 80% line coverage project-wide, plus per-package rules on `…confluent.serialize` and `…confluent.deserialize` |
| Codecov `project` / `patch` statuses | **Yes**, once required (see below) | Project-wide, and coverage of the lines the PR changed |
| `madrapps/jacoco-report` PR comment | **No** | Nothing — it is a report only |

The `madrapps/jacoco-report` step's `min-coverage-overall` and `min-coverage-changed-files` inputs only select the pass/fail emoji in the comment it posts; the action has no failure path for a threshold breach, and its `continue-on-error` input governs runtime exceptions rather than coverage. Treat that comment as information, never as a gate.

`codecov.yml` defines the `project` and `patch` statuses with `informational: false`, which is what makes them capable of failing — an informational status always reports success. The `patch` status is the one that matters most, because it is the only gate on *changed-line* coverage: Gradle's project-wide rule will happily stay above 80% while a PR adds uncovered code.

Two of the steps are outside the repository and must be done once, in repository settings:

1. **Install the Codecov GitHub App.** Without it Codecov cannot create statuses at all — uploads still succeed and it will still comment (as `codecov-commenter` rather than `codecov[bot]`), but no check appears, so nothing can block. This is the state the repository was in before this configuration existed.
2. **List `codecov/project` and `codecov/patch` as required status checks** in the ruleset that protects the default branch. A red status that is not required is advisory only.

Both `codecov/codecov-action` invocations use `fail_ci_if_error: true`, so an upload that fails fails the job rather than passing silently and leaving Codecov to report on stale data.
