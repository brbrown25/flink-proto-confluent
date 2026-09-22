# CI workflow ownership

Each workflow owns a distinct slice of the pipeline so that no Gradle task runs more than once for the same commit on the same JDK. Testing on multiple JDKs is intentional and is *not* considered duplication — the rule is one run of each task **per JDK** per event.

## Workflows

| Workflow | Trigger | Owns |
| --- | --- | --- |
| `pr-checks.yml` | `pull_request` | All pull-request signal: lint, build/JAR check, dependency review, the JDK 17 + 21 unit-test matrix, and one integration-test + coverage run on the primary JDK. |
| `ci.yml` | `push` to `main`/`master`, `workflow_dispatch` | The canonical post-merge build: one job that builds, runs unit + integration tests, verifies coverage and uploads to Codecov, plus a parallel lint job. |
| `publish-snapshot.yml` | `workflow_run` after a successful **CI** run on `main`/`master`, `workflow_dispatch` | Publishing SHA-qualified snapshots (`X.Y.Z-<shortsha>-SNAPSHOT`). It does not re-test: it is gated on the CI run for the same SHA. Skips release commits. |
| `release-please.yml` | `push` to `main`, `workflow_dispatch` | Maintaining the `chore(release): X.Y.Z` pull request, and on its merge creating the tag and the GitHub Release. |
| `release.yml` | tag `v*` | Release verification, publishing to Maven Central, and attaching the shadow JAR to the GitHub Release. |

`ci.yml` deliberately does **not** trigger on `pull_request`; that is what makes the PR path free of same-JDK repeats.

`release.yml` triggers only on the tag push, never on `release: created`. Both triggers would fire concurrent runs for one version whose publish jobs contend for the same Sonatype staging repository; a `concurrency` group keyed on the tag is the second guard against that. See [RELEASING.md](RELEASING.md) for the full release flow.

## Where each task runs

| Task | On a pull request | On push to `main` |
| --- | --- | --- |
| `build -x test -x integrationTest` | `pr-checks.yml` → `build-check` | `ci.yml` → `build` |
| `test` (unit) | `pr-checks.yml` → `test-matrix` (JDK 17 **and** 21) | `ci.yml` → `build` (primary JDK) |
| `integrationTest` | `pr-checks.yml` → `coverage` (primary JDK, once) | `ci.yml` → `build` |
| `jacocoTestReport`, `jacocoTestCoverageVerification` | `pr-checks.yml` → `coverage` | `ci.yml` → `build` |
| Checkstyle / SpotBugs / Spotless | `pr-checks.yml` → `lint` | `ci.yml` → `checkstyle` |

## Two build wiring details that matter

`tasks.test` is wired with `finalizedBy(jacocoTestReport)`, and `jacocoTestReport` depends on both `test` and `integrationTest`. A plain `./gradlew test` therefore also runs the integration suite. Any job that wants unit tests alone must run `./gradlew test -x integrationTest -x jacocoTestReport` — that is why the matrix legs carry those exclusions.

Because of the same wiring, the PR `coverage` job cannot simply run `jacocoTestReport` without re-running `test`. Instead the JDK 21 matrix leg uploads its `build/jacoco/test.exec`, the `coverage` job downloads it, and then runs `./gradlew integrationTest jacocoTestReport jacocoTestCoverageVerification -x test`. `jacocoTestReport` collects every `build/jacoco/*.exec`, so the report and the 80% gate still see unit **and** integration coverage from a single execution of each suite.

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

Gradle's own cache key includes the workflow, job and matrix values, so the JDK 17 and 21 matrix legs maintain separate entries instead of evicting one another.

### Testcontainers Docker images

The suites pull `confluentinc/cp-kafka`, `confluentinc/cp-schema-registry` and `clickhouse/clickhouse-server` on every run. These images are deliberately **not** cached: measurement showed an Actions cache restore plus `docker load` is slower than pulling from Docker Hub, and the tarballs would consume roughly 3.7 GB of the 10 GB repository cache budget. See #76 for the numbers.

## Concurrency and path filters

`ci.yml` and `pr-checks.yml` each declare a `concurrency` group with `cancel-in-progress: true` — keyed on the ref for `ci.yml` and on the pull-request number for `pr-checks.yml` — so pushing several commits in quick succession leaves only the newest pipeline running.

Documentation-only changes skip the heavy jobs. `ci.yml` uses `paths-ignore` on its `push` trigger. `pr-checks.yml` cannot use `paths-ignore` without leaving required checks permanently pending, so it instead runs a cheap `changes` job that diffs the PR against its base and exports `code=true|false`; `lint`, `build-check`, `test-matrix` and `coverage` all gate on it. A docs-only PR therefore reports those checks as skipped rather than absent. Both filters treat `docs/`, any `*.md`, `LICENSE`, `NOTICE` and `.gitignore` as documentation; anything else — including `.github/` itself — counts as code.

## Coverage gates

Three things report on coverage, and only two of them can fail a build. Knowing which is which matters, because the most visible one is the one that cannot.

| Mechanism | Can it fail? | What it covers |
| --- | --- | --- |
| Gradle `jacocoTestCoverageVerification` | **Yes** — fails the `coverage` job | 80% line coverage project-wide, plus per-package rules on `…confluent.serialize` and `…confluent.deserialize` |
| Codecov `project` / `patch` statuses | **Yes**, once required (see below) | Project-wide, and coverage of the lines the PR changed |
| `madrapps/jacoco-report` PR comment | **No** | Nothing — it is a report only |

The `madrapps/jacoco-report` step's `min-coverage-overall` and `min-coverage-changed-files` inputs only select the pass/fail emoji in the comment it posts; the action has no failure path for a threshold breach, and its `continue-on-error` input governs runtime exceptions rather than coverage. Treat that comment as information, never as a gate.

`codecov.yml` defines the `project` and `patch` statuses with `informational: false`, which is what makes them capable of failing — an informational status always reports success. The `patch` status is the one that matters most, because it is the only gate on *changed-line* coverage: Gradle's project-wide rule will happily stay above 80% while a PR adds uncovered code.

Two of the steps are outside the repository and must be done once, in repository settings:

1. **Install the Codecov GitHub App.** Without it Codecov cannot create statuses at all — uploads still succeed and it will still comment (as `codecov-commenter` rather than `codecov[bot]`), but no check appears, so nothing can block. This is the state the repository was in before this configuration existed.
2. **List `codecov/project` and `codecov/patch` as required status checks** in the ruleset that protects the default branch. A red status that is not required is advisory only.

Both `codecov/codecov-action` invocations use `fail_ci_if_error: true`, so an upload that fails fails the job rather than passing silently and leaving Codecov to report on stale data.
