# CI workflow ownership

Each workflow owns a distinct slice of the pipeline so that no Gradle task runs more than once for the same commit on the same JDK. Testing on multiple JDKs is intentional and is *not* considered duplication — the rule is one run of each task **per JDK** per event.

## Workflows

| Workflow | Trigger | Owns |
| --- | --- | --- |
| `pr-checks.yml` | `pull_request` | All pull-request signal: lint, build/JAR check, dependency review, the JDK 17 + 21 unit-test matrix, and one integration-test + coverage run on the primary JDK. |
| `ci.yml` | `push` to `main`/`master`, `workflow_dispatch` | The canonical post-merge build: one job that builds, runs unit + integration tests, verifies coverage and uploads to Codecov, plus a parallel lint job. |
| `publish-snapshot.yml` | `workflow_run` after a successful **CI** run on `main`/`master`, `workflow_dispatch` | Publishing snapshots. It does not re-test: it is gated on the CI run for the same SHA. |
| `release.yml` | tag `v*` / release created | Release verification and publishing to Maven Central. |

`ci.yml` deliberately does **not** trigger on `pull_request`; that is what makes the PR path free of same-JDK repeats.

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

