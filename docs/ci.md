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
