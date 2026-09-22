# Releasing

How to cut a release of `flink-proto-confluent`. Releases are driven by [release-please](https://github.com/googleapis/release-please): it keeps an open release pull request describing the next version, and **merging that pull request is the release**. Everything after the merge — tagging, the GitHub Release, signing, and publishing to [Maven Central](https://central.sonatype.com/artifact/com.bbrownsound/flink-proto-confluent) — is automated. Maintainers do not tag by hand and do not publish from a local machine.

## TL;DR

1. Land your work on `main` with [Conventional Commit](https://www.conventionalcommits.org/) messages.
2. Open [Pull requests](https://github.com/brbrown25/flink-proto-confluent/pulls) and find the one titled `chore(release): X.Y.Z`, opened by the release bot.
3. Read the `CHANGELOG.md` diff. It is the release notes, and the version in the title is the version you are about to publish.
4. Merge it.
5. Watch the `Release` workflow (`gh run watch`). On success the tag, the GitHub Release with the shadow JAR attached, and the Maven Central publish all exist.

There is no post-release version bump to remember — the next release pull request handles it.

## How versioning works

- **Conventional commits decide the version.** `fix:` bumps the patch, `feat:` bumps the minor, and a `!` or a `BREAKING CHANGE:` footer bumps the major. release-please reads every commit since the last release tag and computes the next version from the largest bump it finds.
- **`gradle.properties` records the last released version**, as `releaseVersion=X.Y.Z` between the `x-release-please-start-version` markers. release-please rewrites that line in the release pull request. Do not edit it by hand.
- **The tag drives what gets published.** `release.yml` strips the leading `v` from the tag and passes the result as `-Pversion=`, so tag `v1.1.0` publishes `1.1.0`.
- **Local builds get a snapshot.** With no `-Pversion=` override, `build.gradle.kts` derives the next patch after `releaseVersion` and appends `-SNAPSHOT` (`1.0.0` recorded → `1.0.1-SNAPSHOT` locally). A local build therefore can never produce a coordinate that has already been published.
- Releases use standard [semver](https://semver.org/) and tags are always `vMAJOR.MINOR.PATCH`.

## What happens when you merge the release pull request

1. **release-please** (`.github/workflows/release-please.yml`) sees the merge, creates annotated tag `vX.Y.Z`, and creates the GitHub Release with the changelog entry as its body.
2. **`release.yml`** fires on the tag:
   - **build-and-test** runs `test integrationTest jacocoTestReport jacocoTestCoverageVerification` and builds the shadow JAR. A failure here aborts before anything is published.
   - **publish** rebuilds the shadow JAR at the tag version, signs and publishes to Sonatype (`publishSonatypePublicationToSonatypeRepository closeAndReleaseStagingRepositories`, which auto-closes and releases the staging repository to Maven Central), then attaches the JAR to the GitHub Release that already exists.
3. **`publish-snapshot.yml`** skips this commit, since a snapshot of a release tree would only add a duplicate coordinate.

A `concurrency` group keyed on the tag ensures only one publish per version is ever in flight.

## Prerequisites (one-time, per repo)

| Secret | Purpose |
| --- | --- |
| `SONATYPE_USERNAME` | Sonatype Central Portal user token name. |
| `SONATYPE_PASSWORD` | Sonatype Central Portal user token secret. |
| `GPG_SIGNING_KEY` | **ASCII-armored** secret key: `gpg --armor --export-secret-keys KEY_ID`. |
| `GPG_SIGNING_PASSPHRASE` | Passphrase for that key. |
| `RELEASE_PLEASE_TOKEN` | PAT or GitHub App installation token used to open the release pull request. Optional but strongly recommended — see below. |

Notes:

- The GPG key must be ASCII-armored (starts with `-----BEGIN PGP...`). The snapshot workflow has a debug step that checks this on failure without logging the key.
- The signing public key must be published to a public keyserver so Sonatype can verify signatures.
- `GITHUB_TOKEN` (auto-provided) creates the tag, the GitHub Release, and the GitHub Packages publish; no manual setup needed.
- **`RELEASE_PLEASE_TOKEN` matters.** A pull request opened with the default `GITHUB_TOKEN` does not trigger workflow runs, so the release pull request would arrive with no CI and could be merged unverified. The workflow falls back to `GITHUB_TOKEN` so it still functions without this secret, but set it before relying on the flow.
- **Branch protection.** The repository ruleset requires an approving review. The release bot cannot approve its own pull request and a sole maintainer cannot approve it either, so the bot needs bypass-actor status on the ruleset, or the approval requirement needs an explicit carve-out for release pull requests.

## Before you merge the release pull request

Run the full gate locally if you want early warning — it mirrors what CI and the Release workflow run:

```bash
make coverage   # test + integrationTest + jacoco report + 80% coverage gate
make check      # checkstyle + SpotBugs + Spotless + coverage + integration tests
```

Then confirm:

- [ ] `main` CI is green for the commit the release pull request is based on.
- [ ] The computed version in the pull request title is the version you actually intend. If a commit was mislabelled (a `feat:` that should have been `fix:`, or a missing breaking-change footer), fix the history or override the version rather than merging the wrong number.
- [ ] The `CHANGELOG.md` diff reads as sensible release notes.
- [ ] `flinkVersion` / `confluentVersion` / `protoVersion` in `gradle.properties` are correct for this release.

### Overriding the computed version

To force a specific version, add a `Release-As:` footer to a commit on `main` (an empty commit is fine) and release-please will retarget its pull request:

```bash
git commit --allow-empty -m "chore: release 2.0.0

Release-As: 2.0.0"
```

## Verifying the release

- **Workflow:** `gh run watch` (or the Actions tab) — both `release.yml` jobs green.
- **GitHub Release:** appears at `releases/tag/vX.Y.Z` with the changelog entry as the body and `flink-proto-confluent-X.Y.Z.jar` attached.
- **Maven Central:** the sync from Sonatype takes ~15–30 min (search indexing can take longer). Check `https://repo1.maven.org/maven2/com/bbrownsound/flink-proto-confluent/X.Y.Z/`.
- **Smoke test:** in a scratch project, resolve `com.bbrownsound:flink-proto-confluent:X.Y.Z` from `mavenCentral()` and confirm it downloads.

## Snapshots (automatic)

You do not cut snapshots manually. [`Publish Snapshot`](../.github/workflows/publish-snapshot.yml) runs after every successful CI run on `main`/`master` (and via `workflow_dispatch`) and publishes to the [Sonatype snapshot repository](https://central.sonatype.com/repository/maven-snapshots/).

Snapshot coordinates carry the short commit SHA they were built from — `1.0.1-a1b2c3d-SNAPSHOT` — so each one identifies exactly one commit and a consumer can pin to the build they tested against. The numeric part is the next patch after the last release, which is a placeholder for "unreleased work on `main`": the real next version is whatever release-please computes from the conventional commits, and may be a minor or major instead.

Release commits (`chore(release): ...`) are skipped, so a tagged tree publishes a release and nothing else.

## Fallback: releasing without release-please

If the bot is unavailable, the tag push path still works end to end:

```bash
git checkout main && git pull
git tag -a v1.1.0 -m "Release 1.1.0"
git push origin v1.1.0
```

`release.yml` waits ~60s for a GitHub Release to appear and, finding none, creates one itself with auto-generated notes and the JAR attached. Afterwards, update `releaseVersion` in `gradle.properties` and `.release-please-manifest.json` to the version you just published so release-please picks up from the right place.

Do not create the release through the GitHub UI: that pushes the tag and can leave release-please and this workflow racing to create the same release.

## Rollback / mistakes

- **Wrong version in the release pull request:** do not merge. Use a `Release-As:` footer (above) or correct the offending commit message, and release-please will update the pull request.
- **Bad build caught by the gate:** the tag and GitHub Release exist but nothing reached Central. Fix forward and release again; delete the tag and release only if you intend to reuse the version.

  ```bash
  gh release delete v1.1.0 --cleanup-tag
  ```
- **Already released to Maven Central:** the coordinates are **immutable** — you cannot overwrite `1.1.0`. Publish a new patch (`v1.1.1`) with the fix and note the bad version in the release notes.
- **GitHub Release created but Central publish failed:** check the `publish` job logs (signing-key format and Sonatype credentials are the usual causes). Fix the secret, then re-run the failed job from the Actions tab — no need to delete and re-push the tag.

## Reference

- Release pull request workflow: [`.github/workflows/release-please.yml`](../.github/workflows/release-please.yml)
- Publish workflow: [`.github/workflows/release.yml`](../.github/workflows/release.yml)
- Snapshot workflow: [`.github/workflows/publish-snapshot.yml`](../.github/workflows/publish-snapshot.yml)
- release-please config: [`release-please-config.json`](../release-please-config.json), [`.release-please-manifest.json`](../.release-please-manifest.json)
- Build / publish config: [`build.gradle.kts`](../build.gradle.kts) (`nexusPublishing`, `publishing`, `signing` blocks)
- Version property: [`gradle.properties`](../gradle.properties)
