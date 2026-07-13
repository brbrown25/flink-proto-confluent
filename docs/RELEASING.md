# Releasing

How to cut a release of `flink-proto-confluent`. Releases are built, signed, and
published to [Maven Central](https://central.sonatype.com/artifact/com.bbrownsound/flink-proto-confluent)
by the [`Release`](../.github/workflows/release.yml) workflow. Maintainers do not
publish from a local machine — the workflow does the signing and staging.

## TL;DR

```bash
# 1. Make sure main is green and you are on the commit you want to release.
git checkout main && git pull

# 2. Tag it. The tag (minus the leading v) becomes the release version.
git tag -a v1.0.0 -m "Release 1.0.0"
git push origin v1.0.0

# 3. Watch the Release workflow. On success it publishes to Maven Central
#    and opens a GitHub Release with the shadow JAR attached.
gh run watch
```

Then bump `gradle.properties` to the next `-SNAPSHOT` (see
[After the release](#after-the-release)).

## How versioning works

- The single source of truth on `main` is `gradle.properties`:
  `version=1.0.0-SNAPSHOT`.
- The build reads that property but lets a `-Pversion=` override win
  (`build.gradle.kts` line 14).
- On a tag push the workflow strips the leading `v` and passes the result as
  `-Pversion=` — so **the tag drives the released version**, not
  `gradle.properties`. Tag `v1.0.0` publishes `1.0.0`.
- `gradle.properties` intentionally stays on a `-SNAPSHOT` version so every
  merge to `main` keeps publishing snapshots (see
  [Snapshots](#snapshots-automatic)).

Use standard [semver](https://semver.org/): `vMAJOR.MINOR.PATCH`. The tag must
start with `v` — the workflow only triggers on `v*` and only strips a leading
`v`.

## Prerequisites (one-time, per repo)

The publish job needs these GitHub Actions secrets. Confirm they exist under
**Settings → Secrets and variables → Actions** before the first release:

| Secret | Purpose |
| --- | --- |
| `SONATYPE_USERNAME` | Sonatype Central Portal user token name. |
| `SONATYPE_PASSWORD` | Sonatype Central Portal user token secret. |
| `GPG_SIGNING_KEY` | **ASCII-armored** secret key: `gpg --armor --export-secret-keys KEY_ID`. |
| `GPG_SIGNING_PASSPHRASE` | Passphrase for that key. |

Notes:
- The GPG key must be ASCII-armored (starts with `-----BEGIN PGP...`). The
  snapshot workflow has a debug step that checks this on failure without logging
  the key.
- The signing public key must be published to a public keyserver so Sonatype can
  verify signatures.
- `GITHUB_TOKEN` (auto-provided) is used to create the GitHub Release and to
  publish to GitHub Packages; no manual setup needed.

## Pre-release checklist

Run the full gate locally first — it mirrors what CI and the Release workflow
run, so failures surface before you tag:

```bash
make coverage   # test + integrationTest + jacoco report + 80% coverage gate
make check      # checkstyle + SpotBugs + Spotless + coverage + integration tests
```

Then confirm:

- [ ] `main` CI is green for the commit you intend to release.
- [ ] Working tree is clean and you are on `main` at the intended commit.
- [ ] `flinkVersion` / `confluentVersion` / `protoVersion` in `gradle.properties`
      are correct for this release.
- [ ] README dependency snippets reference the version you are about to publish
      (they currently show `1.0.0`).
- [ ] The tag version does not already exist on Maven Central (releases are
      immutable — you cannot republish the same coordinates).

## Cutting the release

Prefer tagging from the CLI over creating a release in the GitHub UI:

```bash
git checkout main && git pull
git tag -a v1.0.0 -m "Release 1.0.0"
git push origin v1.0.0
```

The push fires the `Release` workflow, which:

1. **build-and-test** — runs `test integrationTest jacocoTestReport
   jacocoTestCoverageVerification` and builds the shadow JAR. A failure here
   aborts the release before anything is published.
2. **publish** — extracts the version from the tag, rebuilds the shadow JAR at
   that version, signs and publishes to Sonatype
   (`publishSonatypePublicationToSonatypeRepository closeAndReleaseStagingRepositories`
   — this auto-closes and releases the staging repo to Maven Central), then
   creates a GitHub Release with the JAR attached and auto-generated notes.

> **Do not also create the release manually in the GitHub UI.** Creating a
> release there pushes the tag *and* fires the `release: created` trigger, so the
> workflow runs twice concurrently and both `publish` jobs race on the same
> Sonatype staging repository. Push the tag from the CLI and let the workflow
> create the GitHub Release for you.

## Verifying the release

- **Workflow:** `gh run watch` (or the Actions tab) — both jobs green.
- **GitHub Release:** appears at `releases/tag/v1.0.0` with
  `flink-proto-confluent-<version>.jar` attached.
- **Maven Central:** the sync from Sonatype takes ~15–30 min (search indexing
  can take longer). Check
  `https://repo1.maven.org/maven2/com/bbrownsound/flink-proto-confluent/1.0.0/`.
- **Smoke test:** in a scratch project, resolve
  `com.bbrownsound:flink-proto-confluent:1.0.0` from `mavenCentral()` and confirm
  it downloads.

## After the release

Bump `main` to the next development snapshot so snapshot publishing resumes on a
fresh version:

```bash
# edit gradle.properties: version=1.1.0-SNAPSHOT   (next MINOR, or PATCH as appropriate)
git commit -am "chore(release): bump to 1.1.0-SNAPSHOT. NOTICKET"
git push origin main
```

Update the version referenced in the README dependency snippets if you want them
to point at the just-released version.

## Snapshots (automatic)

You do not cut snapshots manually. The
[`Publish Snapshot`](../.github/workflows/publish-snapshot.yml) workflow runs on
every push to `main`/`master` (and via `workflow_dispatch`). It publishes to the
[Sonatype snapshot repository](https://central.sonatype.com/repository/maven-snapshots/)
only when the current version ends in `-SNAPSHOT`; on a release-versioned commit
the publish step is skipped. This is why `gradle.properties` should always carry
a `-SNAPSHOT` version between releases.

## Rollback / mistakes

- **Bad build caught by the gate:** nothing was published. Fix, delete the tag,
  re-tag.

  ```bash
  git push origin :refs/tags/v1.0.0   # delete remote tag
  git tag -d v1.0.0                    # delete local tag
  ```
- **Already released to Maven Central:** the coordinates are **immutable** — you
  cannot overwrite `1.0.0`. Publish a new patch (`v1.0.1`) with the fix. If the
  release is genuinely broken, you can also cut the new version and note the bad
  one in the GitHub Release notes.
- **GitHub Release created but Central publish failed:** check the `publish` job
  logs (signing-key format and Sonatype credentials are the usual causes; the
  snapshot workflow's debug step is a good reference for the key-format check),
  fix the secret, delete and re-push the tag.

## Reference

- Release workflow: [`.github/workflows/release.yml`](../.github/workflows/release.yml)
- Snapshot workflow: [`.github/workflows/publish-snapshot.yml`](../.github/workflows/publish-snapshot.yml)
- Build / publish config: [`build.gradle.kts`](../build.gradle.kts) (`nexusPublishing`, `publishing`, `signing` blocks)
- Version property: [`gradle.properties`](../gradle.properties)
