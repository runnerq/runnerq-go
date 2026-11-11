# Releasing

runnerq-go follows [semver](https://semver.org) as described under
[Versioning policy](#versioning-policy). Each release has two kinds of notes:

- [`CHANGELOG.md`](CHANGELOG.md): the curated summary, written as PRs land,
  under `## Unreleased`.
- The GitHub release: that changelog section followed by the list of merged
  PRs GitHub generates from their labels
  ([`.github/release.yml`](.github/release.yml)).

## While developing

- A PR that changes behaviour, the public API, the schema or the supported
  Go or Postgres versions adds a line to `## Unreleased` in `CHANGELOG.md`.
  Breaking changes say what breaks and how to move.
- Label every PR before merging, so the generated notes put it in the right
  section:

  | Label | Section | Bump it implies |
  | --- | --- | --- |
  | `breaking` | Breaking changes | minor before 1.0, major after |
  | `feature` | Features | minor |
  | `fix` | Bug fixes | patch |
  | `perf` | Performance | patch |
  | `security` | Security | patch |
  | `docs` | Documentation | none on its own |
  | `chore`, `refactor`, `ci`, `build`, `deps` | Maintenance | none on its own |
  | `skip-release-notes` | left out | none |

  Unlabelled PRs land under "Other changes".

## Cutting a release

1. Pick the version from the labels merged since the last tag and the
   policy below.
2. In a PR, rename `## Unreleased` in `CHANGELOG.md` to `## vX.Y.Z` (and
   start a new empty `## Unreleased` above it). If the conductor module is
   released too, point `conductor/go.mod`'s
   `require github.com/runnerq/runnerq-go` at `vX.Y.Z` and its
   `runnerq-spec` require at a tagged spec release.
3. After it merges, tag `main`:

   ```bash
   git checkout main && git pull
   git tag -a vX.Y.Z -m "vX.Y.Z"
   git tag -a conductor/vX.Y.Z -m "conductor/vX.Y.Z"   # same commit, when released
   git push origin vX.Y.Z conductor/vX.Y.Z
   ```

The `Release` workflow ([`.github/workflows/release.yml`](.github/workflows/release.yml))
runs on the `vX.Y.Z` tag: it takes the tag's section from `CHANGELOG.md`,
appends the generated PR list, and publishes the GitHub release. It fails if
the changelog has no section for the tag. A tag with a pre-release suffix
(`vX.Y.Z-rc.N`) is published as a pre-release.

There is nothing to build or upload: the Go module proxy picks up the tag
the first time someone runs `go get github.com/runnerq/runnerq-go@vX.Y.Z`.

## Versioning policy

- **v0.x (now).** Pre-1.0, the minor version is the breaking-change
  boundary: a release with any `breaking` PR bumps the minor (v0.6 → v0.7),
  and so does one with new features. A patch release (v0.6.0 → v0.6.1) only
  fixes bugs and never breaks the API or the schema. Pin a minor
  (`v0.6.x`) to stay compatible.
- **v1.0.0 and later.** Breaking changes need a new major and module path
  (`github.com/runnerq/runnerq-go/v2`), per
  [Go module versioning](https://go.dev/ref/mod#major-version-suffixes).
  Features bump the minor, fixes the patch.
- Between releases, depend on a commit with a pseudo-version
  (`go get github.com/runnerq/runnerq-go@<commit>`).

### What the policy covers

- The public API: exported names in `runnerq`, `executor`, `storage`,
  `storage/storagetest`, `storage/postgres`, and the `conductor` module
  (`Start()`, `Config`, `Agent`, `Handler`).
- The Postgres schema: from v0.6 on, schema changes are additive migrations
  (new tables, new nullable columns), so a new SDK can run beside the
  previous minor during a rolling deploy. The upgrade test in CI checks this
  against the previous release. A change that cannot be additive is a
  breaking change.
- Supported toolchains: Go 1.25, 1.26 and 1.27, and Postgres 14 to 18, as
  tested in CI. Raising the Go floor or dropping a Postgres major is a
  breaking change.

### What it does not cover

- Unexported names, `internal/` packages and `examples/`.
- Operational defaults such as `ShutdownGraceSeconds` (30s).
- Wording of log messages. Structured-log field *names* are stable.

## After publishing

1. Update the README's install snippet if it names a version.
2. Nothing to do for the upgrade test: the `upgrade` CI job upgrades from
   the latest `v*` tag from v0.6.0 on (before that, from the merge base with
   `main`, because earlier releases predate the runnerq-spec schema). Set
   the repository variable `UPGRADE_FROM` to a ref to override it.
