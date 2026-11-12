# Releasing

runnerq-go follows [semver](https://semver.org). Release notes are
auto-drafted by [release-drafter](https://github.com/release-drafter/release-drafter)
from merged PR titles + labels. Tagging is the only manual step.

## How notes get drafted

Every PR merge to `main` runs
[`.github/workflows/release-drafter.yml`](.github/workflows/release-drafter.yml),
which:

1. Looks at every PR merged since the last tag.
2. Categorizes each by label (see
   [`.github/release-drafter.yml`](.github/release-drafter.yml)).
3. Resolves the next semver bump (`major` / `minor` / `patch`) from the
   highest-impact label across that batch.
4. Updates an unpublished draft release named `vNEXT` with the formatted
   notes.

Labels are applied automatically by an autolabeler that maps Conventional
Commit prefixes in the PR title to labels:

| PR title prefix | Auto-label | Bumps |
|---|---|---|
| `feat:` | `feature` | minor |
| `fix:` | `fix` | patch |
| `perf:` | `perf` | patch |
| `docs:` | `docs` | patch |
| `chore:` | `chore` | patch |
| `refactor:` | `refactor` | patch |
| `security:` | `security` | patch |
| `build:` | `build` | patch |
| `ci:` | `ci` | patch |
| `deps:` / `chore(deps):` | `deps` | patch |
| Anything ending `!:` (e.g. `feat!:`) | `breaking` | **major** |
| Body contains `BREAKING CHANGE` | `breaking` | **major** |

Override the auto-applied label by setting one manually on the PR — the
autolabeler doesn't strip existing labels.

## Cutting a release

Once `main` is in a state you want to release:

```bash
git checkout main
git pull
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

The `Release` workflow ([`.github/workflows/release.yml`](.github/workflows/release.yml))
fires on the tag push, finalizes the draft (matching the tag and name), and
publishes it.

That's it — no binary artifacts to build, no `go release` step needed. Go's
[module proxy](https://proxy.golang.org/) auto-discovers the new tag the
moment any user runs `go get github.com/alob-mtc/runnerq-go@vX.Y.Z`.

## Versioning policy

- **v0.x**: pre-1.0. API may change between minors. Pseudo-versions
  (`v0.0.0-<date>-<sha>`) are how downstream apps pin to specific
  commits when no tag covers their need.
- **v1.0.0+**: stable public API. Breaking changes require a `v2`
  module path (`github.com/alob-mtc/runnerq-go/v2`), per
  [Go module versioning](https://go.dev/ref/mod#major-version-suffixes).

The "public API" surface includes:

- The `runnerq` package: `Builder()`, `WorkerEngine`, `WorkerConfig`,
  `ActivityHandler`, `ActivityContext`, `ActivityExecutor`,
  `ActivityFuture`, `ActivityBuilder`, error types and constants.
- The `storage` package: `Storage`, `QueueStorage`, `ResultStorage`,
  `QueryStorage`, `CommandStorage`, `LeaseConfigurer` interfaces and the
  supporting data types.
- The `storage/storagetest` package: `Run()`, `Harness`, `Reader`.
- The `storage/postgres` package: `New()`, `WithConfig()`,
  `PostgresBackend`.
- The `conductor` module (`github.com/alob-mtc/runnerq-go/conductor`):
  `Start()`, `Config`, `Agent`. It is its own module so the SDK doesn't
  depend on what only the agent needs. Release it with the SDK: tag
  `conductor/vX.Y.Z` on the same commit as `vX.Y.Z`, after pointing
  `conductor/go.mod`'s `require github.com/alob-mtc/runnerq-go` at `vX.Y.Z`
  (its `replace ../` serves development in this repository only).

Internal types (lowercase) and the `examples/` directory carry no
stability guarantee.

## What is *not* covered by stability guarantees

- Database schema columns added defensively for future use, where a
  breaking change to the column would also bump the major.
- The `ShutdownGraceSeconds` default (30s). Operational tuning, may
  change.
- Exact wording of structured-log fields. Field *names* are stable;
  human-readable messages are not.

## Pre-release flow (optional)

If you want a `vX.Y.Z-rc.N` to live alongside the stable line for a
while, the same tag-and-publish flow applies — release-drafter handles
pre-release tags fine. The Go module proxy treats them as pre-releases
(`go get`'s default `@latest` will not pick them up; users opt in
explicitly).

## After publishing

1. Bump the example `go.mod` files if the new release adds APIs you
   want the examples to demonstrate.
2. Update the README's installation snippet if needed.
3. Announce the release however you announce things — the published
   GitHub Release URL is `https://github.com/alob-mtc/runnerq-go/releases/tag/vX.Y.Z`.
