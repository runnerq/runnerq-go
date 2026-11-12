# Security policy

## Reporting a vulnerability

Report vulnerabilities privately through GitHub:
[open a private advisory](https://github.com/runnerq/runnerq-go/security/advisories/new)
(Security tab, "Report a vulnerability"). Please do not open a public issue,
pull request or discussion for a suspected vulnerability.

Include what you can of: the affected package and version (or commit), how to
reproduce it, and the impact you see. We will acknowledge the report, keep
you updated in the advisory, and credit you when the fix is published unless
you ask us not to.

## Supported versions

RunnerQ is pre-1.0. Fixes land on `main` and ship in the next release; older
minor versions do not get backports.

| Version | Supported |
| --- | --- |
| latest release | yes |
| older releases | no |

## Scope

This repository: the `runnerq` package, `storage` and its Postgres backend,
and the `conductor` module. The shared contract lives in
[runnerq-spec](https://github.com/runnerq/runnerq-spec) and the TypeScript SDK
in [runnerq-ts](https://github.com/runnerq/runnerq-ts); report issues there in
those repositories.
