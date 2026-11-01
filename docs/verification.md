# Source verification

## Boundary

This is the read-only visitor/market edition, not Apollo's private computation or
trading engine. It includes generic market transport, result readers, local worker
lifecycle code and synthetic tests. Computed period Levels and its HTTP endpoint
are omitted. Selected stored result schemas, including the legacy levels schema,
remain readable: reading an admitted result does not calculate an indicator.

No credentials, terminal/account profiles, original datasets, private setup guides,
virtual environments, logs, native binaries or original repository history belong
in publication. `.gitignore` is a convenience, not a disclosure control: the
publication checker inspects actual tracked paths, modes and staged blobs, including
force-added files. It rejects links/junctions and checks both staged and working text
for recognizable secrets. Staged bytes must match the checked working source except
for Git's CRLF-to-LF conversion; cleaning a working file cannot hide a staged secret.
Its checks are not authorship, provenance or legal clearance.

## Synthetic checks

Verification targets Windows AMD64 and CPython 3.13. From an isolated environment,
install the hash-verified full verification lock shown below before running the
synthetic suite, including its tooling regressions:

```powershell
python -B -m unittest discover -s tests -v
```

These tests use generated temporary data and mocked native/process operations. They
must not require a terminal, account, real result selections or private host profiles.
They test selected-source history, dataset contracts, read-only HTTP/CORS restrictions,
WebSocket filtering and local lifecycle guards. They do not certify live feeds,
Windows login/session recovery on a real host, or production readiness.

For the complete local quality gate, install the full verification lock and run:

```powershell
python -m pip install --require-hashes --only-binary=:all: -r requirements-dev-win-py313.lock
python -B -m scripts.check_quality
```

The dev lock includes the exact runtime pins plus pinned verification tools. CI
must install this lock, not resolve the version ranges in `requirements-public.txt`.
The gate compiles every admitted Python file without running it, checks the full
publication boundary, runs all synthetic tests and audits both the runtime and full
verification locks. Both audits run even if one fails; advisories, unknown package
metadata and audit-service failures fail the gate. No automatic fixes or advisory
ignore list is used. This is source verification, not a live runtime deployment.

The generated runtime notices also have a canonical-whitespace check. After updating
their dependency-derived content, `python -B -m scripts.check_notices --write`
normalizes line endings/trailing whitespace while verifying the wording is unchanged.

### Legacy quality exception

Strict lint, docstring-presence, Ruff formatting and mypy checks apply to `scripts/`,
`tests/tooling/`, the period-level removal regressions and the explicit ASGI test-peer
helper/regressions and ownership-guard regressions. Existing runtime
source and legacy tests retain their current style and annotation coverage; their
compilation, synthetic tests and publication guards remain blocking. Mypy does not
follow imports into that legacy source. No full-runtime lint, formatting or type
success is claimed. This staged-adoption exception avoids mixing a broad runtime
cleanup with source publication; later changes must extend the policy deliberately,
not silently exclude new code to obtain green checks.

Before a public push, run the actual-index boundary check:

```powershell
python -B -m scripts.check_publication
```

Before Git exists, `--source` checks the selected authored roots instead. That is a
preparation check only; it is not interchangeable with tracked-file verification.
The checker also requires the publication and verification inputs to be present.

## Verification-only CI boundary

This Windows/Python repository uses backend-owned verification rather than the
shared Node web/Cloudflare Worker delivery profiles. This is an explicitly selected
source-only exception, not a deployment adapter or a successful no-op deployment.
Any activated workflows must use GitHub-hosted Windows runners, read-only tokens,
immutable action references and synthetic tests. They must not use the MT5 host as
a self-hosted runner or receive broker, tunnel or deployment credentials.

`verify-pr.yml` checks the PR merge revision and exposes the blocking `Verify`
result; failures or skipped verification cannot satisfy it. `verify-main.yml`
checks the exact merged main commit. `weekly-vuln-audit.yml` checks both dependency
locks on main and can also be requested manually. None creates a deployment.
Repository protections and review-app installation must be verified separately;
workflow files alone do not establish protection or remote CI success.

The local recovery implementation retains its fixed Windows task/account/dispatcher
checks. Those checks are security boundaries, not portable host provisioning. Actual
policy files, account identifiers, host configuration and dispatcher installation
remain private; do not weaken the checks to make a different host start successfully.

Publishing or merging this source must not install releases, launch workers, restart
the backend, change public ingress or mutate data. A future Windows runtime promotion
requires a separately reviewed, trusted manual release/recovery procedure. The
installed backend and its private operational records remain independent of this
repository.
