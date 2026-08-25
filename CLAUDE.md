# Claude Code Instructions for Fink-Broker Project

## Project Context

- Working directory: same directory as this CLAUDE.md file
- Git repository: `astrolabsoftware/fink-broker` (public), main branch `master`
- Current branch: `git branch --show-current`
- Astronomy/astrophysics project: alert broker built on Apache Spark, supporting several surveys
  (ZTF, Rubin), deployed on Kubernetes

## Architecture map

Read only what the task needs. Each subdirectory below with its own `CLAUDE.md` documents its own
rules — they load automatically when you work in it.

| Path | Role |
|---|---|
| `fink_broker/common/` | Code shared by all surveys (Spark, Avro, HBase, logging, partitioning) |
| `fink_broker/ztf/` | ZTF-specific processing |
| `fink_broker/rubin/` | Rubin/LSST-specific processing |
| `bin/` | Entry points: `fink` (main CLI), `fink_test_ztf`, `fink_test_rubin`, `fink_db`, `fink_shell`, plus per-survey Spark jobs in `bin/ztf/` and `bin/rubin/` |
| `conf/` | Runtime configuration, per survey (`conf/ztf`, `conf/rubin`, `conf/sso`) |
| `scheduler/` | Cron-style job scheduling, per survey |
| `e2e/` | End-to-end test scripts (CI, ArgoCD, diagnostics) |
| `chart/`, `deploy/` | Helm chart and Kubernetes deployment |
| `utest/` | Unit-test runner and datasets (`utest/bin/pytest.sh`, `utest/datasets/`) |
| `doc/` | Developer documentation (devel, e2e, monitor, release, troubleshoot) |

Boundary rule: `ztf/` and `rubin/` must not import from each other. Anything needed by both belongs
in `common/`.

## Commands

| Command | What it does |
|---|---|
| `./lint.sh` | `ruff check --fix --preview` then `ruff format --preview` on the whole repo |
| `./code-checks.sh -u` | Build the image, then run the pytest suite inside the container |
| `./code-checks.sh -m` | Build the image, then run `mypy` inside the container |
| `./build.sh` | Build the container image (via ciux; requires `$CIUXCONFIG`) |
| `e2e/run.sh` | End-to-end test run — see `doc/e2e.md` |
| `bin/fink_test_ztf --unit-tests` | Run the module doctests (needs the full Spark/Kafka environment) |

`pytest` and the Spark stack are **not** available on the host: tests run in the container through
`code-checks.sh`, or in the e2e environment. Never report tests as passing without having actually
run them.

## Coding standards

Enforced by `.ruff.toml` — read it before arguing with the linter:

- line length 88, 4-space indent, double quotes, target `py38`
- docstrings follow the **numpy** convention (pydocstyle `D` rules are enabled)
- rule families enabled: `E`, `W`, `F`, `N`, `D`, `B`, `C4`, `PD`, `PERF`

Write code that reads like the file around it. Run `./lint.sh` after changing Python code — it fixes
what is auto-fixable; the remaining violations are yours.

## Tests

This repository has **two** test mechanisms. Match the one already used by the module you touch.

1. **Doctests — the dominant style.** Functions document their behaviour in a numpy `Examples`
   section, and each module ends with:

   ```python
   if __name__ == "__main__":
       regular_unit_tests(globals())   # or spark_unit_tests(globals()) for Spark code
   ```

   They are executed module by module by `bin/fink_test_ztf --unit-tests` /
   `bin/fink_test_rubin --unit-tests` (`fink_broker/common/tester.py`), which needs the full
   Spark/Kafka environment — that is the e2e/CI environment, not the host.

2. **pytest files** — `fink_broker/<module>/test_*.py`, collected by `utest/bin/pytest.sh` through
   `./code-checks.sh -u` (containerized). Use these when the case needs fixtures or
   parametrization that a doctest cannot express readably.

Test datasets live in `utest/datasets/`.

- **Every bug fix starts with a failing test** that reproduces the bug, run and observed failing
  before the fix. That test stays in the suite as the regression guard.
- Test observable behaviour and the edge cases from the spec, not implementation details.
- Adding a doctest to a public function also improves its documentation — prefer it when the
  example is short and deterministic.

## Expected workflow

1. **Spec** — for any non-trivial change, state objective, out-of-scope, edge cases and acceptance
   criteria first, and get them validated before writing code.
2. **Tests** — turn the acceptance criteria into failing tests.
3. **Implementation** — the minimal code that makes them pass, without widening the scope.
4. **Lint** — `./lint.sh`.
5. **Review** — re-read the diff before concluding.

## Development guidelines

- Act as a devops and development expert
- Never add Claude as co-author
- All commit messages, logs and comments are in English
- Do what has been asked; nothing more, nothing less
- NEVER create files unless absolutely necessary for achieving the goal
- ALWAYS prefer editing an existing file to creating a new one
- NEVER proactively create documentation files (`*.md`) or README files unless explicitly requested

Auto-approved operations are declared in `.claude/settings.json`, not in prose here.
