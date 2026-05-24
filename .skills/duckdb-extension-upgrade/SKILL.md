---
name: upgrade-duckdb-extension
description: Upgrade the hedged_request_fs extension to a new DuckDB release. Use when the user asks to upgrade DuckDB, bump the duckdb submodule, sync to a new DuckDB tag (e.g. v1.5.3), or update the duckdb / extension-ci-tools submodules together.
---

# Upgrade hedged_request_fs to a new DuckDB release

Two submodules (`duckdb` and `extension-ci-tools`) move together to the same release tag. Then build, run two test suites, and write a changelog entry.

## Inputs

Before starting, confirm the target DuckDB version (e.g. `v1.5.3`). Everything else is derived from it. Below, `$TARGET` refers to that tag.

## Workflow

Track these as a checklist; do not skip ahead:

- 1. Pin `duckdb` submodule to `tags/$TARGET`
    + `cd duckdb && git fetch --tags origin && git checkout tags/$TARGET && cd ..`
- 2. Pin `extension-ci-tools` submodule to `$TARGET`
    + Note: `extension-ci-tools` ships the tag as a *branch* named `$TARGET`, not an annotated tag.
    + `cd extension-ci-tools && git fetch origin && git checkout $TARGET && cd ..`
- 3. Build: `CMAKE_BUILD_PARALLEL_LEVEL=10 make reldebug`
- 4. Run tests (both must pass):
    + Extension C++ unit test: `./build/reldebug/extension/hedged_request_fs/test/unittest/unittest_hedged_file_system`
    + SQL test: `make test_reldebug` (expands to `./build/reldebug/test/unittest "test/*"`)
- 5. Add a `CHANGELOG.md` entry at the top, bumping the patch version. Template:

  ```
  ## <next-version>

  ### Changed

  - Upgrade DuckDB and extension-ci-tools to $TARGET
  ```

## Verification

After step 4, `git diff --stat` should show exactly three files changed: `CHANGELOG.md`, `duckdb`, `extension-ci-tools`. Anything else means something went sideways.

`git submodule status` should report both submodules at `$TARGET`.

## Reference: historical upgrade commits

- `0487464` — `Upgrade duckdb to v1.5.2 (#52)`. Minimal: 2 submodule bumps + CHANGELOG. No source or CMake changes.
- `9872e24` — `Upgrade duckdb v1.5.1 (#42)`.
- `7513716` — `Upgrade to duckdb v1.5.0 (#41)`.
