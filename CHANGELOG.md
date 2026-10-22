## 0.3.0

### Changed

- Upgrade DuckDB to `v2.0.0` and extension-ci-tools to the latest commit.
- Require C++17, use Identifier for table-function output names, and replace deprecated type traits with `std::invoke_result_t`.
- Reuse DuckDB's annotated mutexes and locks instead of maintaining local threading utilities.
- Share void and value-returning hedged-request handling with C++17 `if constexpr`.
- Update extension metadata, CI pins, and source/prebuilt unit-test linkage.

### Fixed

- Mark filesystem wrapping as fallible so missing-filesystem errors remain regular execution errors on DuckDB v2; use nondeprecated vector output APIs.
- Preserve the first completed hedged result or exception when later requests finish.

## 0.2.4

### Changed

- Update DuckDB and extension-ci-tools to `v1.5.6`.

### Added

- Added descriptions, examples, parameter names, and categories for all
  extension functions in `duckdb_functions()`.

## 0.2.3

### Changed

- Upgrade DuckDB and extension-ci-tools to v1.5.5

## 0.2.2

### Changed

- Upgrade DuckDB and extension-ci-tools to v1.5.4

## 0.2.1

### Changed

- Upgrade DuckDB and extension-ci-tools to v1.5.3

## 0.2.0

### Changed

- Upgrade DuckDB and extension-ci-tools to v1.5.2

## 0.1.0

### Fixed

- Fix data race, which could lead to incorrect / default IO operation return ([#50])

[#50]: https://github.com/dentiny/duckdb-hedged-request/pull/50

- Add missing filesystem API implementation ([#44])

[#44]: https://github.com/dentiny/duckdb-hedged-request/pull/44

### Added

- Support hedged request for file deletion and directory creation ([#46], [#48])

[#46]: https://github.com/dentiny/duckdb-hedged-request/pull/46
[#48]: https://github.com/dentiny/duckdb-hedged-request/pull/48

## 0.0.3

### Changed

- Upgrade DuckDB core and extension-ci-tools to v1.5.1 ([#42])

[#42]: https://github.com/dentiny/duckdb-hedged-request/pull/42

### Fixed

- Fix glob implementation when extended version supported ([#43])

[#43]: https://github.com/dentiny/duckdb-hedged-request/pull/43

## 0.0.2

### Changed

- Update DuckDB and extension-ci-tools to v1.5.0

### Added

- Add extension description
