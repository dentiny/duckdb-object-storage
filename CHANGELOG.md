# 0.2.0

## Changed

- Upgrade DuckDB to `v2.0.0` and extension-ci-tools to the latest commit.
- Update static extension registration for v2 C++ unit-test linkage.
- Require C++17 and use constexpr string views for fixed names and the filesystem URI prefix.
- Use DuckDB identifiers for table-function column names and initialization-setting keys.
- Replace deprecated SQL quoting helpers with `SQLString` and `SQLIdentifier`.
- Read SlateDB's async thread count from DuckDB's v2 configuration while retaining the CPU thread count for blocking tasks.
- Annotate filesystem initialization locking with DuckDB's thread-safety utilities.

# 0.1.0

- Initial SlateDB-backed object-storage filesystem release.
