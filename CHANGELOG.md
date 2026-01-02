# Changelog

All notable changes to this project will be documented in this file.

## [v2.4] - 2026-01-02

### Security
- **Parameterized Queries**: Implemented full support for parameterized queries to prevent AQL injection (Reported by @nastarkejuu).
    - Updated `SawitClient` to send parameters.
    - Updated `SawitServer` and `WowoEngine` to bind parameters safely.
    - Updated `QueryParser` to handle `@param` placeholders.

### Documentation
- **Enhanced Docs**: Updated `docs/index.html` to match README feature set.
    - Added Benchmark results.
    - Added Dual Syntax (AQL vs SQL) comparison table.
    - Added complete Operators table.
- **Package Fixes**: Corrected invalid paths in `package.json` for `main`, `scripts`, and `bin`.

---

## [v2.3] - 2024-12-31

### 🚀 New Features
- **Generic SQL Support**: Added full support for standard SQL syntax alongside AQL.
    - `CREATE TABLE`, `INSERT INTO`, `SELECT`, `UPDATE`, `DELETE`, `DROP`.
    - `CREATE INDEX ON table (field)` syntax.
- **Advanced Operators**:
    - `IN (...)`, `NOT IN (...)`
    - `LIKE 'pattern%'` (Wildcard)
    - `BETWEEN min AND max`
    - `IS NULL`, `IS NOT NULL`
- **Pagination & Sorting**:
    - `ORDER BY field [ASC|DESC]`
    - `LIMIT n OFFSET m`
- **Native Data Types**: Improved `INSERT` parser to correctly handle `NULL`, `TRUE`, `FALSE` (boolean/null) instead of strings.

### ⚡ Performance
- **Tokenizer Optimization**: Fixed Regex parser to correctly identify `<` and `>` operators.
- **Benchmark**:
    - INSERT: ~3,125 ops/sec
    - SELECT (PK): ~3,846 ops/sec
    - SELECT (Scan): ~4,762 ops/sec
    - UPDATE: ~3,571 ops/sec

### 🐛 Bug Fixes
- Fixed "Normal Ops" parser fallthrough bug where simple comparisons (`>`, `<`) were sometimes misidentified.
- Fixed `CREATE INDEX` parser hanging code block.

---

## [v2.1] - 2024-12-30

### 🚀 New Features
- **Modular Architecture**: Refactored `WowoEngine.js` into modules (`Pager.js`, `QueryParser.js`, `BTreeIndex.js`).
- **Network Edition**: TCP Server implementation in `src/SawitServer.js`.
- **Multi-Database**: Support for `USE [db]` and separate `.sawit` files per database path.
