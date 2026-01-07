# Changelog

All notable changes to this project will be documented in this file.

## [v2.6.0] - 2026-01-07

###  More SQL Features

#### JOIN Enhancements
- **LEFT OUTER JOIN**: Returns all rows from left table, NULL for unmatched right rows
- **RIGHT OUTER JOIN**: Returns all rows from right table, NULL for unmatched left rows
- **FULL OUTER JOIN**: Returns all rows from both tables with NULL for non-matches
- **CROSS JOIN**: Cartesian product (no ON clause required)
- AQL equivalents: `GABUNG KIRI`, `GABUNG KANAN`, `GABUNG SILANG`

```sql
-- LEFT JOIN example
SELECT * FROM employees LEFT JOIN departments ON employees.dept_id = departments.id

-- CROSS JOIN example
SELECT * FROM colors CROSS JOIN sizes
```

#### DISTINCT Keyword
- Remove duplicate rows from query results
- Applied after column projection for correct behavior

```sql
SELECT DISTINCT category FROM products
```

#### HAVING Clause
- Filter grouped results after aggregation
- Supports: `=`, `!=`, `<>`, `>`, `<`, `>=`, `<=`

```sql
HITUNG COUNT(*) DARI sales GROUP BY region HAVING count > 5
```

#### EXPLAIN Query Plan
- Analyze query execution strategy before running
- Shows: scan type, index usage, join methods, processing steps

```sql
EXPLAIN SELECT * FROM users WHERE id = 5
-- Returns: { type: "SELECT", steps: [{ operation: "INDEX SCAN", method: "B-Tree Index Lookup" }] }
```

###  Security Improvements
- **Password Hashing**: Server authentication now uses SHA-256 with random salt
- **Timing-Safe Comparison**: Prevents timing attacks on password verification
- **Input Validation**: Table and column names validated against injection
- **Regex Injection Fix**: LIKE operator now escapes regex metacharacters

###  Performance Optimizations
- **Query Cache**: Replaced expensive `JSON.parse(JSON.stringify())` with shallow clone
- **True LRU Cache**: Pager now properly tracks access order for eviction
- **B-Tree Binary Search**: Index operations now use O(log n) binary search
- **Aggregate Functions**: MIN/MAX use single-pass loop instead of spread operator (prevents stack overflow on large datasets)
- **Reduced JSON Parsing**: DELETE/UPDATE operations parse JSON once instead of multiple times

###  Bug Fixes
- **Fixed `_deleteFullScan()`**: Method was passing undefined table name
- **Removed duplicate code**: Cleaned up duplicate `_loadIndexes()`, pager checks, and exports
- **Division by zero**: AVG now returns `null` for empty datasets instead of `Infinity`
- **Type safety**: Added `parseInt` radix, changed loose equality (`==`) to strict (`===`)
- **Buffer pool leak**: Buffers now returned to pool on read errors

###  Code Quality
- Removed duplicate method definitions
- Added proper error handling for empty catch blocks
- Improved code comments and documentation

---

## [v2.5.0] - 2026-01-07

### 🚀 Major Performance Update
- **Object Caching (Page-Level)**: Implemented a memory-resident Object Cache in `Pager.js`.
    - **Zero-Copy Reads**: Bypasses `JSON.parse` overhead for hot pages.
    - **Performance**: SELECT (Indexed) jumped from ~60k to **~247,000 TPS**.
- **Hash Join**: Optimized `JOIN` operations from O(M*N) to O(M+N).
    - **Faster Queries**: Complex joins reduced from ~2900ms to ~40ms.
- **Query Plan Caching**: Implemented LRU Cache for parsed queries to reduce CPU overhead on repeated queries.

### ✨ Added
- **Async WAL (Write-Ahead Logging)**: Refactored for non-blocking stream-based writes.
- **CLI Enhancements**: Detailed help menus, SQL aliases, and database switching in `local.js`.
- **New Tools**: Added CLI Unit Tests (`cli/test.js`) and Performance Benchmark (`cli/benchmark.js`).

### 🐛 Bug Fixes
- **Persistence**: Fixed critical bug where Indexes were lost on restart (Added `_indexes` system table).
- **File Locking**: Fixed Windows `EPERM` issues during `DROP DATABASE`.
- **Query Parser**: Fixed Operator Precedence (`AND` > `OR`) and escaped quotes handling.

---

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
