# Changelog

All notable changes to this project will be documented in file.

## [v3.0.0] - UPCOMMING

### Major Features - AKAD & TEROPONG

#### AKAD (Transactions) - ACID Compliance
- **Transaction Support**: Full ACID-compliant transactions with in-memory buffering
- **AQL Syntax**:
    - `MULAI AKAD` - Begin transaction
    - `SAHKAN` - Commit transaction
    - `BATALKAN` - Rollback transaction
- **Generic SQL Syntax**: `BEGIN TRANSACTION`, `COMMIT`, `ROLLBACK`
- **Implementation**:
    - New `TransactionManager.js` service for transaction state management
    - Operations buffered in memory during active transaction
    - Atomic commit: all operations succeed or all fail
    - Isolation: uncommitted data not visible to SELECT queries
- **Use Cases**:
    - Multi-step data modifications
    - Financial transactions
    - Batch operations with rollback capability

#### TEROPONG (Views) - Virtual Tables
- **View Support**: Create virtual tables from stored SELECT queries
- **AQL Syntax**:
    - `PASANG TEROPONG [nama] SEBAGAI [query]` - Create view
    - `BUANG TEROPONG [nama]` - Drop view
    - `PANEN * DARI [view_name]` - Query view like a table
- **Generic SQL Syntax**: `CREATE VIEW`, `DROP VIEW`
- **Implementation**:
    - New `ViewManager.js` service for view lifecycle
    - Views stored in `_views` system table
    - No data duplication - only query definition stored
    - Automatic view resolution in SELECT executor
- **Benefits**:
    - Query abstraction and reusability
    - Security through column-level access control
    - Simplified complex queries

#### KENTONGAN (Triggers) - Event Automation
- **Event Hooks**: Automate actions on `INSERT`, `UPDATE`, `DELETE`
- **AQL Syntax**: `PASANG KENTONGAN [nama] PADA [event] [table] LAKUKAN [query]`
- **Implementation**:
    - New `TriggerManager.js` service
    - Triggers stored in `_triggers` system table
    - Synchronous execution within transaction scope

#### SOP (Stored Procedures) - Macro Scripts
- **Stored Logic**: Save and reuse complex queries
- **AQL Syntax**: `SIMPAN SOP [nama] SEBAGAI [query]`
- **Execution**: `JALANKAN SOP [nama]`
- **Implementation**:
    - New `ProcedureManager.js` service
    - Procedures stored in `_procedures` system table

#### CABANG (Replication) - High Availability
- **Primary-Replica Sync**: Basic master-slave replication via CDC
- **AQL Syntax**: `SETEL CABANG SEBAGAI [role] ...`
- **Implementation**:
    - New `ReplicationManager.js` service
    - Real-time event broadcasting from Primary
    - Automatic event application on Replica

#### BLUSUKAN (Full-Text Search) - Search Engine
- **Inverted Index**: Fast text search capabilities
- **AQL Syntax**: `BLUSUKAN KE [table] CARI "[term]"`
- **Implementation**:
    - New `SearchManager.js` service
    - Simple Inverted Index stored in `_fts_index`

#### POS RONDA (Security) - Access Control
- **RBAC**: Table-level permissions (Read/Write)
- **AQL Syntax**: `BERI IZIN ...` / `CABUT IZIN ...`
- **Implementation**:
    - New `SecurityManager.js` service
    - Permissions stored in `_permissions` system table

### Architecture Changes
- **New Services**:
    - `src/services/TransactionManager.js` - Transaction state and buffering
    - `src/services/ViewManager.js` - View definition and execution
- **Parser Enhancements**:
    - Added `parseBeginTransaction()`, `parseCreateView()`, `parseDropView()`
    - Extended command types: `BEGIN_TRANSACTION`, `COMMIT`, `ROLLBACK`, `CREATE_VIEW`, `DROP_VIEW`
- **Engine Integration**:
    - Transaction-aware INSERT/UPDATE/DELETE routing
    - View resolution in SELECT queries
    - System table initialization for `_views`

### Documentation
- **Updated**: `README.md` - Added v3.0 features to feature list and syntax table

### Roadmap Preview
Next features in development:
- *Belum nemu ide*
---

## [v2.6.0] - 2026-01-10

### Modular Architecture (Codebase Refactor)
- **Service-Oriented Core**: Split monolithic `WowoEngine.js` into specialized modules:
    - **Logic Services**: `ConditionEvaluator.js`, `JoinProcessor.js`.
    - **Managers**: `TableManager.js`, `IndexManager.js`.
    - **Executors**: Dedicated classes (Strategy Pattern) for `Select`, `Insert`, `Update`, `Delete`, `Aggregate`.
- **Server modularity**: Split `SawitServer.js` into `AuthManager.js`, `ClientSession.js`, `RequestRouter.js`, and `DatabaseRegistry.js`.
- **Maintainability**: Reduced file size complexity by ~60%, enabling easier feature expansion.

### True Multi-Threading (Worker Pool)
- **Worker Pool Architecture**: Migrated from `cluster` module to `worker_threads` for true parallelism.
- **IO/CPU Separation**: Main thread handles Networking (IO), Worker threads handle Query Execution (CPU).
- **High Concurrency**: Architecture supports thousands of concurrent connections without blocking.
- **Load Balancing**: Implemented "Least-Busy" strategy to distribute queries to the least loaded worker.
- **Fault Tolerance**:
    - **Auto-Healing**: Workers automatically restart upon crash.
    - **Anti-Stuck**: Pending queries on crashed workers are immediately rejected/cleaned up.
- **Per-Worker Stats**: New `stats` command output shows query distribution and active load per worker.

### AQL Syntax Parity (Agricultural Query Language)
Full feature parity with Generic SQL. You can now use AQL for advanced queries:
- **JOINs**: `GABUNG` (Inner), `GABUNG KIRI` (Left), `GABUNG KANAN` (Right), `GABUNG SILANG` (Cross).
- **Ordering**: `URUTKAN BERDASARKAN [field] NAIK|TURUN` (Order By).
- **Pagination**: `HANYA [n]` (Limit), `MULAI DARI [n]` (Offset).
- **Grouping**: `KELOMPOK [field]` (Group By), `DENGAN SYARAT` (Having).
- **Compatibility**: Standard SQL syntax (`JOIN`, `ORDER BY`, `LIMIT`) remains fully supported alongside AQL.

### Advanced SQL Features
#### JOIN Enhancements
- **LEFT OUTER JOIN**: Returns all rows from left table, NULL for unmatched right rows
- **RIGHT OUTER JOIN**: Returns all rows from right table, NULL for unmatched left rows
- **FULL OUTER JOIN**: Returns all rows from both tables with NULL for non-matches
- **CROSS JOIN**: Cartesian product (no ON clause required)

```sql
-- LEFT JOIN example
SELECT * FROM employees LEFT JOIN departments ON employees.dept_id = departments.id
```

#### DISTINCT & AGGREGATE
- **DISTINCT**: `SELECT DISTINCT category FROM products` (or `PANEN UNIK ...`) to remove duplicates.
- **HAVING Clause**: `GROUP BY region HAVING count > 5` to filter aggregated results.
- **EXPLAIN Query Plan**: `EXPLAIN SELECT ...` to analyze execution strategy, index usage, and cost.

### Security Improvements
- **Password Hashing**: Server authentication now uses SHA-256 with random salt
- **Timing-Safe Comparison**: Prevents timing attacks on password verification
- **Input Validation**: Table and column names validated against injection
- **Regex Injection Fix**: LIKE operator now escapes regex metacharacters

### 🛠 Performance & Code Quality
- **Query Cache**: Replaced expensive `JSON.parse` with shallow clone
- **True LRU Cache**: Pager now properly tracks access order for eviction
- **B-Tree Binary Search**: Index operations now use O(log n) binary search
- **Optimized Aggregates**: MIN/MAX use single-pass loop (prevents stack overflow)
- **Reduced JSON Parsing**: DELETE/UPDATE operations optimize parsing overhead
- **Bug Fixes**:
    - Fixed `_deleteFullScan()` undefined table name
    - Cleaned up duplicate code (`_loadIndexes`, pager checks)
    - AVG now returns `null` for empty datasets
    - Fixed buffer pool leaks on read errors

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
