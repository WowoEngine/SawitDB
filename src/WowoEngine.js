const Pager = require('./modules/Pager');
const QueryParser = require('./modules/QueryParser');
const BTreeIndex = require('./modules/BTreeIndex');
const WAL = require('./modules/WAL');
const DBEventHandler = require("./services/event/DBEventHandler");
const DBEvent = require("./services/event/DBEvent");


/**
 * SawitDB implements the Logic over the Pager
 */
class SawitDB {
    constructor(filePath, options = {}) {
        // WAL: Optional crash safety (backward compatible - disabled by default)
        this.wal = options.wal ? new WAL(filePath, options.wal) : null;
        this.dbevent = options.dbevent ? options.dbevent : new DBEventHandler();
        
        if (!this.dbevent instanceof DBEvent) {
          console.error(`dbevent is not instanceof DBEvent`);
        }

        // Recovery: Replay WAL if exists
        if (this.wal && this.wal.enabled) {
            const recovered = this.wal.recover();
            if (recovered.length > 0) {
                console.log(`[WAL] Recovered ${recovered.length} operations from crash`);
            }
        }

        this.pager = new Pager(filePath, this.wal);
        this.indexes = new Map(); // Map of 'tableName.fieldName' -> BTreeIndex
        this.parser = new QueryParser();

        // YEAR OF THE LINUX DESKTOP - just kidding. 
        // CACHE: Simple LRU for Parsed Queries
        this.queryCache = new Map();
        this.queryCacheLimit = 1000;

        // PERSISTENCE: Initialize System Tables
        this._initSystem();
    }

    _initSystem() {
        // Check if _indexes table exists, if not create it
        if (!this._findTableEntry('_indexes')) {
            try {
                this._createTable('_indexes', true); // true = system table
            } catch (e) {
                // Ignore if it effectively exists or concurrency issue
            }
        }

        // Load Indexes
        this._loadIndexes();
    }

    _loadIndexes() {
        // Re-implement load indexes to include Hints
        const indexRecords = this._select('_indexes', null);
        for (const rec of indexRecords) {
            const table = rec.table;
            const field = rec.field;
            const indexKey = `${table}.${field}`;

            if (!this.indexes.has(indexKey)) {
                const index = new BTreeIndex();
                index.name = indexKey;
                index.keyField = field;

                try {
                    // Fetch all records with Hints
                    const entry = this._findTableEntry(table);
                    if (entry) {
                        const allRecords = this._scanTable(entry, null, null, true); // true for Hints
                        for (const record of allRecords) {
                            if (record.hasOwnProperty(field)) {
                                index.insert(record[field], record);
                            }
                        }
                        this.indexes.set(indexKey, index);
                    }
                } catch (e) {
                    console.error(`Failed to rebuild index ${indexKey}: ${e.message}`);
                }
            }
        }
    }

    close() {
        if (this.wal) {
            this.wal.close();
        }
        if (this.pager) {
            this.pager.close();
            this.pager = null;
        }
    }

    /**
     * Shallow clone a command object for cache retrieval
     * Faster than JSON.parse(JSON.stringify()) for simple objects
     */
    _shallowCloneCmd(cmd) {
        const clone = { ...cmd };
        // Deep clone arrays (criteria, joins, cols, sort)
        if (cmd.criteria) clone.criteria = { ...cmd.criteria };
        if (cmd.joins) clone.joins = cmd.joins.map(j => ({ ...j, on: { ...j.on } }));
        if (cmd.cols) clone.cols = [...cmd.cols];
        if (cmd.sort) clone.sort = { ...cmd.sort };
        if (cmd.values) clone.values = { ...cmd.values };
        return clone;
    }

    query(queryString, params) {
        if (!this.pager) return "Error: Database is closed.";

        // QUERY CACHE - Optimized with shallow clone
        let cmd;
        this.queryString = queryString;
        const cacheKey = queryString;

        if (this.queryCache.has(cacheKey) && !params) {
            // Shallow clone for simple command objects (faster than JSON.parse/stringify)
            const cached = this.queryCache.get(cacheKey);
            cmd = this._shallowCloneCmd(cached);
            // Move to end for LRU behavior
            this.queryCache.delete(cacheKey);
            this.queryCache.set(cacheKey, cached);
        } else {
            // Parse without params first to get template
            const templateCmd = this.parser.parse(queryString);
            if (templateCmd.type !== 'ERROR') {
                // Cache only parameterless queries
                if (!params) {
                    this.queryCache.set(cacheKey, templateCmd);
                    // LRU eviction - remove oldest entries
                    while (this.queryCache.size > this.queryCacheLimit) {
                        const firstKey = this.queryCache.keys().next().value;
                        this.queryCache.delete(firstKey);
                    }
                }
                cmd = templateCmd;
            } else {
                return `Error: ${templateCmd.message}`;
            }

            // Bind now if params exist
            if (params) {
                this.parser._bindParameters(cmd, params);
            }
        }

        // Re-check error type just in case
        if (cmd.type === 'ERROR') return `Error: ${cmd.message}`;
        // const cmd = this.parser.parse(queryString, params);

        try {
            switch (cmd.type) {
                case 'CREATE_TABLE':
                    return this._createTable(cmd.table);

                case 'SHOW_TABLES':
                    return this._showTables();

                case 'SHOW_INDEXES':
                    return this._showIndexes(cmd.table); // cmd.table can be null

                case 'INSERT':
                    return this._insert(cmd.table, cmd.data);

                case 'SELECT': {
                    // Map generic generic Select Logic
                    // Note: Pass distinct=false here, we apply DISTINCT after projection
                    let rows = this._select(cmd.table, cmd.criteria, cmd.sort, cmd.limit, cmd.offset, cmd.joins, false);

                    // Column projection
                    if (!(cmd.cols.length === 1 && cmd.cols[0] === '*')) {
                        rows = rows.map(r => {
                            const newRow = {};
                            cmd.cols.forEach(c => newRow[c] = r[c] !== undefined ? r[c] : null);
                            return newRow;
                        });
                    }

                    // Apply DISTINCT after projection for correct behavior
                    if (cmd.distinct) {
                        const seen = new Set();
                        rows = rows.filter(row => {
                            const key = JSON.stringify(row);
                            if (seen.has(key)) return false;
                            seen.add(key);
                            return true;
                        });
                    }

                    return rows;
                }

                case 'DELETE':
                    return this._delete(cmd.table, cmd.criteria);

                case 'UPDATE':
                    return this._update(cmd.table, cmd.updates, cmd.criteria);

                case 'DROP_TABLE':
                    return this._dropTable(cmd.table);

                case 'CREATE_INDEX':
                    return this._createIndex(cmd.table, cmd.field);

                case 'AGGREGATE':
                    return this._aggregate(cmd.table, cmd.func, cmd.field, cmd.criteria, cmd.groupBy, cmd.having);

                case 'EXPLAIN':
                    return this._explain(cmd.innerCommand);

                default:
                    return `Perintah tidak dikenal atau belum diimplementasikan di Engine Refactor.`;
            }
        } catch (e) {
            return `Error: ${e.message}`;
        }
    }

    // --- Core Logic ---

    _findTableEntry(name) {
        const p0 = this.pager.readPage(0);
        const numTables = p0.readUInt32LE(8);
        let offset = 12;

        for (let i = 0; i < numTables; i++) {
            const tName = p0.toString('utf8', offset, offset + 32).replace(/\0/g, '');
            if (tName === name) {
                return {
                    index: i,
                    offset: offset,
                    startPage: p0.readUInt32LE(offset + 32),
                    lastPage: p0.readUInt32LE(offset + 36)
                };
            }
            offset += 40;
        }
        return null;
    }

    _showTables() {
        const p0 = this.pager.readPage(0);
        const numTables = p0.readUInt32LE(8);
        const tables = [];
        let offset = 12;
        for (let i = 0; i < numTables; i++) {
            const tName = p0.toString('utf8', offset, offset + 32).replace(/\0/g, '');
            if (!tName.startsWith('_')) { // Hide system tables
                tables.push(tName);
            }
            offset += 40;
        }
        return tables;
    }

    /**
     * Validate table/column name to prevent injection and ensure safe storage
     * @param {string} name - Name to validate
     * @param {string} type - 'table' or 'column' for error messages
     * @param {boolean} allowSystem - Allow system table names (internal use only)
     */
    _validateName(name, type = 'table', allowSystem = false) {
        if (!name || typeof name !== 'string') {
            throw new Error(`${type} name tidak boleh kosong`);
        }
        if (name.length > 32) {
            throw new Error(`${type} name max 32 karakter`);
        }
        // Only allow alphanumeric, underscore, and starting with letter or underscore
        if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
            throw new Error(`${type} name hanya boleh huruf, angka, underscore, dan harus dimulai dengan huruf atau underscore`);
        }
        // Disallow reserved names for user tables (allow for internal system use)
        if (!allowSystem && type === 'table') {
            const reserved = ['_indexes', '_system', '_schema', 'null', 'true', 'false'];
            if (reserved.includes(name.toLowerCase())) {
                throw new Error(`${type} name '${name}' adalah nama terproteksi`);
            }
        }
        return true;
    }

    _createTable(name, isSystemTable = false) {
        this._validateName(name, 'table', isSystemTable);
        if (this._findTableEntry(name)) return `Kebun '${name}' sudah ada.`;

        const p0 = this.pager.readPage(0);
        const numTables = p0.readUInt32LE(8);
        let offset = 12 + (numTables * 40);
        if (offset + 40 > Pager.PAGE_SIZE) throw new Error("Lahan penuh (Page 0 full)");

        const newPageId = this.pager.allocPage();

        const nameBuf = Buffer.alloc(32);
        nameBuf.write(name);
        nameBuf.copy(p0, offset);

        p0.writeUInt32LE(newPageId, offset + 32);
        p0.writeUInt32LE(newPageId, offset + 36);
        p0.writeUInt32LE(numTables + 1, 8);

        this.pager.writePage(0, p0);
        this.dbevent.OnTableCreated(name, this._findTableEntry(name),this.queryString);
        return `Kebun '${name}' telah dibuka.`;
    }

    _dropTable(name) {
        if (name === '_indexes') return "Tidak boleh membakar catatan sistem.";

        const entry = this._findTableEntry(name);
        if (!entry) return `Kebun '${name}' tidak ditemukan.`;

        // Remove associated indexes
        const toRemove = [];
        for (const key of this.indexes.keys()) {
            if (key.startsWith(name + '.')) {
                toRemove.push(key);
            }
        }

        // Remove from memory
        toRemove.forEach(key => this.indexes.delete(key));

        // Remove from _indexes table
        try {
            this._delete('_indexes', {
                type: 'compound',
                logic: 'AND',
                conditions: [
                    { key: 'table', op: '=', val: name }
                ]
            });
        } catch (e) { /* Ignore if fails, maybe recursive? No, _delete uses basic ops */ }


        const p0 = this.pager.readPage(0);
        const numTables = p0.readUInt32LE(8);

        // Move last entry to this spot to fill gap
        if (numTables > 1 && entry.index < numTables - 1) {
            const lastOffset = 12 + ((numTables - 1) * 40);
            const lastEntryBuf = p0.slice(lastOffset, lastOffset + 40);
            lastEntryBuf.copy(p0, entry.offset);
        }

        // Clear last spot
        const lastOffset = 12 + ((numTables - 1) * 40);
        p0.fill(0, lastOffset, lastOffset + 40);

        p0.writeUInt32LE(numTables - 1, 8);
        this.pager.writePage(0, p0);
        this.dbevent.OnTableDropped(name, entry, this.queryString);
        return `Kebun '${name}' telah dibakar (Drop).`;
    }

    _updateTableLastPage(name, newLastPageId) {
        const entry = this._findTableEntry(name);
        if (!entry) throw new Error("Internal Error: Table missing for update");

        // Update Disk/Page 0
        const p0 = this.pager.readPage(0);
        p0.writeUInt32LE(newLastPageId, entry.offset + 36);
        this.pager.writePage(0, p0);
    }

    _insert(table, data) {
        if (!data || Object.keys(data).length === 0) {
            throw new Error("Data kosong / fiktif? Ini melanggar integritas (Korupsi Data).");
        }
        return this._insertMany(table, [data]);
    }

    // NEW: Batch Insert for High Performance (50k+ TPS)
    _insertMany(table, dataArray) {
        if (!dataArray || dataArray.length === 0) return "Tidak ada bibit untuk ditanam.";

        const entry = this._findTableEntry(table);
        if (!entry) throw new Error(`Kebun '${table}' tidak ditemukan.`);

        let currentPageId = entry.lastPage;
        let pData = this.pager.readPage(currentPageId);
        let freeOffset = pData.readUInt16LE(6);
        let count = pData.readUInt16LE(4);
        let startPageChanged = false;

        for (const data of dataArray) {
            const dataStr = JSON.stringify(data);
            const dataBuf = Buffer.from(dataStr, 'utf8');
            const recordLen = dataBuf.length;
            const totalLen = 2 + recordLen;

            // Check if fits
            if (freeOffset + totalLen > Pager.PAGE_SIZE) {
                // Determine new page ID (predictive or alloc)
                // allocPage reads/writes Page 0, which is expensive in loop.
                // Optimally we should batch Page 0 update too, but Pager.allocPage handles it.
                // For now rely on Pager caching Page 0.

                // Write current full page
                pData.writeUInt16LE(count, 4);
                pData.writeUInt16LE(freeOffset, 6);
                this.pager.writePage(currentPageId, pData);

                const newPageId = this.pager.allocPage();

                // Link old page to new
                pData.writeUInt32LE(newPageId, 0);
                this.pager.writePage(currentPageId, pData); // Rewrite link

                currentPageId = newPageId;
                pData = this.pager.readPage(currentPageId);
                freeOffset = pData.readUInt16LE(6);
                count = pData.readUInt16LE(4);
                startPageChanged = true;
            }


            pData.writeUInt16LE(recordLen, freeOffset);
            dataBuf.copy(pData, freeOffset + 2);
            freeOffset += totalLen;
            count++;

            // Inject Page Hint for Index
            Object.defineProperty(data, '_pageId', {
                value: currentPageId,
                enumerable: false,
                writable: true
            });

            // Index update (can be batched later if needed, but BTree is fast)
            if (table !== '_indexes') {
                this._updateIndexes(table, data, null);
            }
        }

        // Final write
        pData.writeUInt16LE(count, 4);
        pData.writeUInt16LE(freeOffset, 6);
        this.pager.writePage(currentPageId, pData);

        if (startPageChanged) {
            this._updateTableLastPage(table, currentPageId);
        }
        
        this.dbevent.OnTableInserted(table,dataArray,this.queryString);
        return `${dataArray.length} bibit tertanam.`;
    }

    _updateIndexes(table, newObj, oldObj) {
        // If oldObj is null, it's an INSERT. If newObj is null, it's a DELETE. Both? Update.

        for (const [indexKey, index] of this.indexes) {
            const [tbl, field] = indexKey.split('.');
            if (tbl !== table) continue; // Wrong table

            // 1. Remove old value from index (if exists and changed)
            if (oldObj && oldObj.hasOwnProperty(field)) {
                // Only remove if value changed OR it's a delete (newObj is null)
                // If update, check if value diff
                if (!newObj || newObj[field] !== oldObj[field]) {
                    index.delete(oldObj[field]);
                }
            }

            // 2. Insert new value (if exists)
            if (newObj && newObj.hasOwnProperty(field)) {
                // Only insert if it's new OR value changed
                if (!oldObj || newObj[field] !== oldObj[field]) {
                    index.insert(newObj[field], newObj);
                }
            }
        }
    }

    _checkMatch(obj, criteria) {
        if (!criteria) return true;

        if (criteria.type === 'compound') {
            let result = (criteria.logic === 'OR') ? false : true;

            for (let i = 0; i < criteria.conditions.length; i++) {
                const cond = criteria.conditions[i];
                const matches = (cond.type === 'compound')
                    ? this._checkMatch(obj, cond)
                    : this._checkSingleCondition(obj, cond);

                if (criteria.logic === 'OR') {
                    result = result || matches;
                    if (result) return true; // Short circuit
                } else {
                    result = result && matches;
                    if (!result) return false; // Short circuit
                }
            }
            return result;
        }

        return this._checkSingleCondition(obj, criteria);
    }

    _checkSingleCondition(obj, criteria) {
        const val = obj[criteria.key];
        const target = criteria.val;
        switch (criteria.op) {
            // Use strict equality with type-aware comparison
            case '=':
                // Handle numeric comparison (allows "5" === 5 scenario)
                if (typeof val === 'number' || typeof target === 'number') {
                    return Number(val) === Number(target);
                }
                return val === target;
            case '!=':
                if (typeof val === 'number' || typeof target === 'number') {
                    return Number(val) !== Number(target);
                }
                return val !== target;
            case '>': return val > target;
            case '<': return val < target;
            case '>=': return val >= target;
            case '<=': return val <= target;
            case 'IN': return Array.isArray(target) && target.includes(val);
            case 'NOT IN': return Array.isArray(target) && !target.includes(val);
            case 'LIKE': {
                // Escape regex metacharacters except % and _ which are SQL wildcards
                const escaped = target.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
                const regexStr = '^' + escaped.replace(/%/g, '.*').replace(/_/g, '.') + '$';
                const re = new RegExp(regexStr, 'i');
                return re.test(String(val));
            }
            case 'BETWEEN':
                return val >= target[0] && val <= target[1];
            case 'IS NULL':
                return val === null || val === undefined;
            case 'IS NOT NULL':
                return val !== null && val !== undefined;
            default: return false;
        }
    }

    _select(table, criteria, sort, limit, offsetCount, joins) {
        const entry = this._findTableEntry(table);
        if (!entry) throw new Error(`Kebun '${table}' tidak ditemukan.`);

        let results = [];

        if (joins && joins.length > 0) {
            // 1. Scan Main Table with prefixed columns
            let currentRows = this._scanTable(entry, null).map(row => {
                const newRow = { ...row };
                for (const k in row) {
                    newRow[`${table}.${k}`] = row[k];
                }
                return newRow;
            });

            // 2. Perform Joins (supports INNER, LEFT, RIGHT, FULL, CROSS)
            for (const join of joins) {
                const joinEntry = this._findTableEntry(join.table);
                if (!joinEntry) throw new Error(`Kebun '${join.table}' tidak ditemukan.`);

                const joinType = join.type || 'INNER';
                const joinRows = this._scanTable(joinEntry, null);

                // Prefix right table rows
                const prefixRightRow = (row) => {
                    const prefixed = {};
                    for (const k in row) {
                        prefixed[k] = row[k];
                        prefixed[`${join.table}.${k}`] = row[k];
                    }
                    return prefixed;
                };

                // Create null row for outer joins
                const createNullRightRow = () => {
                    const nullRow = {};
                    if (joinRows.length > 0) {
                        for (const k in joinRows[0]) {
                            nullRow[k] = null;
                            nullRow[`${join.table}.${k}`] = null;
                        }
                    }
                    return nullRow;
                };

                const createNullLeftRow = () => {
                    const nullRow = {};
                    if (currentRows.length > 0) {
                        for (const k in currentRows[0]) {
                            nullRow[k] = null;
                        }
                    }
                    return nullRow;
                };

                const nextRows = [];

                // CROSS JOIN - Cartesian product (no ON clause)
                if (joinType === 'CROSS') {
                    for (const leftRow of currentRows) {
                        for (const rightRow of joinRows) {
                            nextRows.push({ ...leftRow, ...prefixRightRow(rightRow) });
                        }
                    }
                    currentRows = nextRows;
                    continue;
                }

                // For other joins, we need the ON condition
                const matchRows = (leftRow, rightRow) => {
                    const lVal = leftRow[join.on.left];
                    const rKey = join.on.right.startsWith(join.table + '.')
                        ? join.on.right.substring(join.table.length + 1)
                        : join.on.right;
                    const rVal = rightRow[rKey];

                    switch (join.on.op) {
                        case '=': return lVal == rVal;
                        case '!=': case '<>': return lVal != rVal;
                        case '>': return lVal > rVal;
                        case '<': return lVal < rVal;
                        case '>=': return lVal >= rVal;
                        case '<=': return lVal <= rVal;
                        default: return false;
                    }
                };

                // Build hash map for equi-joins (optimization)
                const useHashJoin = join.on.op === '=';
                let joinMap = null;

                if (useHashJoin) {
                    joinMap = new Map();
                    for (const row of joinRows) {
                        let rightKey = join.on.right;
                        if (rightKey.startsWith(join.table + '.')) {
                            rightKey = rightKey.substring(join.table.length + 1);
                        }
                        const val = row[rightKey];
                        if (val === undefined || val === null) continue;
                        if (!joinMap.has(val)) joinMap.set(val, []);
                        joinMap.get(val).push(row);
                    }
                }

                // Track matched right rows for FULL OUTER JOIN
                const matchedRightRows = new Set();

                // Process LEFT/INNER/FULL joins
                if (joinType === 'INNER' || joinType === 'LEFT' || joinType === 'FULL') {
                    for (const leftRow of currentRows) {
                        let hasMatch = false;

                        if (useHashJoin) {
                            const lVal = leftRow[join.on.left];
                            if (joinMap.has(lVal)) {
                                const matches = joinMap.get(lVal);
                                for (let ri = 0; ri < matches.length; ri++) {
                                    const rightRow = matches[ri];
                                    nextRows.push({ ...leftRow, ...prefixRightRow(rightRow) });
                                    hasMatch = true;
                                    if (joinType === 'FULL') {
                                        // Track by index in original joinRows
                                        const origIdx = joinRows.indexOf(rightRow);
                                        if (origIdx !== -1) matchedRightRows.add(origIdx);
                                    }
                                }
                            }
                        } else {
                            for (let ri = 0; ri < joinRows.length; ri++) {
                                const rightRow = joinRows[ri];
                                if (matchRows(leftRow, rightRow)) {
                                    nextRows.push({ ...leftRow, ...prefixRightRow(rightRow) });
                                    hasMatch = true;
                                    if (joinType === 'FULL') matchedRightRows.add(ri);
                                }
                            }
                        }

                        // LEFT or FULL: include unmatched left rows with NULL right
                        if (!hasMatch && (joinType === 'LEFT' || joinType === 'FULL')) {
                            nextRows.push({ ...leftRow, ...createNullRightRow() });
                        }
                    }
                }

                // RIGHT JOIN: swap logic - iterate right rows, find matching left
                if (joinType === 'RIGHT') {
                    const leftMap = useHashJoin ? new Map() : null;
                    if (useHashJoin) {
                        for (const row of currentRows) {
                            const val = row[join.on.left];
                            if (val === undefined || val === null) continue;
                            if (!leftMap.has(val)) leftMap.set(val, []);
                            leftMap.get(val).push(row);
                        }
                    }

                    for (const rightRow of joinRows) {
                        let hasMatch = false;
                        const prefixedRight = prefixRightRow(rightRow);

                        if (useHashJoin) {
                            let rightKey = join.on.right;
                            if (rightKey.startsWith(join.table + '.')) {
                                rightKey = rightKey.substring(join.table.length + 1);
                            }
                            const rVal = rightRow[rightKey];
                            if (leftMap.has(rVal)) {
                                const matches = leftMap.get(rVal);
                                for (const leftRow of matches) {
                                    nextRows.push({ ...leftRow, ...prefixedRight });
                                    hasMatch = true;
                                }
                            }
                        } else {
                            for (const leftRow of currentRows) {
                                if (matchRows(leftRow, rightRow)) {
                                    nextRows.push({ ...leftRow, ...prefixedRight });
                                    hasMatch = true;
                                }
                            }
                        }

                        // RIGHT: include unmatched right rows with NULL left
                        if (!hasMatch) {
                            nextRows.push({ ...createNullLeftRow(), ...prefixedRight });
                        }
                    }
                }

                // FULL OUTER: add unmatched right rows
                if (joinType === 'FULL') {
                    for (let ri = 0; ri < joinRows.length; ri++) {
                        if (!matchedRightRows.has(ri)) {
                            nextRows.push({ ...createNullLeftRow(), ...prefixRightRow(joinRows[ri]) });
                        }
                    }
                }

                currentRows = nextRows;
            }
            results = currentRows;

            if (criteria) {
                results = results.filter(r => this._checkMatch(r, criteria));
            }

        } else {
            // OPTIMIZATION: Use index for = queries
            if (criteria && criteria.op === '=' && !sort) {
                const indexKey = `${table}.${criteria.key}`;
                if (this.indexes.has(indexKey)) {
                    const index = this.indexes.get(indexKey);
                    results = index.search(criteria.val);
                } else {
                    // If sorting, we cannot limit the scan early
                    const scanLimit = sort ? null : limit;
                    results = this._scanTable(entry, criteria, scanLimit);
                }
            } else {
                // If sorting, we cannot limit the scan early
                const scanLimit = sort ? null : limit;
                results = this._scanTable(entry, criteria, scanLimit);
            }
        }

        // Note: DISTINCT is applied after column projection in query() method

        // Sorting
        if (sort) {
            results.sort((a, b) => {
                const valA = a[sort.key];
                const valB = b[sort.key];
                if (valA < valB) return sort.dir === 'asc' ? -1 : 1;
                if (valA > valB) return sort.dir === 'asc' ? 1 : -1;
                return 0;
            });
        }

        // Limit & Offset
        let start = 0;
        let end = results.length;

        if (offsetCount) start = offsetCount;
        if (limit) end = start + limit;
        if (end > results.length) end = results.length;
        if (start > results.length) start = results.length;
        results = results.slice(start, end);
        this.dbevent.OnTableSelected(table,results,this.queryString);
        return results;
    }

    // Modifiy _scanTable to allow returning extended info (pageId) for internal use
    // Modifiy _scanTable to allow returning extended info (pageId) for internal use
    _scanTable(entry, criteria, limit = null, returnRaw = false) {
        let currentPageId = entry.startPage;
        const results = [];
        const effectiveLimit = limit || Infinity;

        // OPTIMIZATION: Pre-compute condition check for hot path
        const hasSimpleCriteria = criteria && !criteria.type && criteria.key && criteria.op;
        const criteriaKey = hasSimpleCriteria ? criteria.key : null;
        const criteriaOp = hasSimpleCriteria ? criteria.op : null;
        const criteriaVal = hasSimpleCriteria ? criteria.val : null;

        while (currentPageId !== 0 && results.length < effectiveLimit) {
            // NEW: Use Object Cache
            // Returns { next: uint32, items: Array<Object> }
            const pageData = this.pager.readPageObjects(currentPageId);

            for (const obj of pageData.items) {
                if (results.length >= effectiveLimit) break;

                // OPTIMIZATION: Inline simple condition check (hot path)
                let matches = true;
                if (hasSimpleCriteria) {
                    const val = obj[criteriaKey];
                    switch (criteriaOp) {
                        case '=': matches = (val == criteriaVal); break;
                        case '>': matches = (val > criteriaVal); break;
                        case '<': matches = (val < criteriaVal); break;
                        case '>=': matches = (val >= criteriaVal); break;
                        case '<=': matches = (val <= criteriaVal); break;
                        case '!=': matches = (val != criteriaVal); break;
                        case 'LIKE':
                            const pattern = criteriaVal.replace(/%/g, '.*').replace(/_/g, '.');
                            matches = new RegExp('^' + pattern + '$', 'i').test(val);
                            break;
                        default: matches = this._checkMatch(obj, criteria);
                    }
                } else if (criteria) {
                    matches = this._checkMatch(obj, criteria);
                }

                if (matches) {
                    if (returnRaw) {
                        // Inject Page Hint
                        // Safe to modify cached object (it's non-enumerable)
                        Object.defineProperty(obj, '_pageId', {
                            value: currentPageId,
                            enumerable: false, // Hidden
                            writable: true
                        });
                        results.push(obj);
                    } else {
                        results.push(obj);
                    }
                }
            }

            currentPageId = pageData.next;
        }
        return results;
    }

    _delete(table, criteria, forceFullScan = false) {
        const entry = this._findTableEntry(table);
        if (!entry) throw new Error(`Kebun '${table}' tidak ditemukan.`);

        // OPTIMIZATION: Check Index Hint for simple equality delete
        let hintPageId = -1;
        if (!forceFullScan && criteria && criteria.op === '=' && criteria.key) {
            const indexKey = `${table}.${criteria.key}`;
            if (this.indexes.has(indexKey)) {
                const index = this.indexes.get(indexKey);
                const searchRes = index.search(criteria.val);
                if (searchRes.length > 0 && searchRes[0]._pageId !== undefined) {
                    // Use the hint! Scan ONLY this page
                    hintPageId = searchRes[0]._pageId;
                    // Note: If multiple results, we might need to check multiple pages.
                    // For now simple single record optimization.
                }
            }
        }

        let currentPageId = (hintPageId !== -1) ? hintPageId : entry.startPage;
        let deletedCount = 0;
        let deletedData=[];
        // Loop: If hint used, only loop once (unless next page logic needed, but pageId is specific)
        // We modify the while condition

        while (currentPageId !== 0) {
            let pData = this.pager.readPage(currentPageId);
            const count = pData.readUInt16LE(4);
            let offset = 8;
            const recordsToKeep = [];
            let pageModified = false;

            for (let i = 0; i < count; i++) {
                const len = pData.readUInt16LE(offset);
                const jsonStr = pData.toString('utf8', offset + 2, offset + 2 + len);
                let shouldDelete = false;
                let parsedObj = null;

                try {
                    parsedObj = JSON.parse(jsonStr);
                    if (this._checkMatch(parsedObj, criteria)) shouldDelete = true;
                } catch (e) {
                    // Skip malformed JSON records
                }

                if (shouldDelete) {
                    deletedCount++;
                    // Remove from Index if needed - reuse already parsed object
                    if (table !== '_indexes' && parsedObj) {
                        this._removeFromIndexes(table, parsedObj);
                    }
                    deletedData.push(parsedObj)
                    pageModified = true;
                } else {
                    recordsToKeep.push({ len, data: pData.slice(offset + 2, offset + 2 + len) });
                }
                offset += 2 + len;
            }

            if (pageModified) {
                let writeOffset = 8;
                pData.writeUInt16LE(recordsToKeep.length, 4);

                for (let rec of recordsToKeep) {
                    pData.writeUInt16LE(rec.len, writeOffset);
                    rec.data.copy(pData, writeOffset + 2);
                    writeOffset += 2 + rec.len;
                }
                pData.writeUInt16LE(writeOffset, 6); // New free offset
                pData.fill(0, writeOffset); // Zero out rest

                this.pager.writePage(currentPageId, pData);
            }

            // Next page logic
            if (hintPageId !== -1) {
                break; // Optimized single page scan done
            }
            currentPageId = pData.readUInt32LE(0);
        }

        // If hint failed (record moved?), fallback to full scan? 
        // For now assume hint is good. If record moved, it's effectively deleted from old page already (during move).
        // If we missed it, it means inconsistency. But with this engine, move only happens on Update overflow.

        if (hintPageId !== -1 && deletedCount === 0) {
            // Hint failed (maybe race condition or stale index?), fallback to full scan
            // This ensures safety by re-calling _delete with forceFullScan=true
            return this._delete(table, criteria, true);
        }
        
        this.dbevent.OnTableInserted(table,deletedData,this.queryString);
        return `Berhasil menggusur ${deletedCount} bibit.`;
    }

    _removeFromIndexes(table, data) {
        for (const [indexKey, index] of this.indexes) {
            const [tbl, field] = indexKey.split('.');
            if (tbl === table && data.hasOwnProperty(field)) {
                index.delete(data[field]); // Basic deletion from B-Tree
            }
        }
    }

    _update(table, updates, criteria) {
        const entry = this._findTableEntry(table);
        if (!entry) throw new Error(`Kebun '${table}' tidak ditemukan.`);

        // OPTIMIZATION: Check Index Hint for simple equality update
        let hintPageId = -1;
        if (criteria && criteria.op === '=' && criteria.key) {
            const indexKey = `${table}.${criteria.key}`;
            if (this.indexes.has(indexKey)) {
                const index = this.indexes.get(indexKey);
                const searchRes = index.search(criteria.val);
                if (searchRes.length > 0 && searchRes[0]._pageId !== undefined) {
                    hintPageId = searchRes[0]._pageId;
                }
            }
        }

        let currentPageId = (hintPageId !== -1) ? hintPageId : entry.startPage;
        let updatedCount = 0;
        let updatedData=[]

        // OPTIMIZATION: In-place update instead of DELETE+INSERT
        while (currentPageId !== 0) {
            let pData = this.pager.readPage(currentPageId);
            const count = pData.readUInt16LE(4);
            let offset = 8;
            let modified = false;

            for (let i = 0; i < count; i++) {
                const len = pData.readUInt16LE(offset);
                const jsonStr = pData.toString('utf8', offset + 2, offset + 2 + len);

                try {
                    const obj = JSON.parse(jsonStr);

                    if (this._checkMatch(obj, criteria)) {
                        // Store original values for index update (shallow copy)
                        const originalObj = { ...obj };

                        // Apply updates
                        for (const k in updates) {
                            obj[k] = updates[k];
                        }

                        // Update index if needed
                        // Inject _pageId hint so the index knows where this record lives
                        Object.defineProperty(obj, '_pageId', {
                            value: currentPageId,
                            enumerable: false,
                            writable: true
                        });

                        // Use original object instead of re-parsing JSON
                        this._updateIndexes(table, originalObj, obj);

                        // Serialize updated object
                        const newJsonStr = JSON.stringify(obj);
                        const newLen = Buffer.byteLength(newJsonStr, 'utf8');

                        // Check if it fits in same space
                        if (newLen <= len) {
                            // In-place update
                            pData.writeUInt16LE(newLen, offset);
                            pData.write(newJsonStr, offset + 2, newLen, 'utf8');
                            // Zero out remaining space
                            if (newLen < len) {
                                pData.fill(0, offset + 2 + newLen, offset + 2 + len);
                            }
                            modified = true;
                            updatedCount++;
                        } else {
                            // Fallback: DELETE + INSERT (rare case)
                            this._delete(table, criteria);
                            this._insert(table, obj);
                            updatedCount++;
                            break; // Exit loop as page structure changed
                        }
                        
                        updatedData.push(obj)
                    }
                } catch (err) {
                    // Skip malformed JSON records
                }

                offset += 2 + len;
            }

            if (modified) {
                this.pager.writePage(currentPageId, pData);
            }

            if (hintPageId !== -1) break; // Scan only one page

            currentPageId = pData.readUInt32LE(0);
        }

        if (hintPageId !== -1 && updatedCount === 0) {
            // Hint failed, fallback (not implemented fully for update, assume safe)
            // But to be safe, restart scan? For now let's hope hint works.
            // TODO: Fallback to full scan logic if mission critical.
        }
        this.dbevent.OnTableUpdated(table,updatedData,this.queryString);
        return `Berhasil memupuk ${updatedCount} bibit.`;
    }

    _createIndex(table, field) {
        const entry = this._findTableEntry(table);
        if (!entry) throw new Error(`Kebun '${table}' tidak ditemukan.`);

        const indexKey = `${table}.${field}`;
        if (this.indexes.has(indexKey)) {
            return `Indeks pada '${table}.${field}' sudah ada.`;
        }

        // Create index
        const index = new BTreeIndex();
        index.name = indexKey;
        index.keyField = field;

        // Build index from existing data
        const allRecords = this._select(table, null);
        for (const record of allRecords) {
            if (record.hasOwnProperty(field)) {
                index.insert(record[field], record);
            }
        }

        this.indexes.set(indexKey, index);

        // PERSISTENCE: Save to _indexes table
        try {
            this._insert('_indexes', { table, field });
        } catch (e) {
            console.error("Failed to persist index definition", e);
        }

        return `Indeks dibuat pada '${table}.${field}' (${allRecords.length} records indexed)`;
    }

    _showIndexes(table) {
        if (table) {
            const indexes = [];
            for (const [key, index] of this.indexes) {
                if (key.startsWith(table + '.')) {
                    indexes.push(index.stats());
                }
            }
            return indexes.length > 0 ? indexes : `Tidak ada indeks pada '${table}'`;
        } else {
            const allIndexes = [];
            for (const index of this.indexes.values()) {
                allIndexes.push(index.stats());
            }
            return allIndexes;
        }
    }

    _aggregate(table, func, field, criteria, groupBy, having) {
        const records = this._select(table, criteria);

        if (groupBy) {
            return this._groupedAggregate(records, func, field, groupBy, having);
        }

        switch (func.toUpperCase()) {
            case 'COUNT':
                return { count: records.length };

            case 'SUM':
                if (!field) throw new Error("SUM requires a field");
                const sum = records.reduce((acc, r) => acc + (Number(r[field]) || 0), 0);
                return { sum, field };

            case 'AVG': {
                if (!field) throw new Error("AVG requires a field");
                if (records.length === 0) {
                    return { avg: null, field, count: 0 };
                }
                const avgSum = records.reduce((acc, r) => acc + (Number(r[field]) || 0), 0);
                const avg = avgSum / records.length;
                return { avg, field, count: records.length };
            }

            case 'MIN': {
                if (!field) throw new Error("MIN requires a field");
                if (records.length === 0) {
                    return { min: null, field };
                }
                // Use loop instead of spread to avoid stack overflow on large datasets
                let min = Infinity;
                for (const r of records) {
                    const val = Number(r[field]);
                    if (!isNaN(val) && val < min) min = val;
                }
                return { min: min === Infinity ? null : min, field };
            }

            case 'MAX': {
                if (!field) throw new Error("MAX requires a field");
                if (records.length === 0) {
                    return { max: null, field };
                }
                // Use loop instead of spread to avoid stack overflow on large datasets
                let max = -Infinity;
                for (const r of records) {
                    const val = Number(r[field]);
                    if (!isNaN(val) && val > max) max = val;
                }
                return { max: max === -Infinity ? null : max, field };
            }

            default:
                throw new Error(`Unknown aggregate function: ${func}`);
        }
    }

    _groupedAggregate(records, func, field, groupBy, having) {
        const groups = {};
        for (const record of records) {
            const key = record[groupBy];
            if (!groups[key]) groups[key] = [];
            groups[key].push(record);
        }

        const results = [];
        for (const [key, groupRecords] of Object.entries(groups)) {
            const result = { [groupBy]: key };
            switch (func.toUpperCase()) {
                case 'COUNT':
                    result.count = groupRecords.length;
                    break;
                case 'SUM':
                    result.sum = groupRecords.reduce((acc, r) => acc + (Number(r[field]) || 0), 0);
                    break;
                case 'AVG':
                    if (groupRecords.length === 0) {
                        result.avg = null;
                    } else {
                        result.avg = groupRecords.reduce((acc, r) => acc + (Number(r[field]) || 0), 0) / groupRecords.length;
                    }
                    break;
                case 'MIN': {
                    let min = Infinity;
                    for (const r of groupRecords) {
                        const val = Number(r[field]);
                        if (!isNaN(val) && val < min) min = val;
                    }
                    result.min = min === Infinity ? null : min;
                    break;
                }
                case 'MAX': {
                    let max = -Infinity;
                    for (const r of groupRecords) {
                        const val = Number(r[field]);
                        if (!isNaN(val) && val > max) max = val;
                    }
                    result.max = max === -Infinity ? null : max;
                    break;
                }
            }
            results.push(result);
        }

        // Apply HAVING filter on aggregated results
        if (having) {
            return results.filter(row => {
                const val = row[having.field];
                const target = having.val;
                switch (having.op) {
                    case '=': return val === target;
                    case '!=': case '<>': return val !== target;
                    case '>': return val > target;
                    case '<': return val < target;
                    case '>=': return val >= target;
                    case '<=': return val <= target;
                    default: return true;
                }
            });
        }

        return results;
    }

    /**
     * EXPLAIN - Analyze query execution plan
     * Returns information about how the query would be executed
     */
    _explain(cmd) {
        const plan = {
            type: cmd.type,
            table: cmd.table,
            steps: []
        };

        switch (cmd.type) {
            case 'SELECT': {
                const entry = this._findTableEntry(cmd.table);
                if (!entry) {
                    plan.error = `Table '${cmd.table}' not found`;
                    return plan;
                }

                // Check if joins are used
                if (cmd.joins && cmd.joins.length > 0) {
                    plan.steps.push({
                        operation: 'SCAN',
                        table: cmd.table,
                        method: 'Full Table Scan',
                        reason: 'Base table for JOIN'
                    });

                    for (const join of cmd.joins) {
                        const joinType = join.type || 'INNER';
                        const useHashJoin = join.on && join.on.op === '=';
                        plan.steps.push({
                            operation: `${joinType} JOIN`,
                            table: join.table,
                            method: useHashJoin ? 'Hash Join' : 'Nested Loop Join',
                            condition: join.on ? `${join.on.left} ${join.on.op} ${join.on.right}` : 'CROSS'
                        });
                    }
                } else if (cmd.criteria) {
                    // Check index usage
                    const indexKey = `${cmd.table}.${cmd.criteria.key}`;
                    const hasIndex = this.indexes.has(indexKey);

                    if (hasIndex && cmd.criteria.op === '=') {
                        plan.steps.push({
                            operation: 'INDEX SCAN',
                            table: cmd.table,
                            index: indexKey,
                            method: 'B-Tree Index Lookup',
                            condition: `${cmd.criteria.key} ${cmd.criteria.op} ${JSON.stringify(cmd.criteria.val)}`
                        });
                    } else {
                        plan.steps.push({
                            operation: 'TABLE SCAN',
                            table: cmd.table,
                            method: hasIndex ? 'Full Scan (index not usable for this operator)' : 'Full Table Scan',
                            condition: `${cmd.criteria.key} ${cmd.criteria.op} ${JSON.stringify(cmd.criteria.val)}`
                        });
                    }
                } else {
                    plan.steps.push({
                        operation: 'TABLE SCAN',
                        table: cmd.table,
                        method: 'Full Table Scan',
                        reason: 'No WHERE clause'
                    });
                }

                // DISTINCT step
                if (cmd.distinct) {
                    plan.steps.push({
                        operation: 'DISTINCT',
                        method: 'Hash-based deduplication'
                    });
                }

                // Sorting step
                if (cmd.sort) {
                    plan.steps.push({
                        operation: 'SORT',
                        field: cmd.sort.by,
                        direction: cmd.sort.order || 'ASC'
                    });
                }

                // Limit/Offset step
                if (cmd.limit || cmd.offset) {
                    plan.steps.push({
                        operation: 'LIMIT/OFFSET',
                        limit: cmd.limit || 'none',
                        offset: cmd.offset || 0
                    });
                }

                // Projection step
                if (cmd.cols && !(cmd.cols.length === 1 && cmd.cols[0] === '*')) {
                    plan.steps.push({
                        operation: 'PROJECT',
                        columns: cmd.cols
                    });
                }
                break;
            }

            case 'DELETE':
            case 'UPDATE': {
                const entry = this._findTableEntry(cmd.table);
                if (!entry) {
                    plan.error = `Table '${cmd.table}' not found`;
                    return plan;
                }

                if (cmd.criteria) {
                    const indexKey = `${cmd.table}.${cmd.criteria.key}`;
                    const hasIndex = this.indexes.has(indexKey);

                    plan.steps.push({
                        operation: 'SCAN',
                        table: cmd.table,
                        method: hasIndex && cmd.criteria.op === '=' ? 'Index-assisted scan' : 'Full Table Scan',
                        condition: `${cmd.criteria.key} ${cmd.criteria.op} ${JSON.stringify(cmd.criteria.val)}`
                    });
                } else {
                    plan.steps.push({
                        operation: 'SCAN',
                        table: cmd.table,
                        method: 'Full Table Scan',
                        reason: 'No WHERE clause - affects all rows'
                    });
                }

                plan.steps.push({
                    operation: cmd.type,
                    table: cmd.table,
                    method: 'In-place modification'
                });
                break;
            }

            case 'AGGREGATE': {
                plan.steps.push({
                    operation: 'SCAN',
                    table: cmd.table,
                    method: cmd.criteria ? 'Filtered Scan' : 'Full Table Scan'
                });

                if (cmd.groupBy) {
                    plan.steps.push({
                        operation: 'GROUP',
                        field: cmd.groupBy,
                        method: 'Hash-based grouping'
                    });
                }

                plan.steps.push({
                    operation: 'AGGREGATE',
                    function: cmd.func,
                    field: cmd.field || '*'
                });

                if (cmd.having) {
                    plan.steps.push({
                        operation: 'HAVING',
                        condition: `${cmd.having.field} ${cmd.having.op} ${cmd.having.val}`
                    });
                }
                break;
            }
        }

        // Add available indexes info
        const tableIndexes = [];
        for (const [key] of this.indexes) {
            if (key.startsWith(cmd.table + '.')) {
                tableIndexes.push(key);
            }
        }
        if (tableIndexes.length > 0) {
            plan.availableIndexes = tableIndexes;
        }

        return plan;
    }
}

module.exports = SawitDB;
