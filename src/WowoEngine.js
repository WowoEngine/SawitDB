const Pager = require('./modules/Pager');
const QueryParser = require('./modules/QueryParser');
const BTreeIndex = require('./modules/BTreeIndex');

/**
 * SawitDB implements the Logic over the Pager
 */
class SawitDB {
    constructor(filePath) {
        this.pager = new Pager(filePath);
        this.indexes = new Map(); // Map of 'tableName.fieldName' -> BTreeIndex
        this.parser = new QueryParser();
    }

    query(queryString, params) {
        // Parse the query into a command object
        const cmd = this.parser.parse(queryString, params);

        if (cmd.type === 'EMPTY') return "";
        if (cmd.type === 'ERROR') return `Error: ${cmd.message}`;

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

                case 'SELECT':
                    // Map generic generic Select Logic
                    const rows = this._select(cmd.table, cmd.criteria, cmd.sort, cmd.limit, cmd.offset);
                    // Projection handled inside _select or here?
                    // _select now handles SORT/LIMIT/OFFSET which acts on All Rows (mostly).
                    // Projection should be last.

                    if (cmd.cols.length === 1 && cmd.cols[0] === '*') return rows;

                    return rows.map(r => {
                        const newRow = {};
                        cmd.cols.forEach(c => newRow[c] = r[c]);
                        return newRow;
                    });

                case 'DELETE':
                    return this._delete(cmd.table, cmd.criteria);

                case 'UPDATE':
                    return this._update(cmd.table, cmd.updates, cmd.criteria);

                case 'DROP_TABLE':
                    return this._dropTable(cmd.table);

                case 'CREATE_INDEX':
                    return this._createIndex(cmd.table, cmd.field);

                case 'AGGREGATE':
                    return this._aggregate(cmd.table, cmd.func, cmd.field, cmd.criteria, cmd.groupBy);

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
            tables.push(tName);
            offset += 40;
        }
        return tables;
    }

    _createTable(name) {
        if (!name) throw new Error("Nama kebun tidak boleh kosong");
        if (name.length > 32) throw new Error("Nama kebun max 32 karakter");
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
        return `Kebun '${name}' telah dibuka.`;
    }

    _dropTable(name) {
        // Simple Drop: Remove from directory. Pages leak (fragmentation) but that's typical for simple heap files.
        const entry = this._findTableEntry(name);
        if (!entry) return `Kebun '${name}' tidak ditemukan.`;

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

        return `Kebun '${name}' telah dibakar (Drop).`;
    }

    _updateTableLastPage(name, newLastPageId) {
        const entry = this._findTableEntry(name);
        if (!entry) throw new Error("Internal Error: Table missing for update");
        const p0 = this.pager.readPage(0);
        p0.writeUInt32LE(newLastPageId, entry.offset + 36);
        this.pager.writePage(0, p0);
    }

    _insert(table, data) {
        if (!data || Object.keys(data).length === 0) {
            throw new Error("Data kosong / fiktif? Ini melanggar integritas (Korupsi Data).");
        }

        const entry = this._findTableEntry(table);
        if (!entry) throw new Error(`Kebun '${table}' tidak ditemukan.`);

        const dataStr = JSON.stringify(data);
        const dataBuf = Buffer.from(dataStr, 'utf8');
        const recordLen = dataBuf.length;
        const totalLen = 2 + recordLen;

        let currentPageId = entry.lastPage;
        let pData = this.pager.readPage(currentPageId);
        let freeOffset = pData.readUInt16LE(6);

        if (freeOffset + totalLen > Pager.PAGE_SIZE) {
            const newPageId = this.pager.allocPage();
            pData.writeUInt32LE(newPageId, 0);
            this.pager.writePage(currentPageId, pData);

            currentPageId = newPageId;
            pData = this.pager.readPage(currentPageId);
            freeOffset = pData.readUInt16LE(6);
            this._updateTableLastPage(table, currentPageId);
        }

        pData.writeUInt16LE(recordLen, freeOffset);
        dataBuf.copy(pData, freeOffset + 2);

        const count = pData.readUInt16LE(4);
        pData.writeUInt16LE(count + 1, 4);
        pData.writeUInt16LE(freeOffset + totalLen, 6);

        this.pager.writePage(currentPageId, pData);

        // Update Indexes if any
        this._updateIndexes(table, data);

        return "Bibit tertanam.";
    }

    _updateIndexes(table, data) {
        for (const [indexKey, index] of this.indexes) {
            const [tbl, field] = indexKey.split('.');
            if (tbl === table && data.hasOwnProperty(field)) {
                index.insert(data[field], data);
            }
        }
    }

    _checkMatch(obj, criteria) {
        if (!criteria) return true;

        // Handle compound conditions (AND/OR)
        if (criteria.type === 'compound') {
            let result = true;
            let currentLogic = 'AND'; // Initial logic is irrelevant for first item, but technically AND identity is true

            for (let i = 0; i < criteria.conditions.length; i++) {
                const cond = criteria.conditions[i];
                const matches = this._checkSingleCondition(obj, cond);

                if (i === 0) {
                    result = matches;
                } else {
                    if (cond.logic === 'OR') {
                        result = result || matches;
                    } else {
                        result = result && matches;
                    }
                }
            }
            return result;
        }

        // Simple single condition
        return this._checkSingleCondition(obj, criteria);
    }

    _checkSingleCondition(obj, criteria) {
        const val = obj[criteria.key];
        const target = criteria.val;
        switch (criteria.op) {
            case '=': return val == target;
            case '!=': return val != target;
            case '>': return val > target;
            case '<': return val < target;
            case '>=': return val >= target;
            case '<=': return val <= target;
            case 'IN': return Array.isArray(target) && target.includes(val);
            case 'NOT IN': return Array.isArray(target) && !target.includes(val);
            case 'LIKE':
                // Simple regex-like match. Handle % for wildcards.
                const regexStr = '^' + target.replace(/%/g, '.*') + '$';
                const re = new RegExp(regexStr, 'i');
                return re.test(String(val));
            case 'BETWEEN':
                return val >= target[0] && val <= target[1];
            case 'IS NULL':
                return val === null || val === undefined;
            case 'IS NOT NULL':
                return val !== null && val !== undefined;
            default: return false;
        }
    }

    _select(table, criteria, sort, limit, offsetCount) {
        const entry = this._findTableEntry(table);
        if (!entry) throw new Error(`Kebun '${table}' tidak ditemukan.`);

        // Optimization: If Index exists and criteria is simple '='
        // Only valid if NO sorting is needed, or if index matches sort (not implemented yet).
        // For now, always do full scan if sort/fancy criteria involved 
        // OR rely on in-memory sort after index fetch.

        let results = [];

        // --- 1. Fetch Candidates ---
        if (criteria && !criteria.type && criteria.op === '=' && !sort) {
            // Index optimization path - only if no sort (for safety)
            // ... (Index Logic) ...
            const indexKey = `${table}.${criteria.key}`;
            if (this.indexes.has(indexKey)) {
                const index = this.indexes.get(indexKey);
                results = index.search(criteria.val);
            } else {
                results = this._scanTable(entry, criteria);
            }
        } else {
            results = this._scanTable(entry, criteria);
        }

        // --- 2. Sorting (In-Memory) ---
        if (sort) {
            // sort = { key, dir: 'ASC' | 'DESC' }
            results.sort((a, b) => {
                const valA = a[sort.key];
                const valB = b[sort.key];
                if (valA < valB) return sort.dir === 'asc' ? -1 : 1;
                if (valA > valB) return sort.dir === 'asc' ? 1 : -1;
                return 0;
            });
        }

        // --- 3. Limit & Offset ---
        let start = 0;
        let end = results.length;

        if (offsetCount) start = offsetCount;
        if (limit) end = start + limit;

        return results.slice(start, end);
    }

    _scanTable(entry, criteria) {
        let currentPageId = entry.startPage;
        const results = [];

        while (currentPageId !== 0) {
            const pData = this.pager.readPage(currentPageId);
            const count = pData.readUInt16LE(4);
            let offset = 8;

            for (let i = 0; i < count; i++) {
                const len = pData.readUInt16LE(offset);
                const jsonStr = pData.toString('utf8', offset + 2, offset + 2 + len);
                try {
                    const obj = JSON.parse(jsonStr);
                    if (this._checkMatch(obj, criteria)) {
                        results.push(obj);
                    }
                } catch (err) { }
                offset += 2 + len;
            }
            currentPageId = pData.readUInt32LE(0);
        }
        return results;
    }

    _delete(table, criteria) {
        const entry = this._findTableEntry(table);
        if (!entry) throw new Error(`Kebun '${table}' tidak ditemukan.`);

        let currentPageId = entry.startPage;
        let deletedCount = 0;

        while (currentPageId !== 0) {
            let pData = this.pager.readPage(currentPageId);
            const count = pData.readUInt16LE(4);
            let offset = 8;
            const recordsToKeep = [];

            for (let i = 0; i < count; i++) {
                const len = pData.readUInt16LE(offset);
                const jsonStr = pData.toString('utf8', offset + 2, offset + 2 + len);
                let shouldDelete = false;
                try {
                    const obj = JSON.parse(jsonStr);
                    if (this._checkMatch(obj, criteria)) shouldDelete = true;
                } catch (e) { }

                if (shouldDelete) {
                    deletedCount++;
                } else {
                    recordsToKeep.push({ len, data: pData.slice(offset + 2, offset + 2 + len) });
                }
                offset += 2 + len;
            }

            if (recordsToKeep.length < count) {
                let writeOffset = 8;
                pData.writeUInt16LE(recordsToKeep.length, 4);
                for (let rec of recordsToKeep) {
                    pData.writeUInt16LE(rec.len, writeOffset);
                    rec.data.copy(pData, writeOffset + 2);
                    writeOffset += 2 + rec.len;
                }
                pData.writeUInt16LE(writeOffset, 6);
                pData.fill(0, writeOffset);
                this.pager.writePage(currentPageId, pData);
            }
            currentPageId = pData.readUInt32LE(0);
        }
        return `Berhasil menggusur ${deletedCount} bibit.`;
    }

    _update(table, updates, criteria) {
        const records = this._select(table, criteria);
        if (records.length === 0) return "Tidak ada bibit yang cocok untuk dipupuk.";

        this._delete(table, criteria);

        let count = 0;
        for (const rec of records) {
            for (const k in updates) {
                rec[k] = updates[k];
            }
            this._insert(table, rec);
            count++;
        }
        return `Berhasil memupuk ${count} bibit.`;
    }

    // --- Index Management ---

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

    // --- Aggregation Support ---

    _aggregate(table, func, field, criteria, groupBy) {
        const records = this._select(table, criteria);

        if (groupBy) {
            return this._groupedAggregate(records, func, field, groupBy);
        }

        switch (func.toUpperCase()) {
            case 'COUNT':
                return { count: records.length };

            case 'SUM':
                if (!field) throw new Error("SUM requires a field");
                const sum = records.reduce((acc, r) => acc + (Number(r[field]) || 0), 0);
                return { sum, field };

            case 'AVG':
                if (!field) throw new Error("AVG requires a field");
                const avg = records.reduce((acc, r) => acc + (Number(r[field]) || 0), 0) / records.length;
                return { avg, field, count: records.length };

            case 'MIN':
                if (!field) throw new Error("MIN requires a field");
                const min = Math.min(...records.map(r => Number(r[field]) || Infinity));
                return { min, field };

            case 'MAX':
                if (!field) throw new Error("MAX requires a field");
                const max = Math.max(...records.map(r => Number(r[field]) || -Infinity));
                return { max, field };

            default:
                throw new Error(`Unknown aggregate function: ${func}`);
        }
    }

    _groupedAggregate(records, func, field, groupBy) {
        const groups = {};

        // Group records
        for (const record of records) {
            const key = record[groupBy];
            if (!groups[key]) {
                groups[key] = [];
            }
            groups[key].push(record);
        }

        // Apply aggregate to each group
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
                    result.avg = groupRecords.reduce((acc, r) => acc + (Number(r[field]) || 0), 0) / groupRecords.length;
                    break;

                case 'MIN':
                    result.min = Math.min(...groupRecords.map(r => Number(r[field]) || Infinity));
                    break;

                case 'MAX':
                    result.max = Math.max(...groupRecords.map(r => Number(r[field]) || -Infinity));
                    break;
            }
            results.push(result);
        }
        return results;
    }
}

module.exports = SawitDB;
