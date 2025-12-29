const fs = require('fs');
const BTreeIndex = require('./BTreeIndex');

const PAGE_SIZE = 4096;
const MAGIC = 'WOWO';

/**
 * Pager handles 4KB page I/O
 */
class Pager {
    constructor(filePath) {
        this.filePath = filePath;
        this.fd = null;
        this._open();
    }

    _open() {
        if (!fs.existsSync(this.filePath)) {
            this.fd = fs.openSync(this.filePath, 'w+');
            this._initNewFile();
        } else {
            this.fd = fs.openSync(this.filePath, 'r+');
        }
    }

    _initNewFile() {
        const buf = Buffer.alloc(PAGE_SIZE);
        buf.write(MAGIC, 0);
        buf.writeUInt32LE(1, 4); // Total Pages = 1
        buf.writeUInt32LE(0, 8); // Num Tables = 0
        fs.writeSync(this.fd, buf, 0, PAGE_SIZE, 0);
    }

    readPage(pageId) {
        const buf = Buffer.alloc(PAGE_SIZE);
        const offset = pageId * PAGE_SIZE;
        fs.readSync(this.fd, buf, 0, PAGE_SIZE, offset);
        return buf;
    }

    writePage(pageId, buf) {
        if (buf.length !== PAGE_SIZE) throw new Error("Buffer must be 4KB");
        const offset = pageId * PAGE_SIZE;
        fs.writeSync(this.fd, buf, 0, PAGE_SIZE, offset);
        // STABILITY UPGRADE: Force write to disk. 
        try { fs.fsyncSync(this.fd); } catch (e) { /* Ignore if not supported */ }
    }

    allocPage() {
        const page0 = this.readPage(0);
        const totalPages = page0.readUInt32LE(4);

        const newPageId = totalPages;
        const newTotal = totalPages + 1;

        page0.writeUInt32LE(newTotal, 4);
        this.writePage(0, page0);

        const newPage = Buffer.alloc(PAGE_SIZE);
        newPage.writeUInt32LE(0, 0); // Next Page = 0
        newPage.writeUInt16LE(0, 4); // Count = 0
        newPage.writeUInt16LE(8, 6); // Free Offset = 8
        this.writePage(newPageId, newPage);

        return newPageId;
    }
}

/**
 * SawitDB implements the Logic over the Pager
 */
class SawitDB {
    constructor(filePath) {
        this.pager = new Pager(filePath);
        this.indexes = new Map(); // Map of 'tableName.fieldName' -> BTreeIndex
    }

    /**
     * Tokenizer
     */
    _tokenize(sql) {
        // Regex to match tokens: keywords, identifiers, strings, numbers, symbols, comparators
        const tokenRegex = /\s*(=>|!=|>=|<=|<>|[(),=*.]|[a-zA-Z_]\w*|\d+|'[^']*'|"[^"]*")\s*/g;
        const tokens = [];
        let match;
        while ((match = tokenRegex.exec(sql)) !== null) {
            tokens.push(match[1]);
        }
        return tokens;
    }

    query(queryString) {
        const tokens = this._tokenize(queryString);
        if (tokens.length === 0) return "";

        const cmd = tokens[0].toUpperCase();

        try {
            switch (cmd) {
                case 'LAHAN':
                    return this._parseCreate(tokens);
                case 'LIHAT': // SHOW TABLES or SHOW INDEXES
                    return this._parseShow(tokens);
                case 'TANAM':
                    return this._parseInsert(tokens);
                case 'PANEN':
                    return this._parseSelect(tokens);
                case 'GUSUR':
                    return this._parseDelete(tokens);
                case 'PUPUK':
                    return this._parseUpdate(tokens);
                case 'BAKAR': // DROP TABLE
                    return this._parseDrop(tokens);
                case 'INDEKS': // CREATE INDEX
                    return this._parseCreateIndex(tokens);
                case 'HITUNG': // AGGREGATE
                    return this._parseAggregate(tokens);
                default:
                    return `Perintah tidak dikenal: ${cmd}`;
            }
        } catch (e) {
            return `Error: ${e.message}`;
        }
    }

    // --- Parser Methods ---

    // LAHAN users
    _parseCreate(tokens) {
        if (tokens.length < 2) throw new Error("Syntax: LAHAN [nama_kebun]");
        return this._createTable(tokens[1]);
    }

    // LIHAT LAHAN or LIHAT INDEKS
    _parseShow(tokens) {
        if (tokens[1]) {
            const subCmd = tokens[1].toUpperCase();
            if (subCmd === 'LAHAN') {
                return this._showTables();
            } else if (subCmd === 'INDEKS') {
                const table = tokens[2] || null;
                return this._showIndexes(table);
            }
        }
        throw new Error("Syntax: LIHAT LAHAN | LIHAT INDEKS [table]");
    }

    // BAKAR LAHAN users
    _parseDrop(tokens) {
        if (tokens[1] && tokens[1].toUpperCase() === 'LAHAN') {
            if (tokens.length < 3) throw new Error("Syntax: BAKAR LAHAN [nama_kebun]");
            return this._dropTable(tokens[2]);
        }
        throw new Error("Syntax: BAKAR LAHAN [nama_kebun]");
    }

    // TANAM KE ...
    _parseInsert(tokens) {
        if (tokens[1].toUpperCase() !== 'KE') throw new Error("Syntax: TANAM KE [kebun] ...");
        const table = tokens[2];

        let i = 3;
        const cols = [];
        if (tokens[i] === '(') {
            i++;
            while (tokens[i] !== ')') {
                if (tokens[i] !== ',') cols.push(tokens[i]);
                i++;
                if (i >= tokens.length) throw new Error("Unclosed parenthesis in columns");
            }
            i++;
        } else {
            throw new Error("Syntax: TANAM KE [kebun] (col1, ...) ...");
        }

        if (tokens[i].toUpperCase() !== 'BIBIT') throw new Error("Expected BIBIT");
        i++;

        const vals = [];
        if (tokens[i] === '(') {
            i++;
            while (tokens[i] !== ')') {
                if (tokens[i] !== ',') {
                    let val = tokens[i];
                    if (val.startsWith("'") || val.startsWith('"')) val = val.slice(1, -1);
                    else if (!isNaN(val)) val = Number(val);
                    vals.push(val);
                }
                i++;
            }
        } else {
            throw new Error("Syntax: ... BIBIT (val1, ...)");
        }

        if (cols.length !== vals.length) throw new Error("Columns and Values count mismatch");

        const data = {};
        for (let k = 0; k < cols.length; k++) {
            data[cols[k]] = vals[k];
        }

        return this._insert(table, data);
    }

    // PANEN ...
    _parseSelect(tokens) {
        let i = 1;
        const cols = [];
        while (tokens[i].toUpperCase() !== 'DARI') {
            if (tokens[i] !== ',') cols.push(tokens[i]);
            i++;
            if (i >= tokens.length) throw new Error("Expected DARI");
        }
        i++; // Skip DARI
        const table = tokens[i];
        i++;

        let criteria = null;
        if (i < tokens.length && tokens[i].toUpperCase() === 'DIMANA') {
            i++;
            criteria = this._parseWhere(tokens, i);
        }

        const rows = this._select(table, criteria);
        if (cols.length === 1 && cols[0] === '*') return rows;

        return rows.map(r => {
            const newRow = {};
            cols.forEach(c => newRow[c] = r[c]);
            return newRow;
        });
    }

    _parseWhere(tokens, startIndex) {
        // Enhanced parser supporting AND/OR
        const conditions = [];
        let i = startIndex;
        let currentLogic = 'AND';

        while (i < tokens.length) {
            const token = tokens[i];
            const upper = token ? token.toUpperCase() : '';

            if (upper === 'AND' || upper === 'OR') {
                currentLogic = upper;
                i++;
                continue;
            }

            // Stop if we hit another keyword
            if (['DENGAN', 'ORDER', 'LIMIT', 'GROUP'].includes(upper)) {
                break;
            }

            // Parse condition: key op val
            if (i + 2 < tokens.length) {
                const key = tokens[i];
                const op = tokens[i + 1];
                let val = tokens[i + 2];

                if (val && (val.startsWith("'") || val.startsWith('"'))) {
                    val = val.slice(1, -1);
                } else if (val && !isNaN(val)) {
                    val = Number(val);
                }

                conditions.push({ key, op, val, logic: currentLogic });
                i += 3;
            } else {
                break;
            }
        }

        // Return single condition format for backwards compatibility
        if (conditions.length === 1) {
            return conditions[0];
        }

        return { type: 'compound', conditions };
    }

    // GUSUR ...
    _parseDelete(tokens) {
        if (tokens[1].toUpperCase() !== 'DARI') throw new Error("Syntax: GUSUR DARI [kebun] ...");
        const table = tokens[2];

        let i = 3;
        let criteria = null;
        if (i < tokens.length && tokens[i].toUpperCase() === 'DIMANA') {
            i++;
            criteria = this._parseWhere(tokens, i);
        }

        return this._delete(table, criteria);
    }

    // PUPUK ...
    _parseUpdate(tokens) {
        if (tokens.length < 3) throw new Error("Syntax: PUPUK [kebun] DENGAN ...");
        const table = tokens[1];
        if (tokens[2].toUpperCase() !== 'DENGAN') throw new Error("Expected DENGAN");

        let i = 3;
        const updates = {};
        while (i < tokens.length && tokens[i].toUpperCase() !== 'DIMANA') {
            if (tokens[i] === ',') { i++; continue; }
            const key = tokens[i];
            if (tokens[i + 1] !== '=') throw new Error("Syntax: key=value in update list");
            let val = tokens[i + 2];
            if (val.startsWith("'") || val.startsWith('"')) val = val.slice(1, -1);
            else if (!isNaN(val)) val = Number(val);
            updates[key] = val;
            i += 3;
        }

        let criteria = null;
        if (i < tokens.length && tokens[i].toUpperCase() === 'DIMANA') {
            i++;
            criteria = this._parseWhere(tokens, i);
        }
        return this._update(table, updates, criteria);
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
        if (name.length > 32) throw new Error("Nama kebun max 32 karakter");
        if (this._findTableEntry(name)) return `Kebun '${name}' sudah ada.`;

        const p0 = this.pager.readPage(0);
        const numTables = p0.readUInt32LE(8);
        let offset = 12 + (numTables * 40);
        if (offset + 40 > PAGE_SIZE) throw new Error("Lahan penuh (Page 0 full)");

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

        if (freeOffset + totalLen > PAGE_SIZE) {
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
        return "Bibit tertanam.";
    }

    _checkMatch(obj, criteria) {
        if (!criteria) return true;

        // Handle compound conditions (AND/OR)
        if (criteria.type === 'compound') {
            let result = true;
            let currentLogic = 'AND';

            for (const cond of criteria.conditions) {
                const matches = this._checkSingleCondition(obj, cond);
                
                if (cond.logic === 'OR' || currentLogic === 'OR') {
                    result = result || matches;
                    currentLogic = 'OR';
                } else {
                    result = result && matches;
                    currentLogic = 'AND';
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
            default: return false;
        }
    }

    _select(table, criteria) {
        const entry = this._findTableEntry(table);
        if (!entry) throw new Error(`Kebun '${table}' tidak ditemukan.`);

        // Try to use index if available and criteria is simple
        if (criteria && !criteria.type && criteria.op === '=') {
            const indexKey = `${table}.${criteria.key}`;
            if (this.indexes.has(indexKey)) {
                const index = this.indexes.get(indexKey);
                const indexedRecords = index.search(criteria.val);
                return indexedRecords;
            }
        }

        // Fall back to full table scan
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

    /**
     * Create an index on a table field
     * INDEKS [table] PADA [field]
     */
    _parseCreateIndex(tokens) {
        if (tokens.length < 4) throw new Error("Syntax: INDEKS [table] PADA [field]");
        const table = tokens[1];
        if (tokens[2].toUpperCase() !== 'PADA') throw new Error("Expected PADA");
        const field = tokens[3];

        return this._createIndex(table, field);
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

    // Override _insert to update indexes
    _insertWithIndexUpdate(table, data) {
        const result = this._insert(table, data);
        
        // Update indexes
        for (const [indexKey, index] of this.indexes) {
            const [tbl, field] = indexKey.split('.');
            if (tbl === table && data.hasOwnProperty(field)) {
                index.insert(data[field], data);
            }
        }

        return result;
    }

    // --- Aggregation Support ---

    /**
     * Aggregate functions: COUNT, SUM, AVG, MIN, MAX, GROUP BY
     * HITUNG COUNT(*) DARI [table]
     * HITUNG SUM(field) DARI [table] DIMANA ...
     * HITUNG AVG(field) DARI [table] KELOMPOK [field]
     */
    _parseAggregate(tokens) {
        let i = 1;
        
        // Parse aggregate function
        const funcToken = tokens[i];
        let aggFunc = null;
        let aggField = null;

        if (funcToken.includes('(')) {
            // Parse FUNC(field)
            const match = funcToken.match(/([A-Z]+)\\((.*)\\)/);
            if (match) {
                aggFunc = match[1];
                aggField = match[2] === '*' ? null : match[2];
            }
            i++;
        } else {
            throw new Error("Syntax: HITUNG FUNC(field) DARI [table]");
        }

        // Expect DARI
        if (!tokens[i] || tokens[i].toUpperCase() !== 'DARI') {
            throw new Error("Expected DARI");
        }
        i++;

        const table = tokens[i];
        i++;

        // Parse WHERE clause
        let criteria = null;
        if (i < tokens.length && tokens[i].toUpperCase() === 'DIMANA') {
            i++;
            criteria = this._parseWhere(tokens, i);
            // Skip past where conditions
            while (i < tokens.length && !['KELOMPOK'].includes(tokens[i].toUpperCase())) {
                i++;
            }
        }

        // Parse GROUP BY
        let groupField = null;
        if (i < tokens.length && tokens[i].toUpperCase() === 'KELOMPOK') {
            i++;
            groupField = tokens[i];
        }

        return this._aggregate(table, aggFunc, aggField, criteria, groupField);
    }

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
