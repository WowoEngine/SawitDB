const BTreeIndex = require('../modules/BTreeIndex');

class IndexManager {
    constructor(db) {
        this.db = db;
        // Indexes are stored in db.indexes for now to maintain state
        // access via db.indexes
    }

    get indexes() {
        return this.db.indexes;
    }

    createIndex(table, field) {
        const entry = this.db.tableManager
            ? this.db.tableManager.findTableEntry(table)
            : this.db._findTableEntry(table);

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
        // We need a way to scan the table. 
        // If TableManager has no scan capability yet, use db._select or db._scanTable fallback
        // Ideally this logic belongs here or uses a Scanner service.
        // For now, assuming db._select or similar is available or we replicate scan logic.
        // Actually, db._select depends on QueryExecutor now?
        // Let's rely on db._select being available (maybe delegating to SelectExecutor!)
        // Circular dependency risk: SelectExecutor needs IndexManager, IndexManager needs SelectExecutor logic?
        // Index building needs a simple table scan.
        // Let's imply we can use db._scanTable directly if available.

        let allRecords = [];
        if (this.db._scanTable) {
            // Use low-level scan if available, WITH HINTS to capture _pageId
            allRecords = this.db._scanTable(entry, null, null, true);
        } else if (this.db.query) {
            // Use high level query? Risky.
            // Assume _scanTable is preserved or moved to a Scanner.
            // For now, let's assume WowoEngine keeps _scanTable or we move it to TableScanner.
            // Let's use `db._scanTable` assuming it exists.
            allRecords = this.db._scanTable(entry, null, null, true);
        }

        for (const record of allRecords) {
            if (record.hasOwnProperty(field)) {
                index.insert(record[field], record);
            }
        }

        this.indexes.set(indexKey, index);

        // PERSISTENCE: Save to _indexes table
        try {
            // Use db._insert to persist request. 
            // If InsertExecutor delegates to db._insert, or db._insert uses InsertExecutor?
            // Safer to allow db._insert if it resolves to the executor.
            this.db._insert('_indexes', { table, field });
        } catch (e) {
            console.error("Failed to persist index definition", e);
        }

        return `Indeks dibuat pada '${table}.${field}' (${allRecords.length} records indexed)`;
    }

    updateIndexes(table, newObj, oldObj) {
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

    removeFromIndexes(table, data) {
        for (const [indexKey, index] of this.indexes) {
            const [tbl, field] = indexKey.split('.');
            if (tbl === table && data.hasOwnProperty(field)) {
                index.delete(data[field]); // Basic deletion from B-Tree
            }
        }
    }

    showIndexes(table) {
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

    // Initial loader
    loadIndexes() {
        if (!this.db._select) return; // Wait until ready?

        // Re-implement load indexes to include Hints
        // We need to read _indexes.
        // db._select might use SelectExecutor, which uses IndexManager... 
        // Bootstrapping issue.
        // We should use low-level scan for bootstrapping.

        let indexRecords = [];
        try {
            // Manual scan of _indexes
            const entry = this.db.tableManager
                ? this.db.tableManager.findTableEntry('_indexes')
                : this.db._findTableEntry('_indexes');

            if (entry && this.db._scanTable) {
                indexRecords = this.db._scanTable(entry, null);
            }
        } catch (e) { return; }

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
                    const entry = this.db.tableManager
                        ? this.db.tableManager.findTableEntry(table)
                        : this.db._findTableEntry(table);

                    if (entry && this.db._scanTable) {
                        const allRecords = this.db._scanTable(entry, null, null, true); // true for Hints
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
}

module.exports = IndexManager;
