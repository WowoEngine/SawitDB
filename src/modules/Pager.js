const fs = require('fs');

const PAGE_SIZE = 4096;
const MAGIC = 'WOWO';

/**
 * Pager handles 4KB page I/O
 * Includes simple LRU Cache
 */
class Pager {
    constructor(filePath, wal = null) {
        this.filePath = filePath;
        this.fd = null;
        this.cache = new Map(); // PageID -> Buffer
        this.cacheLimit = 15000; // Increased cache limit for performance
        this.wal = wal;

        // LAZY WRITE OPTIMIZATION
        this.dirtyPages = new Set(); // Track pages that need flushing
        this.lazyWrite = true; // Enable by default for performance

        // OPTIMIZATION: Buffer pool
        this.bufferPool = [];
        this.maxPoolSize = 1000;

        // OBJECT CACHE
        this.objectCache = new Map(); // pageId -> { next: uint32, items: [] }
        this.dirtyObjects = new Set(); // pageIds

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

    _allocBuffer() {
        if (this.bufferPool.length > 0) {
            return this.bufferPool.pop();
        }
        return Buffer.allocUnsafe(PAGE_SIZE);
    }

    _releaseBuffer(buf) {
        if (this.bufferPool.length < this.maxPoolSize) {
            this.bufferPool.push(buf);
        }
    }

    /**
     * OPTIMIZATION: Read page as Objects
     */
    readPageObjects(pageId) {
        if (this.objectCache.has(pageId)) {
            const entry = this.objectCache.get(pageId);
            this.objectCache.delete(pageId);
            this.objectCache.set(pageId, entry);
            return entry;
        }

        const buffer = this.readPage(pageId);

        // Parse Header
        const next = buffer.readUInt32LE(0);
        const count = buffer.readUInt16LE(4);

        const items = [];
        let offset = 8;

        for (let i = 0; i < count; i++) {
            const len = buffer.readUInt16LE(offset);
            const jsonStr = buffer.toString('utf8', offset + 2, offset + 2 + len);
            try {
                const obj = JSON.parse(jsonStr);
                // Inject hint? No, kept clean.
                items.push(obj);
            } catch (e) { }
            offset += 2 + len;
        }

        const entry = { next, items };

        this.objectCache.set(pageId, entry);
        return entry;
    }

    /**
     * Serialize Objects back to Buffer
     */
    _serializeObjectsToBuffer(pageId) {
        if (!this.objectCache.has(pageId)) return;

        const entry = this.objectCache.get(pageId);
        const buffer = Buffer.alloc(Pager.PAGE_SIZE);

        // Header
        buffer.writeUInt32LE(entry.next, 0);
        buffer.writeUInt16LE(entry.items.length, 4);

        let offset = 8;
        for (const obj of entry.items) {
            const jsonStr = JSON.stringify(obj);
            const len = Buffer.byteLength(jsonStr, 'utf8');

            if (offset + 2 + len > Pager.PAGE_SIZE) break;

            buffer.writeUInt16LE(len, offset);
            buffer.write(jsonStr, offset + 2, len, 'utf8');
            offset += 2 + len;
        }

        // Free offset update? 
        // Original Pager used byte 6 for freeOffset. 
        // We need to preserve that if we want full compat!
        // _insertMany reads byte 6. 
        buffer.writeUInt16LE(offset, 6); // Update free offset

        this.cache.set(pageId, buffer);
        this.dirtyObjects.delete(pageId);
    }

    readPage(pageId) {
        // Coherency: If we have dirty objects, serialize them first
        if (this.dirtyObjects.has(pageId)) {
            this._serializeObjectsToBuffer(pageId);
        }

        if (this.cache.has(pageId)) {
            const buf = this.cache.get(pageId);
            // True LRU: Move to end by delete+set (marks as recently used)
            this.cache.delete(pageId);
            this.cache.set(pageId, buf);
            return buf;
        }

        const buf = this._allocBuffer(); // Use pool
        const offset = pageId * PAGE_SIZE;
        try {
            fs.readSync(this.fd, buf, 0, PAGE_SIZE, offset);
        } catch (e) {
            // Return buffer to pool on error
            if (e.code !== 'EOF') {
                this._releaseBuffer(buf);
                throw e;
            }
        }

        this.cache.set(pageId, buf);
        // LRU eviction - evict oldest (first) entries
        while (this.cache.size > this.cacheLimit) {
            const firstKey = this.cache.keys().next().value;
            // Don't evict dirty pages without flushing!
            if (this.dirtyPages.has(firstKey)) {
                this._flushPage(firstKey);
            }
            const oldBuf = this.cache.get(firstKey);
            this.cache.delete(firstKey);
            // Also clean up object cache for this page
            this.objectCache.delete(firstKey);
            this._releaseBuffer(oldBuf);
        }

        return buf;
    }

    writePage(pageId, buf) {
        if (buf.length !== PAGE_SIZE) throw new Error("Buffer must be 4KB");

        // WAL: Log before-image and after-image
        if (this.wal && this.wal.enabled) {
            // Note: Optimizing this to avoid readPage if already in cache is good
            // but readPage handles cache check.
            // For max speed, we might want to skip logging full pages if not needed, 
            // but for safety we keep it. 
            // Async WAL will handle the throughput.
            const beforeImage = this.cache.get(pageId) || buf; // Approximation if new page
            this.wal.logOperation('UPDATE', 'page', pageId, beforeImage, buf);
        }

        this.cache.set(pageId, buf);
        this.objectCache.delete(pageId); // INVALIDATE OBJECT CACHE

        if (this.lazyWrite) {
            this.dirtyPages.add(pageId);
        } else {
            this._flushPage(pageId);
        }
    }

    _flushPage(pageId) {
        if (this.dirtyObjects.has(pageId)) {
            this._serializeObjectsToBuffer(pageId);
        }

        const buf = this.cache.get(pageId);
        if (!buf) return;
        const offset = pageId * PAGE_SIZE;
        fs.writeSync(this.fd, buf, 0, PAGE_SIZE, offset);
        this.dirtyPages.delete(pageId);
    }

    flush() {
        // Flush object dirty pages first? handled by _flushPage
        if (this.dirtyPages.size === 0 && this.dirtyObjects.size === 0) return;

        // Merge sets
        const allDirty = new Set([...this.dirtyPages, ...this.dirtyObjects]);
        const sortedPages = Array.from(allDirty).sort((a, b) => a - b);

        for (const pageId of sortedPages) {
            this._flushPage(pageId);
        }
    }

    allocPage() {
        const page0 = this.readPage(0);
        const totalPages = page0.readUInt32LE(4);

        const newPageId = totalPages;
        const newTotal = totalPages + 1;

        page0.writeUInt32LE(newTotal, 4);
        this.writePage(0, page0);

        const newPage = this._allocBuffer();
        newPage.fill(0);
        newPage.writeUInt32LE(0, 0); // Next Page = 0
        newPage.writeUInt16LE(0, 4); // Count = 0
        newPage.writeUInt16LE(8, 6); // Free Offset = 8
        this.writePage(newPageId, newPage);

        return newPageId;
    }

    close() {
        if (this.fd !== null) {
            this.flush(); // Ensure all data is written
            fs.closeSync(this.fd);
            this.fd = null;
            this.cache.clear();
            this.objectCache.clear();
            this.dirtyPages.clear();
            this.dirtyObjects.clear();
        }
    }

    _enforceLimit() {
        while (this.cache.size > this.cacheLimit) {
            const iterator = this.cache.keys();
            const oldestPageId = iterator.next().value;

            if (this.dirtyPages.has(oldestPageId) || this.dirtyObjects.has(oldestPageId)) {
                this._flushPage(oldestPageId);
            }

            this.cache.delete(oldestPageId);
            // Also eviction from object cache
            this.objectCache.delete(oldestPageId);
            this.dirtyObjects.delete(oldestPageId);
        }
    }
}

Pager.PAGE_SIZE = PAGE_SIZE;

module.exports = Pager;
