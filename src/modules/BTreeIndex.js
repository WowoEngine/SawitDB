/**
 * Simple B-Tree Index for SawitDB
 * Provides fast lookups by key
 */
class BTreeNode {
    constructor(isLeaf = true) {
        this.isLeaf = isLeaf;
        this.keys = [];
        this.values = []; // For leaf nodes: array of record references
        this.children = []; // For internal nodes
    }
}

class BTreeIndex {
    constructor(order = 32) {
        this.order = order; // Maximum number of keys per node
        this.root = new BTreeNode(true);
        this.name = null;
        this.keyField = null;
    }

    /**
     * Binary search to find insertion point
     * Returns the index where key should be inserted
     */
    _binarySearchInsertPoint(keys, key) {
        let left = 0;
        let right = keys.length;

        while (left < right) {
            const mid = (left + right) >>> 1;
            if (keys[mid] < key) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }

    /**
     * Binary search to find exact key or insertion point
     * Returns { found: boolean, index: number }
     */
    _binarySearch(keys, key) {
        let left = 0;
        let right = keys.length - 1;

        while (left <= right) {
            const mid = (left + right) >>> 1;
            if (keys[mid] === key) {
                return { found: true, index: mid };
            } else if (keys[mid] < key) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return { found: false, index: left };
    }

    /**
     * Insert a key-value pair into the index
     * @param {*} key - The key to index
     * @param {*} value - The value to store (usually record reference/data)
     */
    insert(key, value) {
        const root = this.root;

        // If root is full, split it
        if (root.keys.length >= this.order) {
            const newRoot = new BTreeNode(false);
            newRoot.children.push(this.root);
            this._splitChild(newRoot, 0);
            this.root = newRoot;
        }

        this._insertNonFull(this.root, key, value);
    }

    _insertNonFull(node, key, value) {
        if (node.isLeaf) {
            // OPTIMIZATION: Use binary search to find insertion point
            const insertIdx = this._binarySearchInsertPoint(node.keys, key);
            // Use splice for O(n) insertion (still O(n) due to array shift, but search is O(log n))
            node.keys.splice(insertIdx, 0, key);
            node.values.splice(insertIdx, 0, value);
        } else {
            // Find child to insert into using binary search
            let i = this._binarySearchInsertPoint(node.keys, key);
            // Adjust: if key >= keys[i], we go to child[i+1], but _binarySearchInsertPoint
            // returns where key would be inserted, so child index is the same
            // Actually for internal node navigation: go to child[i] where i is the index
            // such that keys[i-1] < key <= keys[i]

            // OPTIMIZATION & BUGFIX: handled undefined child proactively
            if (!node.children[i]) {
                // If child is missing (should not happen in valid B-Tree), default to last valid child or create? 
                // This indicates tree corruption or logic error. 
                // In a correct B-Tree, children count = keys count + 1.
                // If i > keys.length, it should be the last child.
                /* 
                   Example: Keys [10, 20]
                   Children: [ <10, 10-20, >20 ]
                   i=0 (key<10), i=1 (10<key<20), i=2 (key>20)
                */
                // Recovery logic:
                if (i >= node.children.length) {
                    i = node.children.length - 1;
                }
            }

            if (node.children[i].keys.length >= this.order) {
                this._splitChild(node, i);
                if (key > node.keys[i]) {
                    i++;
                }
            }

            this._insertNonFull(node.children[i], key, value);
        }
    }

    _splitChild(parent, index) {
        const fullNode = parent.children[index];
        const newNode = new BTreeNode(fullNode.isLeaf);
        const mid = Math.floor(this.order / 2);

        // Standard B+ Tree Split
        // Leaf: Split at mid. Right node includes mid. Parent gets COPY of mid key.
        // Internal: Split at mid. Mid key MOVES to parent (not in Left or Right). Children split at mid+1.

        if (fullNode.isLeaf) {
            newNode.keys = fullNode.keys.splice(mid); // Right half
            newNode.values = fullNode.values.splice(mid); // Right half values

            // Promote copy of first key of right node
            const middleKey = newNode.keys[0];
            parent.keys.splice(index, 0, middleKey);
            parent.children.splice(index + 1, 0, newNode);
        } else {
            // Internal Node
            // Move half of keys to new node (mid to end)
            // fullNode has [0..mid-1], [mid], [mid+1..end]
            // We want fullNode: [0..mid-1]
            // newNode: [mid+1..end]
            // pivot: [mid]

            const rightKeys = fullNode.keys.splice(mid);
            const pivot = rightKeys.shift(); // Remove pivot from right keys
            newNode.keys = rightKeys;

            // Children
            // fullNode children: [0..mid] (Keep)
            // newNode children: [mid+1..end] (Move)
            newNode.children = fullNode.children.splice(mid + 1);

            parent.keys.splice(index, 0, pivot);
            parent.children.splice(index + 1, 0, newNode);
        }
    }

    /**
     * Search for a key in the index
     * @param {*} key - The key to search for
     * @returns {Array} - Array of values associated with the key
     */
    search(key) {
        return this._searchNode(this.root, key);
    }

    _searchNode(node, key) {
        // BUGFIX: Handle null/undefined node
        if (!node) {
            return [];
        }

        // OPTIMIZATION: Use binary search instead of linear scan
        const { found, index: i } = this._binarySearch(node.keys, key);

        // Check if key is found
        if (found) {
            if (node.isLeaf) {
                return Array.isArray(node.values[i]) ? node.values[i] : [node.values[i]];
            } else {
                // BUGFIX: Check child exists before recursing
                if (node.children && node.children[i + 1]) {
                    return this._searchNode(node.children[i + 1], key);
                }
                return [];
            }
        }

        // If not found and this is a leaf, return empty
        if (node.isLeaf) {
            return [];
        }

        // BUGFIX: Check child exists before recursing
        if (node.children && node.children[i]) {
            return this._searchNode(node.children[i], key);
        }

        return [];
    }

    /**
     * Range query: find all keys between min and max
     * @param {*} min - Minimum key (inclusive)
     * @param {*} max - Maximum key (inclusive)
     * @returns {Array} - Array of values in range
     */
    range(min, max) {
        const results = [];
        this._rangeSearch(this.root, min, max, results);
        return results;
    }

    _rangeSearch(node, min, max, results) {
        let i = 0;

        while (i < node.keys.length) {
            if (node.isLeaf) {
                if (node.keys[i] >= min && node.keys[i] <= max) {
                    const values = Array.isArray(node.values[i]) ? node.values[i] : [node.values[i]];
                    results.push(...values);
                }
                i++;
            } else {
                if (node.keys[i] > min) {
                    this._rangeSearch(node.children[i], min, max, results);
                }
                if (node.keys[i] >= min && node.keys[i] <= max) {
                    const values = Array.isArray(node.values[i]) ? node.values[i] : [node.values[i]];
                    results.push(...values);
                }
                i++;
            }
        }

        // Search rightmost child
        if (!node.isLeaf && node.children.length > i) {
            this._rangeSearch(node.children[i], min, max, results);
        }
    }

    /**
     * Get all values (full scan)
     * @returns {Array} - All values in the index
     */
    all() {
        const results = [];
        this._collectAll(this.root, results);
        return results;
    }

    _collectAll(node, results) {
        if (node.isLeaf) {
            for (const value of node.values) {
                const values = Array.isArray(value) ? value : [value];
                results.push(...values);
            }
        } else {
            for (let i = 0; i < node.children.length; i++) {
                this._collectAll(node.children[i], results);
            }
        }
    }

    /**
     * Delete a key from the index
     * @param {*} key - The key to delete
     */
    delete(key) {
        this._deleteFromNode(this.root, key);

        // If root is empty after deletion, make its only child the new root
        if (this.root.keys.length === 0 && !this.root.isLeaf) {
            this.root = this.root.children[0];
        }
    }

    _deleteFromNode(node, key) {
        let i = 0;
        while (i < node.keys.length && key > node.keys[i]) {
            i++;
        }

        if (i < node.keys.length && key === node.keys[i]) {
            if (node.isLeaf) {
                // Remove key and value
                node.keys.splice(i, 1);
                node.values.splice(i, 1);
                return true;
            } else {
                // Internal node deletion (simplified - not fully balanced)
                return this._deleteFromNode(node.children[i + 1], key);
            }
        }

        if (node.isLeaf) {
            return false; // Key not found
        }

        // Recursively delete from child
        return this._deleteFromNode(node.children[i], key);
    }

    /**
     * Get index statistics
     */
    stats() {
        let nodeCount = 0;
        let leafCount = 0;
        let keyCount = 0;
        let maxDepth = 0;

        const traverse = (node, depth) => {
            nodeCount++;
            keyCount += node.keys.length;
            maxDepth = Math.max(maxDepth, depth);

            if (node.isLeaf) {
                leafCount++;
            } else {
                for (const child of node.children) {
                    traverse(child, depth + 1);
                }
            }
        };

        traverse(this.root, 0);

        return {
            name: this.name,
            keyField: this.keyField,
            nodeCount,
            leafCount,
            keyCount,
            maxDepth,
            order: this.order
        };
    }

    /**
     * Clear the index
     */
    clear() {
        this.root = new BTreeNode(true);
    }
}

module.exports = BTreeIndex;
