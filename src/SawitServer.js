const net = require('net');
const SawitDB = require('./WowoEngine');
const path = require('path');
const fs = require('fs');

/**
 * SawitDB Server - Network Database Server
 * Supports sawitdb:// protocol connections
 */
class SawitServer {
    constructor(config = {}) {
        // Validate and set configuration
        this.port = this._validatePort(config.port || 7878);
        this.host = config.host || '0.0.0.0';
        this.dataDir = config.dataDir || path.join(__dirname, '../data');
        this.databases = new Map(); // Map of database name -> SawitDB instance
        this.clients = new Set();
        this.server = null;
        this.auth = config.auth || null; // { username: 'password' }
        this.maxConnections = config.maxConnections || 100;
        this.queryTimeout = config.queryTimeout || 30000; // 30 seconds
        this.logLevel = config.logLevel || 'info'; // 'debug', 'info', 'warn', 'error'
        this.stats = {
            totalConnections: 0,
            activeConnections: 0,
            totalQueries: 0,
            errors: 0,
            startTime: Date.now()
        };

        // Ensure data directory exists
        if (!fs.existsSync(this.dataDir)) {
            fs.mkdirSync(this.dataDir, { recursive: true });
        }

        this._log('info', `Data directory: ${this.dataDir}`);
        this._log('info', `Max connections: ${this.maxConnections}`);
    }

    _validatePort(port) {
        const p = parseInt(port);
        if (isNaN(p) || p < 1 || p > 65535) {
            throw new Error(`Invalid port: ${port}. Must be between 1-65535`);
        }
        return p;
    }

    _log(level, message, data = null) {
        const levels = { debug: 0, info: 1, warn: 2, error: 3 };
        const currentLevel = levels[this.logLevel] || 1;
        const msgLevel = levels[level] || 1;

        if (msgLevel >= currentLevel) {
            const timestamp = new Date().toISOString();
            const prefix = level.toUpperCase().padEnd(5);
            console.log(`[${timestamp}] [${prefix}] ${message}`);
            if (data && this.logLevel === 'debug') {
                console.log(JSON.stringify(data, null, 2));
            }
        }
    }

    start() {
        this.server = net.createServer((socket) => this._handleConnection(socket));

        this.server.listen(this.port, this.host, () => {
            console.log(`╔══════════════════════════════════════════════════╗`);
            console.log(`║   🌴 SawitDB Server - Tani Edition v2.0        ║`);
            console.log(`╚══════════════════════════════════════════════════╝`);
            console.log(`[Server] Listening on ${this.host}:${this.port}`);
            console.log(`[Server] Protocol: sawitdb://${this.host}:${this.port}/[database]`);
            console.log(`[Server] Ready to accept petani connections...`);
        });

        this.server.on('error', (err) => {
            console.error('[Server] Error:', err.message);
        });
    }

    stop() {
        console.log('[Server] Shutting down...');

        // Close all client connections
        for (const client of this.clients) {
            client.destroy();
        }

        // Close server
        if (this.server) {
            this.server.close(() => {
                console.log('[Server] Server stopped.');
            });
        }
    }

    _handleConnection(socket) {
        const clientId = `${socket.remoteAddress}:${socket.remotePort}`;

        // Check connection limit
        if (this.clients.size >= this.maxConnections) {
            this._log('warn', `Connection limit reached. Rejecting ${clientId}`);
            socket.write(JSON.stringify({
                type: 'error',
                error: 'Server connection limit reached. Please try again later.'
            }) + '\n');
            socket.end();
            return;
        }

        this._log('info', `Client connected: ${clientId}`);
        this.stats.totalConnections++;
        this.stats.activeConnections++;

        this.clients.add(socket);
        socket.clientId = clientId;
        socket.connectedAt = Date.now();

        let authenticated = !this.auth; // If no auth required, auto-authenticate
        let currentDatabase = null;
        let buffer = '';

        socket.on('data', (data) => {
            buffer += data.toString();

            // Prevent buffer overflow attacks
            if (buffer.length > 1048576) { // 1MB limit
                this._log('warn', `Buffer overflow attempt from ${clientId}`);
                this._sendError(socket, 'Request too large');
                socket.destroy();
                return;
            }

            // Process complete messages (delimited by newline)
            let newlineIndex;
            while ((newlineIndex = buffer.indexOf('\n')) !== -1) {
                const message = buffer.substring(0, newlineIndex);
                buffer = buffer.substring(newlineIndex + 1);

                try {
                    const request = JSON.parse(message);
                    this._log('debug', `Request from ${clientId}`, request);

                    // Handle with timeout
                    this._handleRequest(socket, request, {
                        authenticated,
                        currentDatabase,
                        setAuth: (val) => { authenticated = val; },
                        setDatabase: (db) => { currentDatabase = db; }
                    });
                } catch (err) {
                    this._log('error', `Invalid request from ${clientId}: ${err.message}`);
                    this.stats.errors++;
                    this._sendError(socket, `Invalid request format: ${err.message}`);
                }
            }
        });

        socket.on('end', () => {
            const duration = Date.now() - (socket.connectedAt || Date.now());
            this._log('info', `Client disconnected: ${clientId} (duration: ${duration}ms)`);
            this.clients.delete(socket);
            this.stats.activeConnections--;
        });

        socket.on('error', (err) => {
            this._log('error', `Client error: ${clientId} - ${err.message}`);
            this.clients.delete(socket);
            this.stats.activeConnections--;
            this.stats.errors++;
        });

        socket.on('timeout', () => {
            this._log('warn', `Client timeout: ${clientId}`);
            socket.destroy();
        });

        // Set socket timeout
        socket.setTimeout(this.queryTimeout);

        // Send welcome message
        this._sendResponse(socket, {
            type: 'welcome',
            message: 'SawitDB Server - Tani Edition',
            version: '2.0',
            protocol: 'sawitdb'
        });
    }

    _handleRequest(socket, request, context) {
        const { type, payload } = request;

        // Authentication check
        if (this.auth && !context.authenticated && type !== 'auth') {
            return this._sendError(socket, 'Authentication required');
        }

        switch (type) {
            case 'auth':
                this._handleAuth(socket, payload, context);
                break;

            case 'use':
                this._handleUseDatabase(socket, payload, context);
                break;

            case 'query':
                this._handleQuery(socket, payload, context);
                break;

            case 'ping':
                this._sendResponse(socket, { type: 'pong', timestamp: Date.now() });
                break;

            case 'list_databases':
                this._handleListDatabases(socket);
                break;

            case 'drop_database':
                this._handleDropDatabase(socket, payload, context);
                break;

            case 'stats':
                this._handleStats(socket);
                break;

            default:
                this._sendError(socket, `Unknown request type: ${type}`);
        }
    }

    _handleStats(socket) {
        const uptime = Date.now() - this.stats.startTime;
        const stats = {
            ...this.stats,
            uptime,
            uptimeFormatted: this._formatUptime(uptime),
            databases: this.databases.size,
            memoryUsage: process.memoryUsage()
        };

        this._sendResponse(socket, {
            type: 'stats',
            stats
        });
    }

    _formatUptime(ms) {
        const seconds = Math.floor(ms / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);
        const days = Math.floor(hours / 24);

        if (days > 0) return `${days}d ${hours % 24}h`;
        if (hours > 0) return `${hours}h ${minutes % 60}m`;
        if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
        return `${seconds}s`;
    }

    _handleAuth(socket, payload, context) {
        const { username, password } = payload;

        if (!this.auth) {
            context.setAuth(true);
            return this._sendResponse(socket, { type: 'auth_success', message: 'No authentication required' });
        }

        if (this.auth[username] === password) {
            context.setAuth(true);
            this._sendResponse(socket, { type: 'auth_success', message: 'Authentication successful' });
        } else {
            this._sendError(socket, 'Invalid credentials');
        }
    }

    _handleUseDatabase(socket, payload, context) {
        const { database } = payload;

        if (!database || typeof database !== 'string') {
            return this._sendError(socket, 'Invalid database name');
        }

        // Validate database name (alphanumeric, underscore, dash)
        if (!/^[a-zA-Z0-9_-]+$/.test(database)) {
            return this._sendError(socket, 'Database name can only contain letters, numbers, underscore, and dash');
        }

        try {
            const db = this._getOrCreateDatabase(database);
            context.setDatabase(database);
            this._sendResponse(socket, {
                type: 'use_success',
                database,
                message: `Switched to database '${database}'`
            });
        } catch (err) {
            this._sendError(socket, `Failed to use database: ${err.message}`);
        }
    }

    _handleQuery(socket, payload, context) {
        const { query } = payload;
        const startTime = Date.now();

        if (!context.currentDatabase) {
            return this._sendError(socket, 'No database selected. Use USE command first.');
        }

        try {
            const db = this._getOrCreateDatabase(context.currentDatabase);
            const result = db.query(query);
            const duration = Date.now() - startTime;

            this.stats.totalQueries++;
            this._log('debug', `Query executed in ${duration}ms: ${query.substring(0, 50)}...`);

            this._sendResponse(socket, {
                type: 'query_result',
                result,
                query,
                executionTime: duration
            });
        } catch (err) {
            this._log('error', `Query failed: ${err.message} - Query: ${query}`);
            this.stats.errors++;
            this._sendError(socket, `Query error: ${err.message}`);
        }
    }

    _handleListDatabases(socket) {
        try {
            const databases = fs.readdirSync(this.dataDir)
                .filter(file => file.endsWith('.sawit'))
                .map(file => file.replace('.sawit', ''));

            this._sendResponse(socket, {
                type: 'database_list',
                databases,
                count: databases.length
            });
        } catch (err) {
            this._sendError(socket, `Failed to list databases: ${err.message}`);
        }
    }

    _handleDropDatabase(socket, payload, context) {
        const { database } = payload;

        if (!database) {
            return this._sendError(socket, 'Database name required');
        }

        try {
            const dbPath = path.join(this.dataDir, `${database}.sawit`);

            if (!fs.existsSync(dbPath)) {
                return this._sendError(socket, `Database '${database}' does not exist`);
            }

            // Close database if open
            if (this.databases.has(database)) {
                this.databases.delete(database);
            }

            // Delete file
            fs.unlinkSync(dbPath);

            // Clear current database if it was dropped
            if (context.currentDatabase === database) {
                context.setDatabase(null);
            }

            this._sendResponse(socket, {
                type: 'drop_success',
                database,
                message: `Database '${database}' has been burned (dropped)`
            });
        } catch (err) {
            this._sendError(socket, `Failed to drop database: ${err.message}`);
        }
    }

    _getOrCreateDatabase(name) {
        if (!this.databases.has(name)) {
            const dbPath = path.join(this.dataDir, `${name}.sawit`);
            const db = new SawitDB(dbPath);
            this.databases.set(name, db);
        }
        return this.databases.get(name);
    }

    _sendResponse(socket, data) {
        try {
            socket.write(JSON.stringify(data) + '\n');
        } catch (err) {
            console.error('[Server] Failed to send response:', err.message);
        }
    }

    _sendError(socket, message) {
        this._sendResponse(socket, { type: 'error', error: message });
    }
}

module.exports = SawitServer;

// Allow running as standalone server
if (require.main === module) {
    const server = new SawitServer({
        port: process.env.SAWIT_PORT || 7878,
        host: process.env.SAWIT_HOST || '0.0.0.0',
        dataDir: process.env.SAWIT_DATA_DIR || path.join(__dirname, '../data'),
        // Optional auth: { username: 'petani', password: 'sawit123' }
    });

    server.start();

    // Graceful shutdown
    process.on('SIGINT', () => {
        console.log('\n[Server] Received SIGINT, shutting down gracefully...');
        server.stop();
        process.exit(0);
    });

    process.on('SIGTERM', () => {
        console.log('\n[Server] Received SIGTERM, shutting down gracefully...');
        server.stop();
        process.exit(0);
    });
}
