const net = require('net');
const { URL } = require('url');

/**
 * SawitDB Client - Connect to SawitDB Server
 * Usage: sawitdb://[username:password@]host:port/database
 */
class SawitClient {
    constructor(connectionString) {
        this.connectionString = connectionString;
        this.socket = null;
        this.connected = false;
        this.authenticated = false;
        this.currentDatabase = null;
        this.buffer = '';
        this.pendingRequests = [];
        this.requestId = 0;

        this._parseConnectionString(connectionString);
    }

    _parseConnectionString(connStr) {
        // Parse sawitdb://[user:pass@]host:port/database
        const url = connStr.replace('sawitdb://', 'http://'); // Trick to use URL parser
        const parsed = new URL(url);

        this.host = parsed.hostname || 'localhost';
        this.port = parseInt(parsed.port) || 7878;
        this.database = parsed.pathname.replace('/', '') || null;
        this.username = parsed.username || null;
        this.password = parsed.password || null;
    }

    async connect() {
        return new Promise((resolve, reject) => {
            this.socket = net.createConnection({ host: this.host, port: this.port }, () => {
                console.log(`[Client] Connected to ${this.host}:${this.port}`);
                this.connected = true;
            });

            this.socket.on('data', (data) => {
                this._handleData(data);
            });

            this.socket.on('end', () => {
                console.log('[Client] Disconnected from server');
                this.connected = false;
            });

            this.socket.on('error', (err) => {
                console.error('[Client] Socket error:', err.message);
                this.connected = false;
                reject(err);
            });

            // Wait for welcome message
            const welcomeHandler = (response) => {
                if (response.type === 'welcome') {
                    console.log(`[Client] ${response.message} v${response.version}`);
                    this._initConnection().then(resolve).catch(reject);
                }
            };

            this.pendingRequests.push({ id: 'welcome', handler: welcomeHandler });
        });
    }

    async _initConnection() {
        // Authenticate if credentials provided
        if (this.username && this.password) {
            await this._authenticate();
        }

        // Select database if specified
        if (this.database) {
            await this.use(this.database);
        }
    }

    async _authenticate() {
        return new Promise((resolve, reject) => {
            this._sendRequest({
                type: 'auth',
                payload: {
                    username: this.username,
                    password: this.password
                }
            }, (response) => {
                if (response.type === 'auth_success') {
                    this.authenticated = true;
                    console.log('[Client] Authenticated successfully');
                    resolve();
                } else if (response.type === 'error') {
                    reject(new Error(response.error));
                }
            });
        });
    }

    async use(database) {
        return new Promise((resolve, reject) => {
            this._sendRequest({
                type: 'use',
                payload: { database }
            }, (response) => {
                if (response.type === 'use_success') {
                    this.currentDatabase = database;
                    console.log(`[Client] Using database '${database}'`);
                    resolve(response);
                } else if (response.type === 'error') {
                    reject(new Error(response.error));
                } else {
                    resolve(response);
                }
            });
        });
    }

    async query(queryString, params = []) {
        if (!this.connected) {
            throw new Error('Not connected to server');
        }

        return new Promise((resolve, reject) => {
            this._sendRequest({
                type: 'query',
                payload: {
                    query: queryString,
                    params: params
                }
            }, (response) => {
                if (response.type === 'query_result') {
                    resolve(response.result);
                } else if (response.type === 'error') {
                    reject(new Error(response.error));
                } else {
                    resolve(response);
                }
            });
        });
    }

    async listDatabases() {
        return new Promise((resolve, reject) => {
            this._sendRequest({
                type: 'list_databases',
                payload: {}
            }, (response) => {
                if (response.type === 'database_list') {
                    resolve(response.databases);
                } else if (response.type === 'error') {
                    reject(new Error(response.error));
                } else {
                    resolve(response);
                }
            });
        });
    }

    async dropDatabase(database) {
        return new Promise((resolve, reject) => {
            this._sendRequest({
                type: 'drop_database',
                payload: { database }
            }, (response) => {
                if (response.type === 'drop_success') {
                    if (this.currentDatabase === database) {
                        this.currentDatabase = null;
                    }
                    resolve(response.message);
                } else if (response.type === 'error') {
                    reject(new Error(response.error));
                } else {
                    resolve(response);
                }
            });
        });
    }

    async ping() {
        return new Promise((resolve, reject) => {
            const start = Date.now();
            this._sendRequest({
                type: 'ping',
                payload: {}
            }, (response) => {
                if (response.type === 'pong') {
                    const latency = Date.now() - start;
                    resolve({ latency, serverTime: response.timestamp });
                } else {
                    resolve(response);
                }
            });
        });
    }

    async stats() {
        return new Promise((resolve, reject) => {
            this._sendRequest({
                type: 'stats',
                payload: {}
            }, (response) => {
                if (response.type === 'stats') {
                    resolve(response.stats);
                } else if (response.type === 'error') {
                    reject(new Error(response.error));
                } else {
                    resolve(response);
                }
            });
        });
    }

    _sendRequest(request, callback) {
        const id = ++this.requestId;
        this.pendingRequests.push({ id, handler: callback });

        try {
            this.socket.write(JSON.stringify(request) + '\n');
        } catch (err) {
            console.error('[Client] Failed to send request:', err.message);
            const idx = this.pendingRequests.findIndex(r => r.id === id);
            if (idx !== -1) {
                this.pendingRequests.splice(idx, 1);
            }
            throw err;
        }
    }

    _handleData(data) {
        this.buffer += data.toString();

        let newlineIndex;
        while ((newlineIndex = this.buffer.indexOf('\n')) !== -1) {
            const message = this.buffer.substring(0, newlineIndex);
            this.buffer = this.buffer.substring(newlineIndex + 1);

            try {
                const response = JSON.parse(message);
                this._handleResponse(response);
            } catch (err) {
                console.error('[Client] Failed to parse response:', err.message);
            }
        }
    }

    _handleResponse(response) {
        if (this.pendingRequests.length > 0) {
            const request = this.pendingRequests.shift();
            if (request.handler) {
                request.handler(response);
            }
        }
    }

    disconnect() {
        if (this.socket) {
            this.socket.end();
            this.socket = null;
            this.connected = false;
        }
    }
}

module.exports = SawitClient;
