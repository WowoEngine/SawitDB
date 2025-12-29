#!/usr/bin/env node

/**
 * Interactive CLI Client for SawitDB
 * 
 * Usage:
 *   node cli_client.js [connection_string]
 *   
 * Example:
 *   node cli_client.js sawitdb://localhost:7878/mydb
 *   node cli_client.js sawitdb://user:pass@localhost:7878/mydb
 */

const readline = require('readline');
const SawitClient = require('../src/SawitClient');

const connStr = process.argv[2] || 'sawitdb://localhost:7878/default';

console.log('╔══════════════════════════════════════════════════╗');
console.log('║   🌴 SawitDB Client - Interactive CLI          ║');
console.log('╚══════════════════════════════════════════════════╝');
console.log('');
console.log(`Connecting to: ${connStr}`);
console.log('');

const client = new SawitClient(connStr);
let rl;

async function init() {
    try {
        await client.connect();
        console.log('✓ Connected to SawitDB server\n');
        console.log('Commands:');
        console.log('  LAHAN [nama]              - Create table');
        console.log('  LIHAT LAHAN              - Show tables');
        console.log('  LIHAT INDEKS [table]     - Show indexes');
        console.log('  TANAM KE ...             - Insert data');
        console.log('  PANEN ... DARI ...       - Select data');
        console.log('  PUPUK ... DENGAN ...     - Update data');
        console.log('  GUSUR DARI ...           - Delete data');
        console.log('  BAKAR LAHAN [nama]       - Drop table');
        console.log('  INDEKS [table] PADA [field] - Create index');
        console.log('  HITUNG FUNC(...) DARI ... - Aggregate');
        console.log('  .databases               - List all databases');
        console.log('  .use [db]                - Switch database');
        console.log('  .ping                    - Ping server');
        console.log('  .help                    - Show this help');
        console.log('  EXIT                     - Disconnect and exit');
        console.log('');

        rl = readline.createInterface({
            input: process.stdin,
            output: process.stdout
        });

        prompt();
    } catch (error) {
        console.error('Failed to connect:', error.message);
        process.exit(1);
    }
}

function prompt() {
    rl.question(`${client.currentDatabase || 'none'}> `, async (line) => {
        const cmd = line.trim();

        if (!cmd) {
            return prompt();
        }

        if (cmd.toUpperCase() === 'EXIT') {
            console.log('\nDisconnecting...');
            client.disconnect();
            rl.close();
            process.exit(0);
            return;
        }

        // Special commands
        if (cmd.startsWith('.')) {
            await handleSpecialCommand(cmd);
            return prompt();
        }

        // Regular query
        try {
            const result = await client.query(cmd);
            if (typeof result === 'object') {
                console.log(JSON.stringify(result, null, 2));
            } else {
                console.log(result);
            }
        } catch (error) {
            console.error('Error:', error.message);
        }

        prompt();
    });
}

async function handleSpecialCommand(cmd) {
    const parts = cmd.split(' ');
    const command = parts[0];

    try {
        switch (command) {
            case '.databases':
                const dbs = await client.listDatabases();
                console.log('Databases:', dbs.join(', '));
                break;

            case '.use':
                if (parts.length < 2) {
                    console.log('Usage: .use [database]');
                } else {
                    await client.use(parts[1]);
                }
                break;

            case '.ping':
                const ping = await client.ping();
                console.log(`Pong! Latency: ${ping.latency}ms`);
                break;

            case '.stats':
                const stats = await client.stats();
                console.log('\nServer Statistics:');
                console.log(`  Uptime: ${stats.uptimeFormatted}`);
                console.log(`  Total Connections: ${stats.totalConnections}`);
                console.log(`  Active Connections: ${stats.activeConnections}`);
                console.log(`  Total Queries: ${stats.totalQueries}`);
                console.log(`  Errors: ${stats.errors}`);
                console.log(`  Databases: ${stats.databases}`);
                console.log(`  Memory: ${Math.round(stats.memoryUsage.heapUsed / 1024 / 1024)}MB / ${Math.round(stats.memoryUsage.heapTotal / 1024 / 1024)}MB`);
                break;

            case '.help':
                console.log('\nAvailable commands:');
                console.log('  .databases    - List all databases');
                console.log('  .use [db]     - Switch to database');
                console.log('  .ping         - Test connection');
                console.log('  .stats        - Show server statistics');
                console.log('  .help         - Show this help');
                console.log('');
                break;

            default:
                console.log(`Unknown command: ${command}`);
                console.log('Type .help for available commands');
        }
    } catch (error) {
        console.error('Error:', error.message);
    }
}

// Start
init();

// Handle exit
process.on('SIGINT', () => {
    console.log('\n\nDisconnecting...');
    client.disconnect();
    if (rl) rl.close();
    process.exit(0);
});
