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
        console.log('Commands (Tani / SQL):');
        console.log('  LAHAN [nama] | CREATE TABLE [name]         - Create table');
        console.log('  LIHAT LAHAN | SHOW TABLES                  - Show tables');
        console.log('  LIHAT INDEKS [table] | SHOW INDEXES [table]- Show indexes');
        console.log('  MANIPULASI DATA (DML):');
        console.log('  TANAM KE [table] ... BIBIT ...             - Insert Data');
        console.log('  PANEN [cols] DARI [table] DIMANA ...       - Select Data');
        console.log('  ... URUTKAN BERDASARKAN [cal] [NAIK|TURUN] - Order By');
        console.log('  ... HANYA [n] MULAI DARI [m]               - Limit & Offset');
        console.log('  ... KELOMPOK [col] DENGAN SYARAT ...       - Group By & Having');
        console.log('  PANEN UNIK [col] DARI ...                  - Select Distinct');
        console.log('  PUPUK [table] DENGAN ... DIMANA ...        - Update Data');
        console.log('  GUSUR DARI [table] DIMANA ...              - Delete Data');
        console.log('  HITUNG FUNC(field) DARI ...                - Aggregate (SUM, AVG, COUNT)');
        console.log('');
        console.log('  RELASI (JOINS):');
        console.log('  GABUNG [table] PADA ...                    - Inner Join');
        console.log('  GABUNG KIRI [table] PADA ...               - Left Join');
        console.log('  GABUNG KANAN [table] PADA ...              - Right Join');
        console.log('  GABUNG SILANG [table]                      - Cross Join');
        console.log('');
        console.log('  LAIN-LAIN:');
        console.log('  LAHAN [nama]                               - Create Table');
        console.log('  LIHAT LAHAN                                - Show Tables');
        console.log('  BAKAR LAHAN [table]                        - Drop Table');
        console.log('  INDEKS [table] PADA [field]                - Create Index');
        console.log('  JELASKAN PANEN ...                         - Explain Query');
        console.log('');
        console.log('  OPERATOR:');
        console.log('  =, !=, >, <, LIKE, IN, NOT IN, BETWEEN, IS NULL');
        console.log('');
        console.log('Special Commands:');
        console.log('  .databases               - List all databases');
        console.log('  .use [db]                - Switch database');
        console.log('  .ping                    - Ping server');
        console.log('  .stats                   - Server Statistics');
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

        // Multi-schema support: Intercept USE / MASUK WILAYAH to update client state
        const upperCmd = cmd.toUpperCase();
        if (upperCmd.startsWith('USE ') || upperCmd.startsWith('MASUK WILAYAH ')) {
            const parts = cmd.trim().split(/\s+/);
            let dbName = null;

            if (upperCmd.startsWith('USE ')) {
                if (parts.length < 2) {
                    console.log('Syntax: USE [database]');
                    return prompt();
                }
                dbName = parts[1];
            } else {
                if (parts.length < 3) {
                    console.log('Syntax: MASUK WILAYAH [nama_wilayah]');
                    return prompt();
                }
                dbName = parts[2];
            }

            try {
                await client.use(dbName); // This updates client.currentDatabase
                // server sends 'use_success' message which client.use logs? 
                // client.use logs "[Client] Using database..."
                // We can add extra user feedback if needed, usually client logs are enough or suppressed.
                // SawitClient.use does console.log.
            } catch (error) {
                console.error('Error switching database:', error.message);
            }
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
