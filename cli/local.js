const readline = require('readline');
const SawitDB = require('../src/WowoEngine');
const path = require('path');
const fs = require('fs');

let currentDbName = 'example';
const dataDir = __dirname; // Default to CLI dir or configurable
let dbPath = path.join(dataDir, `${currentDbName}.sawit`);
let db = new SawitDB(dbPath);

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

console.log("--- SawitDB (Local Mode) ---");
console.log("Perintah (Tani / SQL):");
console.log("  LAHAN [nama] | CREATE TABLE [name]");
console.log("  LIHAT LAHAN | SHOW TABLES");
console.log("\n  MANIPULASI DATA (DML):");
console.log("  TANAM KE [table] ... BIBIT ... | INSERT INTO ... VALUES ...");
console.log("  PANEN [cols] DARI [table]      | SELECT [cols] FROM [table]");
console.log("  ... DIMANA [cond]              | ... WHERE [cond]");
console.log("  ... URUTKAN BERDASARKAN [col]  | ... ORDER BY [col] [ASC/DESC]");
console.log("  ... HANYA [n] MULAI DARI [m]   | ... LIMIT [n] OFFSET [m]");
console.log("  ... KELOMPOK [col]             | ... GROUP BY [col]");
console.log("  ... DENGAN SYARAT [cond]       | ... HAVING [cond]");
console.log("  PANEN UNIK [col] DARI ...      | SELECT DISTINCT [col] FROM ...");
console.log("  PUPUK [table] DENGAN ...       | UPDATE [table] SET ...");
console.log("  GUSUR DARI [table]             | DELETE FROM [table]");
console.log("\n  RELASI & GABUNGAN (JOINS):");
console.log("  GABUNG [table] PADA [cond]     | INNER JOIN [table] ON [cond]");
console.log("  GABUNG KIRI [table] PADA ...   | LEFT JOIN [table] ON ...");
console.log("  GABUNG KANAN [table] PADA ...  | RIGHT JOIN [table] ON ...");
console.log("  GABUNG SILANG [table]          | CROSS JOIN [table]");
console.log("\n  LAIN-LAIN (MISC):");
console.log("  INDEKS [table] PADA [field]    | CREATE INDEX ON [table]([field])");
console.log("  HITUNG FUNC(field) DARI ...    | SELECT AGGREGATE(...) FROM ...");
console.log("  JELASKAN PANEN ...             | EXPLAIN SELECT ...");
console.log("\n  OPERATOR:");
console.log("  =, !=, >, <, >=, <=, LIKE, IN, NOT IN, BETWEEN, IS NULL");
console.log("\nManajemen Wilayah:");
console.log("  MASUK WILAYAH [nama]  - Pindah Database (USE)");
console.log("  BUKA WILAYAH [nama]   - Buat Database Baru (CREATE DATABASE)");
console.log("  LIHAT WILAYAH         - List Database (SHOW DATABASES)");
console.log("  BAKAR WILAYAH [nama]  - Hapus Database (DROP DATABASE)");
console.log("\nContoh:");
console.log("  TANAM KE sawit (id, bibit) BIBIT (1, 'Dura')");
console.log("  PANEN * DARI sawit DIMANA id > 0");
console.log("  HITUNG AVG(umur) DARI sawit KELOMPOK bibit");
console.log("  BAKAR LAHAN karet");

function switchDatabase(name) {
    try {
        if (db) {
            try { db.close(); } catch (e) { }
        }
        currentDbName = name;
        dbPath = path.join(dataDir, `${name}.sawit`);
        db = new SawitDB(dbPath);
        console.log(`\nBerhasil masuk ke wilayah '${name}'.`);
    } catch (e) {
        console.error(`Gagal masuk wilayah: ${e.message}`);
    }
}

function listDatabases() {
    const files = fs.readdirSync(dataDir).filter(f => f.endsWith('.sawit'));
    console.log("Daftar Wilayah:");
    files.forEach(f => console.log(`- ${f.replace('.sawit', '')}`));
}

function prompt() {
    rl.question(`${currentDbName}> `, (line) => {
        const cmd = line.trim();
        const upperCmd = cmd.toUpperCase();

        if (upperCmd === 'EXIT') {
            if (db) try { db.close(); } catch (e) { }
            rl.close();
            return;
        }

        if (upperCmd.startsWith('MASUK WILAYAH ') || upperCmd.startsWith('USE ')) {
            const parts = cmd.split(/\s+/);
            const name = parts[2] || parts[1]; // Handle USE [name] or MASUK WILAYAH [name]
            if (name) {
                switchDatabase(name);
            } else {
                console.log("Syntax: MASUK WILAYAH [nama]");
            }
            return prompt();
        }

        if (upperCmd.startsWith('BUKA WILAYAH ')) {
            const parts = cmd.split(/\s+/);
            const name = parts[2];
            if (name) {
                switchDatabase(name); // Buka defaults to open/create in generic engine
            } else {
                console.log("Syntax: BUKA WILAYAH [nama]");
            }
            return prompt();
        }

        if (upperCmd === 'LIHAT WILAYAH' || upperCmd === 'SHOW DATABASES') {
            listDatabases();
            return prompt();
        }

        if (cmd) {
            try {
                const result = db.query(cmd);
                if (typeof result === 'object') {
                    console.log(JSON.stringify(result, null, 2));
                } else {
                    console.log(result);
                }
            } catch (e) {
                console.error("Error:", e.message);
            }
        }
        prompt();
    });
}

prompt();
