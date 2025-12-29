const readline = require('readline');
const SawitDB = require('../src/WowoEngine');
const path = require('path');

const dbPath = path.join(__dirname, 'example.sawit');
const db = new SawitDB(dbPath);

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

console.log("--- WOWODB TANI EDITION V2 (SQL-Like) ---");
console.log("Perintah:");
console.log("  LAHAN [nama_kebun]");
console.log("  LIHAT LAHAN");
console.log("  TANAM KE [kebun] (col,...) BIBIT (val,...)");
console.log("  PANEN * DARI [kebun]");
console.log("  PANEN ... DIMANA col [=,>,<,!=] val");
console.log("  GUSUR DARI [kebun] DIMANA col = val");
console.log("  PUPUK [kebun] DENGAN col=val ... DIMANA col = val");
console.log("  BAKAR LAHAN [kebun]");
console.log("\nContoh:");
console.log("  TANAM KE sawit (id, bibit) BIBIT (1, 'Dura')");
console.log("  PANEN * DARI sawit DIMANA id > 0");
console.log("  BAKAR LAHAN karet");
console.log("Ketik 'EXIT' untuk pulang.");

function prompt() {
    rl.question('petani> ', (line) => {
        const cmd = line.trim();
        if (cmd.toUpperCase() === 'EXIT') {
            rl.close();
            return;
        }

        if (cmd) {
            const result = db.query(cmd);
            if (typeof result === 'object') {
                console.log(JSON.stringify(result, null, 2));
            } else {
                console.log(result);
            }
        }
        prompt();
    });
}

prompt();
