const SawitDB = require('../src/WowoEngine');
const path = require('path');
const fs = require('fs');
const env = require('../src/modules/Env')

const event = require('./dbeventHandlerExample')

const dbPath = path.join(__dirname, 'example.sawit');
if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);

env.LoadEnv("../../.env.example")

const db = new SawitDB(dbPath,{dbevent:new event()});

console.log("Generating example.sawit...");

// 1. Create Tables
console.log(db.query("LAHAN sawit"));

// 2. Insert Data
console.log(db.query("TANAM KE sawit (id, bibit, umur) BIBIT (101, 'Dura', 2)"));
console.log(db.query("TANAM KE sawit (id, bibit, umur) BIBIT (102, 'Tenera', 5)"));
console.log(db.query("TANAM KE sawit (id, bibit, umur) BIBIT (103, 'Tenera', 1)"));

console.log("\n--- VERIFICATION TEST ---");
//console.log("Sawit:", JSON.stringify(db.query("PANEN * DARI sawit")));
console.log("Sawit:", JSON.stringify(db.query("PUPUK sawit DENGAN bibit='Dura' DIMANA bibit='Tenera' ")));
console.log("Sawit:", JSON.stringify(db.query("PANEN * DARI sawit")));
console.log("Sawit:", JSON.stringify(db.query("GUSUR DARI sawit DIMANA id=101")));
console.log("Sawit:", JSON.stringify(db.query("BAKAR LAHAN sawit")));
