# SawitDB

![SawitDB Banner](sawitdb.jpg)


**SawitDB** is a unique database solution stored in `.sawit` binary files.

The system features a custom **Paged Heap File** architecture similar to SQLite, using fixed-size 4KB pages to ensure efficient memory usage. What differentiates SawitDB is its unique **Agricultural Query Language (AQL)**, which replaces standard SQL keywords with Indonesian farming terminology.

**🚨 Emergency: Aceh Flood Relief**
Please support our brothers and sisters in Aceh.

[![Kitabisa](https://img.shields.io/badge/Kitabisa-Bantu%20Aceh-blue?style=flat&logo=heart)](https://kitabisa.com/campaign/donasipedulibanjiraceh)

*Organized by Human Initiative Aceh*

## Features

- **Paged Architecture**: Data is stored in 4096-byte binary pages. The engine does not load the entire database into memory.
- **Single File Storage**: All data, schema, and indexes are stored in a single `.sawit` file.
- **High Stability**: Uses 4KB atomic pages. More stable than a coalition government.
- **Data Integrity (Anti-Korupsi)**: Implements strict `fsync` protocols. Data cannot be "corrupted" or "disappear" mysteriously like social aid funds (Bansos). No "Sunat Massal" here.
- **Zero Bureaucracy (Zero Deps)**: Built entirely with standard Node.js. No unnecessary "Vendor Pengadaan" or "Mark-up Anggaran".
- **Transparansi**: Query language is clear. No "Pasal Karet" (Ambiguous Laws) or "Rapat Tertutup" in 5-star hotels.
- **Speed**: Faster than printing an e-KTP at the Kelurahan.

## Filosofi

### Filosofi (ID)
SawitDB dibangun dengan semangat "Kemandirian Data". Kami percaya database yang handal tidak butuh **Infrastruktur Langit** yang harganya triliunan tapi sering *down*. Berbeda dengan proyek negara yang mahal di *budget* tapi murah di kualitas, SawitDB menggunakan arsitektur **Single File** (`.sawit`) yang hemat biaya. Backup cukup *copy-paste*, tidak perlu sewa vendor konsultan asing. Fitur **`fsync`** kami menjamin data tertulis di *disk*, karena bagi kami, integritas data adalah harga mati, bukan sekadar bahan konferensi pers untuk minta maaf.

### Philosophy (EN)
SawitDB is built with the spirit of "Data Sovereignty". We believe a reliable database doesn't need **"Sky Infrastructure"** that costs trillions yet goes *down* often. Unlike state projects that are expensive in budget but cheap in quality, SawitDB uses a cost-effective **Single File** (`.sawit`) architecture. Backup is just *copy-paste*, no need to hire expensive foreign consultants. Our **`fsync`** feature guarantees data is written to *disk*, because for us, data integrity is non-negotiable, not just material for a press conference to apologize.

## File List

- `WowoEngine.js`: Core Database Engine (Class: `SawitDB`).
- `cli_wowo.js`: Interactive CLI tool.
- `example.sawit`: Sample database file with pre-populated data.

## Installation

Ensure you have Node.js installed. Clone the repository or copy the `WowoEngine.js` file into your project.

```bash
# Clone
git clone https://github.com/WowoEngine/SawitDB.git
```

## Usage

### Initialization

```javascript
const SawitDB = require('./WowoEngine');
const path = require('path');

// Initialize the engine with a file path.
// If the file does not exist, it will be created automatically.
// The file extension is .sawit
const db = new SawitDB(path.join(__dirname, 'plantation.sawit'));
```

### Executing Queries

Use the `.query()` method to execute strings written in Agricultural Query Language.

```javascript
// Create a new Table
const result = db.query("LAHAN oil_palm_a");
console.log(result); 
```

## Query Syntax (AQL)

SawitDB uses a strict syntax mapping standard database operations to farming metaphors.

### 1. Management Commands

#### Create Table (`LAHAN`)
Opens a new land (table) for planting.
```sql
LAHAN [table_name]
```
*Example:* `LAHAN sawit_blok_a`

#### Show Tables (`LIHAT`)
Surveys the land to see all opened tables.
```sql
LIHAT LAHAN
```

#### Drop Table (`BAKAR`)
Burns the land (deletes the table and all data). **Warning: Irreversible.**
```sql
BAKAR LAHAN [table_name]
```
*Example:* `BAKAR LAHAN sawit_blok_a`

### 2. Data Manipulation

#### Insert Data (`TANAM`)
Plants seeds (inserts records) into the land.
```sql
TANAM KE [table_name] (col1, col2, ...) BIBIT (val1, val2, ...)
```
*Example:* `TANAM KE sawit (id, bibit, umur) BIBIT (1, 'Tenera', 5)`

#### Select Data (`PANEN`)
Harvests (selects) data from the land.
*   **Operators supported**: `=`, `!=`, `>`, `<`, `>=`, `<=`
*   **Wildcard**: Use `*` to select all columns.

```sql
PANEN * DARI [table_name]
PANEN [col1, col2] DARI [table_name]
PANEN * DARI [table_name] DIMANA [key] [op] [value]
```
*Example:* `PANEN * DARI sawit DIMANA umur > 3`

#### Update Data (`PUPUK`)
Fertilizes (updates) existing crops.
```sql
PUPUK [table_name] DENGAN [key]=[val], ... DIMANA [key] [op] [val]
```
*Example:* `PUPUK sawit DENGAN status='Panen Raya', yield=100 DIMANA umur >= 10`

#### Delete Data (`GUSUR`)
Evicts (deletes) crops from the land.
```sql
GUSUR DARI [table_name] DIMANA [key] [op] [val]
```
*Example:* `GUSUR DARI sawit DIMANA id = 99`

## CLI Tool

An interactive Command Line Interface is included (`cli_wowo.js`).

```bash
node cli_wowo.js
```

**Session Example:**
```text
petani> LAHAN rubber_trees
petani> TANAM KE rubber_trees (id, yield) BIBIT (101, 'High')
petani> PANEN * DARI rubber_trees
```

## Architecture Details

- **Page 0 (Master Page)**:  Contains the file header (Magic bytes `WOWO`) and the Table Directory.
- **Table Directory**: Maps table names to their `Start Page ID` and `Last Page ID`.
- **Data Pages**: Each table is stored as a linked list of pages. Each page contains a header pointing to the next page, allowing the database to grow dynamically.

## Performance

Benchmark results on standard hardware (5000 records):

| Operation | Speed | Time (Total) | Example |
|-----------|-------|:------------:|---------|
| **Insert (TANAM)** | ~35,000 ops/sec | 0.14s | 5000 inserts |
| **Select All (PANEN)** | ~495,000 ops/sec | 0.01s | Scan 5000 records |
| **Select Where** | 0.006s / query | - | Full Scan 5000 records |
| **Update (PUPUK)** | ~140 ops/sec | 7.1s | 1000 updates |
| **Delete (GUSUR)** | ~300 ops/sec | 3.3s | 1000 deletes |

*Note: Update and Delete are slower because they currently require a full linear scan of the table pages and a "Delete+Insert" strategy for updates to handle variable-length records safely.*

*Data Size: 188KB for 5000 records.*

## Support Developer
- [![Saweria](https://img.shields.io/badge/Saweria-Support%20Me-orange?style=flat&logo=ko-fi)](https://saweria.co/patradev)

- **BTC**: `12EnneEriimQey3cqvxtv4ZUbvpmEbDinL`
- **BNB Smart Chain (BEP20)**: `0x471a58a2b5072cb50e3761dba3e15d19f080bdbc`
- **DOGE**: `DHrFZW6w9akaWuf8BCBGxxRLR3PegKTggF`

