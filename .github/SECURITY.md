# SECURITY POLICY

SawitDB dibangun dengan fokus pada integritas data, konsistensi transaksi, dan kontrol akses berbasis role (POLRI).
Setiap kerentanan akan ditangani secara terstruktur dan bertanggung jawab.

---

## SUPPORTED VERSIONS

| Version | Status      |
| ------- | ----------- |
| 3.0.x   | Supported   |
| < 3.0   | Unsupported |

Catatan:

* Versi 3.0 menggunakan WAL (Write-Ahead Logging), RBAC, dan Network Mode
* Laporan untuk versi lama tetap diterima namun tidak dijamin diperbaiki

---

## REPORTING A VULNERABILITY

Jangan publikasikan kerentanan secara langsung.

Laporkan melalui:

* GitHub Security Advisories 
* Atau melalui issue 

### Informasi wajib:

1. Deskripsi kerentanan
2. Dampak (impact)
3. Langkah reproduksi
4. Proof of Concept (PoC)
5. Versi SawitDB
6. Environment:

   * OS
   * Node.js version
   * Mode (Single / Server / Cluster)

---

## SECURITY TESTING EXAMPLES (POC)

Berikut contoh pengujian untuk mengidentifikasi potensi kerentanan.
Semua testing wajib dilakukan secara lokal atau dengan izin.

---

### Authorization Bypass 

```javascript
const { SawitClient } = require("@wowoengine/sawitdb");

(async () => {
  const client = new SawitClient("sawitdb://127.0.0.1:7878");

  await client.connect("guest", "guest123");

  try {
    const result = await client.query(
      "SELECT * FROM users WHERE role = 'admin'"
    );

    console.log("UNEXPECTED DATA:", result);
  } catch (err) {
    console.error("EXPECTED ERROR:", err.message);
  }
})();
```

---

### Query Injection (Parser Edge Case)

```javascript
const payload = "admin' OR '1'='1";

const query = `
  SELECT * FROM users
  WHERE username = '${payload}'
`;

console.log(query);

const result = db.query(query);
console.log(result);
```

---

### WAL Consistency (Crash Test)

```javascript
const db = new SawitDB("./test.sawit");

db.query("CREATE TABLE logs");
db.query("BEGIN TRANSACTION");

for (let i = 0; i < 1000; i++) {
  db.query(`INSERT INTO logs (msg) VALUES ('log-${i}')`);
}

// Force crash
process.exit(1);
```


```javascript
const db = new SawitDB("./test.sawit");

const result = db.query("SELECT COUNT(*) FROM logs");

console.log(result);
```


---

### Race Condition (Worker Thread)

```javascript
const { SawitClient } = require("@wowoengine/sawitdb");

(async () => {
  const client = new SawitClient("sawitdb://127.0.0.1:7878");

  await client.connect("admin", "admin123");

  const tasks = [];

  for (let i = 0; i < 50; i++) {
    tasks.push(
      client.query(
        "UPDATE products SET stock = stock + 1 WHERE id = 1"
      )
    );
  }

  await Promise.all(tasks);

  const result = await client.query(
    "SELECT stock FROM products WHERE id = 1"
  );

  console.log(result);
})();
```


---

### Memory Stress / Cache Abuse

```javascript
const db = new SawitDB("./stress.sawit");

db.query("CREATE TABLE bigdata");

for (let i = 0; i < 100000; i++) {
  db.query(`INSERT INTO bigdata (value) VALUES ('data-${i}')`);
}

const result = db.query("SELECT * FROM bigdata");

console.log("ROWS:", result.length);
```


---

## RESPONSE PROCESS

* Respon awal: 3–7 hari
* Validasi teknis
* Klasifikasi severity
* Patch development
* Security advisory / release

Semua laporan diverifikasi secara objektif.

---

## DISCLOSURE POLICY

* Tidak menyebarkan exploit sebelum patch
* Koordinasi dengan maintainer
* Mengikuti responsible disclosure

Pelapor dapat:

* Dicantumkan sebagai security contributor
* Atau tetap anonim

---

## SCOPE

### Termasuk:

* Authentication / authorization bypass
* RCE (Remote Code Execution)
* Query / parser exploit
* Data corruption (WAL / paging)
* Race condition
* Memory issue yang berdampak keamanan
* Network protocol abuse (sawitdb://)

### Tidak termasuk:

* Bug minor tanpa impact
* Misconfiguration user
* Issue performa
* Social engineering

---

## PROHIBITED ACTIONS

* Akses data tanpa izin
* Testing di server publik tanpa persetujuan
* Eksploitasi di luar ruang lingkup penelitian

Pengujian harus bertanggung jawab dan terkendali.


## ACKNOWLEDGEMENT

Kontributor yang melaporkan kerentanan valid dapat dicantumkan sebagai security contributor.

---

