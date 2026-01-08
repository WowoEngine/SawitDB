const EventEmitter = require('events');

/**
 * Modul Kemitraan Sawit (Clustering & Replication)
 * 
 * Filosofi:
 * Mengadopsi Pola Kemitraan Inti-Plasma.
 * 
 * - KEBUN INTI: Node Utama (Master) yang memiliki pabrik pengolahan.
 * - KEBUN PLASMA: Node Mitra (Slave) yang menyetorkan data ke Inti.
 * 
 * Kalau node mati, statusnya "SENGKETA LAHAN" atau sedang "REPLANTING".
 */
class KemitraanSawit extends EventEmitter {
    constructor(server, config = {}) {
        super();
        this.server = server;

        // Peran default adalah 'KEBUN_INTI' (Perusahaan Besar)
        // Kalau ada 'offtaker', berarti dia 'KEBUN_PLASMA'
        this.peran = config.peran || 'KEBUN_INTI';

        // Daftar mitra (nodes) dalam satu Koperasi
        this.mitra = new Map(); // id -> { host, port, status, produktivitas }

        // Identitas Lahan
        this.hGU = { // Hak Guna Usaha
            id: `${server.host}:${server.port}`,
            peran: this.peran,
            luas_lahan: '1000 Hektar'
        };

        this.logPrefix = '[Kemitraan Sawit]';
        this.patroliInterval = null;

        console.log(`${this.logPrefix} Membuka lahan baru. Status HGU: ${this.peran}`);
    }

    /**
     * Mulai kegiatan patroli mandor (Heartbeat)
     */
    mulaiPatroli() {
        console.log(`${this.logPrefix} Mandor mulai keliling kebun (Starting Heartbeat)...`);

        // Cek patok lahan setiap 5 detik
        this.patroliInterval = setInterval(() => {
            this._cekPatokLahan();
        }, 5000);
    }

    berhentiPatroli() {
        if (this.patroliInterval) {
            clearInterval(this.patroliInterval);
            console.log(`${this.logPrefix} Mandor pulang ke mess karyawan.`);
        }
    }

    /**
     * Mendaftarkan mitra baru (Plasma)
     */
    daftarMitra(id, info) {
        if (!this.mitra.has(id)) {
            console.log(`${this.logPrefix} Penandatanganan akad kredit plasma baru: ${id}.`);
        }

        this.mitra.set(id, {
            ...info,
            status: 'SIAP_PANEN', // Default: Sehat/Online
            terakhir_setor: Date.now(),
            kualitas_buah: 'GRADE A'
        });
    }

    /**
     * Mengecek kondisi mitra (Health Check)
     */
    _cekPatokLahan() {
        const sekarang = Date.now();
        const batasToleransi = 15000; // 15 detik
        const batasSengketa = 60000; // 1 menit

        for (const [id, lahan] of this.mitra) {
            const terakhir = lahan.terakhir_setor || 0;
            const selisih = sekarang - terakhir;

            if (selisih > batasSengketa) {
                if (lahan.status !== 'SENGKETA_LAHAN') {
                    console.error(`${this.logPrefix} Lahan ${id} sudah lama tidak setor TBS. Status: SENGKETA LAHAN (Offline).`);
                    lahan.status = 'SENGKETA_LAHAN';
                }
            } else if (selisih > batasToleransi) {
                if (lahan.status !== 'MASA_TREK') {
                    console.warn(`${this.logPrefix} Lahan ${id} produktivitas menurun. Status: MASA TREK (Lagging).`);
                    lahan.status = 'MASA_TREK'; // Trek = musim buah sedikit
                }
            }
        }
    }

    /**
     * Handle heartbeat dari node lain
     */
    terimaLaporanHarian(laporan) {
        const { id, peran } = laporan;
        if (this.mitra.has(id)) {
            const lahan = this.mitra.get(id);
            lahan.terakhir_setor = Date.now();
            lahan.peran = peran;

            if (lahan.status !== 'SIAP_PANEN') {
                console.log(`${this.logPrefix} Lahan ${id} kembali produktif. Status: SIAP PANEN.`);
                lahan.status = 'SIAP_PANEN';
            }
        } else {
            this.daftarMitra(id, { host: laporan.host, port: laporan.port, peran });
        }
    }

    /**
     * Distribusi Data (Replikasi)
     * Disebut: Distribusi Pupuk & Bibit
     */
    distribusiBibit(dataLog) {
        if (this.peran !== 'KEBUN_INTI') return;

        let jumlahTruk = 0;
        for (const [id, lahan] of this.mitra) {
            if (lahan.peran === 'KEBUN_PLASMA' && lahan.status === 'SIAP_PANEN') {
                // console.log(`${this.logPrefix} Mengirim truk pupuk (data) ke Plasma ${id}...`);
                // TODO: Socket send
                jumlahTruk++;
            }
        }
    }

    getLaporanAgraria() {
        const laporan = {
            status_hgu: this.peran,
            luas_mitra: this.mitra.size + ' Kavling',
            daftar_mitra: []
        };

        this.mitra.forEach((val, key) => {
            laporan.daftar_mitra.push({ kode_kavling: key, ...val });
        });

        return laporan;
    }
}

module.exports = KemitraanSawit;
