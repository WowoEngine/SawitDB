/**
 * Example SawitDB Client Usage
 * 
 * Demonstrates how to connect to SawitDB server and perform operations
 */

const SawitClient = require('../src/SawitClient');

async function main() {
    console.log('╔══════════════════════════════════════════════════╗');
    console.log('║   🌴 SawitDB Client - Example Usage            ║');
    console.log('╚══════════════════════════════════════════════════╝\n');

    // Connect to server
    // Format: sawitdb://[username:password@]host:port/database
    const client = new SawitClient('sawitdb://localhost:7878/plantation');

    try {
        console.log('[1] Connecting to SawitDB server...');
        await client.connect();
        console.log('✓ Connected!\n');

        // Create table
        console.log('[2] Creating table...');
        let result = await client.query('LAHAN sawit_block_a');
        console.log('Result:', result);
        console.log('');

        // Insert data
        console.log('[3] Inserting data...');
        await client.query("TANAM KE sawit_block_a (id, jenis, umur, produksi) BIBIT (1, 'Tenera', 5, 120)");
        await client.query("TANAM KE sawit_block_a (id, jenis, umur, produksi) BIBIT (2, 'Dura', 3, 80)");
        await client.query("TANAM KE sawit_block_a (id, jenis, umur, produksi) BIBIT (3, 'Tenera', 7, 150)");
        await client.query("TANAM KE sawit_block_a (id, jenis, umur, produksi) BIBIT (4, 'Pisifera', 2, 60)");
        await client.query("TANAM KE sawit_block_a (id, jenis, umur, produksi) BIBIT (5, 'Tenera', 10, 200)");
        console.log('✓ Inserted 5 records\n');

        // Select all
        console.log('[4] Selecting all data...');
        result = await client.query('PANEN * DARI sawit_block_a');
        console.log('Result:', JSON.stringify(result, null, 2));
        console.log('');

        // Select with WHERE
        console.log('[5] Selecting with WHERE clause (umur > 5)...');
        result = await client.query('PANEN * DARI sawit_block_a DIMANA umur > 5');
        console.log('Result:', JSON.stringify(result, null, 2));
        console.log('');

        // Select with AND/OR
        console.log('[6] Selecting with AND condition...');
        result = await client.query("PANEN * DARI sawit_block_a DIMANA jenis = 'Tenera' AND umur > 5");
        console.log('Result:', JSON.stringify(result, null, 2));
        console.log('');

        // Create index
        console.log('[7] Creating index on jenis field...');
        result = await client.query('INDEKS sawit_block_a PADA jenis');
        console.log('Result:', result);
        console.log('');

        // Show indexes
        console.log('[8] Showing indexes...');
        result = await client.query('LIHAT INDEKS sawit_block_a');
        console.log('Result:', JSON.stringify(result, null, 2));
        console.log('');

        // Aggregate - COUNT
        console.log('[9] Aggregation - COUNT...');
        result = await client.query('HITUNG COUNT(*) DARI sawit_block_a');
        console.log('Result:', JSON.stringify(result, null, 2));
        console.log('');

        // Aggregate - AVG
        console.log('[10] Aggregation - AVG(produksi)...');
        result = await client.query('HITUNG AVG(produksi) DARI sawit_block_a');
        console.log('Result:', JSON.stringify(result, null, 2));
        console.log('');

        // Aggregate - GROUP BY
        console.log('[11] Aggregation - COUNT by jenis (GROUP BY)...');
        result = await client.query('HITUNG COUNT(*) DARI sawit_block_a KELOMPOK jenis');
        console.log('Result:', JSON.stringify(result, null, 2));
        console.log('');

        // Update
        console.log('[12] Updating data...');
        result = await client.query("PUPUK sawit_block_a DENGAN produksi=250 DIMANA id = 5");
        console.log('Result:', result);
        console.log('');

        // Verify update
        console.log('[13] Verifying update...');
        result = await client.query('PANEN * DARI sawit_block_a DIMANA id = 5');
        console.log('Result:', JSON.stringify(result, null, 2));
        console.log('');

        // Delete
        console.log('[14] Deleting data...');
        result = await client.query('GUSUR DARI sawit_block_a DIMANA id = 4');
        console.log('Result:', result);
        console.log('');

        // Final count
        console.log('[15] Final count...');
        result = await client.query('HITUNG COUNT(*) DARI sawit_block_a');
        console.log('Result:', JSON.stringify(result, null, 2));
        console.log('');

        // List all databases
        console.log('[16] Listing all databases...');
        const databases = await client.listDatabases();
        console.log('Databases:', databases);
        console.log('');

        // Ping test
        console.log('[17] Ping test...');
        const ping = await client.ping();
        console.log(`Latency: ${ping.latency}ms`);
        console.log('');

        console.log('╔══════════════════════════════════════════════════╗');
        console.log('║   ✓ All operations completed successfully!      ║');
        console.log('╚══════════════════════════════════════════════════╝');

    } catch (error) {
        console.error('\n❌ Error:', error.message);
        console.error(error.stack);
    } finally {
        // Disconnect
        console.log('\n[Cleanup] Disconnecting...');
        client.disconnect();
        console.log('✓ Disconnected');
    }
}

// Run example
if (require.main === module) {
    main().catch(err => {
        console.error('Fatal error:', err);
        process.exit(1);
    });
}

module.exports = { main };
