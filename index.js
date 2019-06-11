"use strict";

const mongodb = require('mongodb');
const sql = require('mssql');
const E = require('linq');

const config = require("./config.private.js");

async function exportTable(tableName, targetDb, sqlPool) {
    const collection = targetDb.collection(tableName);
    const queryCantRegistros = "select count(*) as cantidad from [" + config.sourceTable + "] where activo = 'S'";
    const cantRegistries = await sqlPool.request().query(queryCantRegistros);
    let cant = cantRegistries.recordset[0].cantidad;
    const batch = 500000; // Batch max por iteración
    let skip = 0;

    while (cant >= 0) {
        const query = "select * from [" + config.sourceTable + "]" + "where activo = 'S' order by id_smiafiliados offset " + skip + " rows fetch next " + batch + " rows only";
        const tableResult = await sqlPool.request().query(query);
        if (tableResult.recordset.length === 0) {
            console.log('No records to transfer.');
            return;
        }
        const bulkRecordInsert = E.from(tableResult.recordset)
            .select(row => {
                return {
                    insertOne: {
                        document: row
                    },
                }
            }).toArray();
        await collection.bulkWrite(bulkRecordInsert);
        skip = skip + batch;
        cant = cant - batch;
    }
};

async function main() {
    // Conexiones
    const mongoClient = await mongodb.MongoClient.connect(config.mongoConnectionString);
    const targetDb = mongoClient.db(config.targetDatabaseName);
    const sqlPool = await sql.connect(config.sqlConnectionString);
    // Proceso de export
    await exportTable(config.targetTable, targetDb, sqlPool);
}

main()
    .then(() => {
        console.log('Proceso finalizado');
        process.exit(0);
    })
    .catch(err => {
        console.error("Database replication errored out.", err);
        process.exit(0);
    });