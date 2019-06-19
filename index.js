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
    try {
        const mongoClient = await mongodb.MongoClient.connect(config.mongoConnectionString);
        const targetDb = mongoClient.db(config.targetDatabaseName);
        const sqlPool = await sql.connect(config.sqlConnectionString);
        // Proceso de export
        // Backup de los datos actuales de sumar
        const collections = await targetDb.collections();

        // Para el caso que no exista colección de backup
        if (collections.map(c => c.s.name).includes('sumarOld')) {
            await targetDb.dropCollection('sumarOld');
        }
        // Para el caso inicial que la colección sumar no exista
        if (!collections.map(c => c.s.name).includes('sumar')) {
            await targetDb.createCollection('sumar');
        }
        await targetDb.renameCollection('sumar', 'sumarOld');
        await exportTable(config.targetTable, targetDb, sqlPool);
        await targetDb.renameCollection('sumarTemp', 'sumar');
        // Creamos el indice para mejorar la performance
        await targetDb.collection('sumar').createIndex({ activo: 1, afidni: 1 });
    } catch (err) {
        throw (err);
    }
}

main()
    .then(() => {
        console.log('Proceso finalizado');
        process.exit(0);
    })
    .catch(err => {
        console.error("El proceso ha fallado, verifique el error.", err);
        process.exit(0);
    });