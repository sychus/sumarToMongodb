"use strict";

const mongodb = require('mongodb');
const sql = require('mssql');
const E = require('linq');

const config = require("./config.private.js");

async function exportTable(tableName, targetDb, sqlPool) {
    console.log("Replicating " + tableName + " to mongodb");
    const collection = targetDb.collection(tableName);

    const queryCantRegistros = "select count(*) as cantidad from [" + config.sourceTable + "] where activo = 'S'";
    console.log("Executing query cant registries: " + queryCantRegistros);

    const cantRegistries = await sqlPool.request().query(queryCantRegistros);
    let cant = cantRegistries.recordset[0].cantidad;
    console.log('Cantidad de registros encontrados: ', cant);
    const batch = 500000;
    let skip = 0;

    while (cant >= 0) {
        const query = "select * from [" + config.sourceTable + "]" + "where activo = 'S' order by id_smiafiliados offset " + skip + " rows fetch next " + batch + " rows only";
        console.log("Executing query: " + query);
        const tableResult = await sqlPool.request().query(query);

        console.log("Got " + tableResult.recordset.length + " records from table " + config.sourceTable);

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
        console.log('registros restantes: ', cant);
    }
};

async function main() {
    const mongoClient = await mongodb.MongoClient.connect(config.mongoConnectionString);
    const targetDb = mongoClient.db(config.targetDatabaseName);
    const sqlPool = await sql.connect(config.sqlConnectionString);

    await exportTable(config.targetTable, targetDb, sqlPool);
}

main()
    .then(() => {
        console.log('Done');
    })
    .catch(err => {
        console.error("Database replication errored out.");
        console.error(err);
    });