module.exports = {

    sqlConnectionString: "Server=10.1.62.53;Database=sips;User Id=andes;Password=andesIntegration;", // Insert your connection string here.
    // mongoConnectionString: "mongodb://10.1.62.19:27017", // This puts the resulting database in MongoDB running on your local PC.
    mongoConnectionString: "mongodb://admin:golitoMon04@10.1.72.7:27033/andes?authSource=admin",
    targetDatabaseName: "andes", // Specify the MongoDB database where the data will end up.
    sourceTable: "PN_smiafiliados", // Specify the mssql table to export to mongodb.
    targetTable: 'sumar',
    targetTableId: "id_smiafiliados",
    skip: [
        "sql-table-to-skip-1", // Add the tables here that you don't want to replicate to MongoDB.
        "sql-table-to-skip-2"
    ],
    remapKeys: false // Set this to false if you want to leave table keys as they are, set to true to remap them to MongoDB ObjectId's.
};