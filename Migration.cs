using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Table;
using MongoDB.Bson;
using MongoDB.Driver;
using MoreLinq;

namespace StorageToMongo
{
    public class Migration
    {
        private IConfiguration _configuration;
        private readonly CloudTableClient _storageTableClient;
        private readonly IMongoDatabase _mongoDatabase;
        private readonly ILogger<Migration> _log;

        public Migration(IConfiguration configuration, CloudTableClient storageTableClient, IMongoDatabase mongoDatabase, ILogger<Migration> log)
        {
            _configuration = configuration;
            _storageTableClient = storageTableClient;
            _mongoDatabase = mongoDatabase;
            _log = log;
        }

        public async Task Run()
        {
            // get storage table ref
            var storageTableName = _configuration["FromTableStorageTable"];
            var storageTable = _storageTableClient.GetTableReference(storageTableName);

            // get mongo collection ref
            var mongoCollectionName = _configuration["ToMongoCollection"];
            var mongoCollection = _mongoDatabase.GetCollection<BsonDocument>(mongoCollectionName);

            // TODO: Fix this
            /*
            if (cleanUp)
            {
                // get all existing records
                var allKeys = mongoCollection
                    .Find(FilterDefinition<BsonDocument>.Empty)
                    .Project(Builders<BsonDocument>.Projection
                        .Include("PartitionKey")
                        .Include("RowKey")
                        .Exclude("_id"))
                    .ToList();

                if (allKeys.Any())
                {
                    // truncate collection
                    allKeys
                        .GroupBy(o => o["PartitionKey"].AsString)
                        .AsParallel()
                        .WithDegreeOfParallelism(10)
                        .ForEach(grouping =>
                        {
                            mongoCollection.DeleteMany(Builders<BsonDocument>.Filter.In("PartitionKey", grouping.Key));
                        });

                    // clear existing records list
                    allKeys.Clear();
                }
            }
            */

            // create partial query
            var tablePartialQuery = new TableQuery<DynamicTableEntity>()
                .Select(new[] { "PartitionKey", "RowKey"})
                .Take(1000);

            // check for filter
            // ex: "PartitionKey ge 'zinc' and RowKey ge 'zinc02538020 <40426484>'"
            var queryFilter = _configuration["TableStorageQueryFilter"];
            if (!string.IsNullOrWhiteSpace(queryFilter))
                tablePartialQuery.Where(queryFilter);

            TableContinuationToken continuationToken = null;

            // retrieve and migrate while more records are present
            do
            {
                // grab 10k records
                var segmentedBatchResults = new List<DynamicTableEntity>();
                do
                {
                    // retrieve a segment
                    var tableQueryResult = await storageTable
                        .ExecuteQuerySegmentedAsync(tablePartialQuery, continuationToken);
                    var segmentedResults = tableQueryResult.Results;

                    if (segmentedResults.Any())
                    {
                        // get segment info
                        var pk = segmentedResults.First().PartitionKey;
                        var rk = segmentedResults.First().RowKey;

                        _log.LogInformation($"PK: {pk}, RK: {rk}");
                    }
                    
                    // assign continuation token
                    continuationToken = tableQueryResult.ContinuationToken;

                    segmentedBatchResults.AddRange(segmentedResults);
                } while (segmentedBatchResults.Count < 10000 && continuationToken != null);

                // upload documents
                var uploadedDocs = 0;
                var semaphore = new SemaphoreSlim(20, 20);
                var uploadTasks = segmentedBatchResults
                    .Batch(50)
                    .Select(batchEntities => Task.Run(async () =>
                    {
                        await semaphore.WaitAsync();

                        // create full query
                        var batchList = batchEntities.ToList();
                        var fullTableFilter = string.Join("or", batchList
                            .Select(o => $"(PartitionKey eq '{o.PartitionKey.Replace("'", "''")}' and " +
                                         $"RowKey eq '{o.RowKey.Replace("'", "''")}')"));
                        var fullTableQuery = new TableQuery<DynamicTableEntity>().Where(fullTableFilter);

                        // retrieve a segment
                        var fullTableQueryResult = await storageTable
                            .ExecuteQuerySegmentedAsync(fullTableQuery, null);
                        batchList = fullTableQueryResult.Results;

                        // upload to mongo
                        const int sleepBeforeRetry = 750;
                        const int maxRetries = 10;
                        var tryCount = 0;
                        var docs = new List<BsonDocument>();
                        Exception ex = null;

                        do
                        {
                            try
                            {
                                // find already migrated documents
                                var eqFilters = new List<FilterDefinition<BsonDocument>>();
                                foreach (var result in batchList)
                                {
                                    eqFilters.Add(new BsonDocument(new Dictionary<string, object>
                                        {{"PartitionKey", result.PartitionKey}, {"RowKey", result.RowKey}}));
                                }

                                var filter = Builders<BsonDocument>.Filter.Or(eqFilters);
                                var migratedRecords = await mongoCollection.Find(filter)
                                    .Project(Builders<BsonDocument>.Projection.Include("PartitionKey")
                                        .Include("RowKey")
                                        .Exclude("_id"))
                                    .ToListAsync();

                                // exclude them
                                if (migratedRecords.Any())
                                    batchList.RemoveAll(o => migratedRecords.Exists(
                                        a => o.PartitionKey == a["PartitionKey"].AsString && o.RowKey == a["RowKey"].AsString));

                                if (!batchList.Any())
                                {
                                    semaphore.Release();
                                    return;
                                }

                                // map docs
                                docs = batchList.AsParallel()

                                    // convert to dictionary
                                    .Select(o =>
                                    {
                                        var doc = o.Properties.ToDictionary(property => property.Key, property => property.Value.PropertyAsObject);

                                        doc.Add("PartitionKey", o.PartitionKey);
                                        doc.Add("RowKey", o.RowKey);

                                        return doc;
                                    })

                                    // build bson docs from dicts
                                    .Select(o => new BsonDocument(o))
                                    .ToList();

                                await mongoCollection.InsertManyAsync(docs);

                                uploadedDocs += docs.Count;
                            }
                            catch (MongoBulkWriteException<BsonDocument> e)
                            {
                                // prepare to retry
                                tryCount++;
                                ex = e;
                                Thread.Sleep(sleepBeforeRetry);
                            }
                        } while (tryCount < maxRetries && ex != null);

                        if (tryCount == maxRetries)
                            throw ex;
                        
                        semaphore.Release();
                    }))
                    .ToArray();

                Task.WaitAll(uploadTasks);

                _log.LogDebug($"Uploaded: {uploadedDocs}");

            } while (continuationToken != null);
        }
    }
}
