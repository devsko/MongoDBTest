using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Linq;

namespace MongoDBTest
{
    internal class Program
    {
        static void Main(string[] args)
        {
            ServiceProvider services = new ServiceCollection()
                .AddLogging(builder => builder
                    .AddSimpleConsole()
                    .SetMinimumLevel(LogLevel.Debug))
                .BuildServiceProvider();

            MongoClientSettings settings = MongoClientSettings.FromConnectionString("mongodb://localhost:27017");
            settings.LinqProvider = LinqProvider.V3; // LinqProvider.V2 schmeißt eine NotSupportedException bei der Linq Variante
            settings.LoggingSettings = new LoggingSettings(services.GetRequiredService<ILoggerFactory>());

            MongoClient client = new(settings);

            IMongoCollection<ArchiveEntry> archiveEntries = client
                .GetDatabase("local")
                .GetCollection<ArchiveEntry>("ArchiveEntry");

            Measure(Variante1); // Variante1 result: 18 duration: 313 ms
            Measure(Variante2); // Variante2 result: 18 duration: 461 ms

            int Variante1()
            {
                PipelineDefinition<ArchiveEntry, BsonDocument> pipeline = new[]
                {
                    BsonDocument.Parse("""
                        {
                          $project: {
                            DocumentsPageCount: {
                              $sum: "$Documents.PageCount",
                            },
                          },
                        }
                        """),
                    BsonDocument.Parse("""
                        {
                          $group: {
                            _id: null,
                            TotalPageCount: {
                              $sum: "$DocumentsPageCount",
                            },
                          },
                        }
                        """)
                };

                BsonDocument result = archiveEntries
                    .Aggregate<BsonDocument>(pipeline, new AggregateOptions { AllowDiskUse = true })
                    .First();

                // { "_id" : null, "TotalPageCount" : 18 }

                return result[1].AsInt32;

                /*
                dbug: MongoDB.Command[0]
                      1 3 localhost 27017 91 5 1 Command started aggregate local { "aggregate" : "ArchiveEntry", "pipeline" : [{ "$project" : { "DocumentsPageCount" : { "$sum" : "$Documents.PageCount" } } }, { "$group" : { "_id" : null, "TotalPageCount" : { "$sum" : "$DocumentsPageCount" } } }], "allowDiskUse" : true, "cursor" : { }, "$db" : "local", "lsid" : { "id" : CSUUID("5b98ea56-e05c-41ea-86b0-8d6bac07e076") } }
                dbug: MongoDB.Command[0]
                      1 3 localhost 27017 91 5 1 Command succeeded aggregate 26.1502 { "cursor" : { "firstBatch" : [{ "_id" : null, "TotalPageCount" : 18 }], "id" : NumberLong(0), "ns" : "local.ArchiveEntry" }, "ok" : 1.0 }
                */
            }

            int Variante2()
            {
                return archiveEntries
                    .AsQueryable(new AggregateOptions() { AllowDiskUse = true })
                    .Select(entry => entry.Documents.Sum(doc => doc.PageCount))
                    .GroupBy(entry => BsonNull.Value)
                    .Select(group => group.Sum())
                    .First();

                /*
                dbug: MongoDB.Command[0]
                      1 3 localhost 27017 88 5 1 Command started aggregate local { "aggregate" : "ArchiveEntry", "pipeline" : [{ "$project" : { "_v" : { "$sum" : "$Documents.PageCount" }, "_id" : 0 } }, { "$group" : { "_id" : null, "__agg0" : { "$sum" : "$_v" } } }, { "$project" : { "_v" : "$__agg0", "_id" : 0 } }, { "$limit" : NumberLong(1) }], "allowDiskUse" : true, "cursor" : { }, "$db" : "local", "lsid" : { "id" : CSUUID("838650aa-a4f1-4cdd-a703-39bb4a914100") } }
                dbug: MongoDB.Command[0]
                      1 3 localhost 27017 88 5 1 Command succeeded aggregate 24.4999 { "cursor" : { "firstBatch" : [{ "_v" : 18 }], "id" : NumberLong(0), "ns" : "local.ArchiveEntry" }, "ok" : 1.0 }
                */
            }

            void Measure<T>(Func<T> func, [CallerArgumentExpression(nameof(func))] string? expression = null)
            {
                Stopwatch watch = Stopwatch.StartNew();
                T result = func();
                watch.Stop();

                Console.WriteLine($"{expression} result: {result} duration: {watch.ElapsedMilliseconds} ms");
            }
        }
    }

    public class ArchiveEntry
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string? Id { get; set; }

        [BsonElement]
        public string? Name { get; set; }

        [BsonElement]
        [BsonIgnoreIfNull]
        public List<ArchiveDocument> Documents { get; set; } = null!;
    }

    public class ArchiveDocument
    {
        [BsonElement]
        public int PageCount { get; set; }
    }
}