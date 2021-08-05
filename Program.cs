using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.KeyValue;
using Couchbase.Transactions;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Error;

namespace TransactionBulkInsert
{
    class Program
    {
        public static CouchbaseOptions _couchbaseOptions;
        public static BatchOptions _batchOptions;
        public static ICluster _cluster;
        public static IBucket _bucket;
        public static ICouchbaseCollection _collection;
        public static Transactions _transactions;

        static async Task Main(string[] args)
        {
            //init couchbase and batch options
            InitOptions();

            //setup couchbase variables
            _cluster = await Cluster.ConnectAsync(_couchbaseOptions.ConnectionString, _couchbaseOptions.Username, _couchbaseOptions.Password);
            _bucket = await _cluster.BucketAsync(_couchbaseOptions.TargetBucketName);
            _collection = _bucket.DefaultCollection();
            _transactions = Transactions.Create(_cluster,
                TransactionConfigBuilder.Create()
                .DurabilityLevel(DurabilityLevel.None)
                .Build());

            await SaveBatch();

            // Wait until the app unloads
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            var tcs = new TaskCompletionSource<bool>();
            cts.Token.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            await tcs.Task;
        }

        public static async Task SaveBatch()
        {
            //create batch
            var batch = new List<object>();

            //populate batch with simple data
            for (var i = 0; i < _batchOptions.Size; i++)
            {
                batch.Add(new { Idx = i });
            }

            try
            {
                for (var i = 0; i < _batchOptions.NumberOfDocuments / _batchOptions.Size; i++)
                {
                    //save batch in transaction
                    await _transactions.RunAsync(async (ctx) =>
                    {
                        var tasks = new List<Task>();
                        for (int j = 0; j < batch.Count; j++)
                        {
                            var task = ctx.InsertAsync(_collection, $"doc:{Guid.NewGuid()}", batch[j]);
                            tasks.Add(task);
                        }

                        await Task.WhenAll(tasks);
                        await ctx.CommitAsync().ConfigureAwait(false);

                        Console.WriteLine($"{(i + 1) * _batchOptions.Size} document saved");

                    }).ConfigureAwait(false);
                }

                Console.WriteLine($"{_batchOptions.NumberOfDocuments / _batchOptions.Size} Batchs correctly saved");
            }
            catch (TransactionCommitAmbiguousException e)
            {
                Console.Error.WriteLine(e);
            }
            catch (TransactionFailedException e)
            {
                Console.Error.WriteLine(e);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e);
            }
        }


        public static void InitOptions()
        {
            _couchbaseOptions = new CouchbaseOptions
            {
                ConnectionString = "couchbase://localhost",
                Username = "admin",
                Password = "adminn",
                TargetBucketName = "testBucket"
            };

            _batchOptions = new BatchOptions
            {
                Size = 10,
                NumberOfDocuments = 500
            };
        }
    }
}
