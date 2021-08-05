namespace TransactionBulkInsert
{
    public class CouchbaseOptions
    {
        public string ConnectionString { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string TargetBucketName { get; set; }
    }

    public class BatchOptions
    {
        public int Size { get; set; }
        public int NumberOfDocuments { get; set; }
    }
}
