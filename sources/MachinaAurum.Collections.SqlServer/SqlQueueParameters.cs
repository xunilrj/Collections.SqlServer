namespace MachinaAurum.Collections.SqlServer
{
    public class SqlQueueParameters
    {
        public string ConnectionString { get; set; }
        public string ServiceOrigin { get; set; }
        public string ServiceDestination { get; set; }
        public string Contract { get; set; }
        public string MessageType { get; set; }
        public string QueueOrigin { get; set; }
        public string QueueDestination { get; set; }
        public string BaggageTable { get; set; }

        public SqlQueueParameters(string connectionString, string serviceOrigin, string serviceDestination, string contract, string messageType, string queueOrigin, string queueDestination, string tableBaggage)
        {
            ConnectionString = connectionString;
            ServiceOrigin = serviceOrigin;
            ServiceDestination = serviceDestination;
            Contract = contract;
            MessageType = messageType;
            QueueOrigin = queueOrigin;
            QueueDestination = queueDestination;
            BaggageTable = tableBaggage;
        }
    }
}
