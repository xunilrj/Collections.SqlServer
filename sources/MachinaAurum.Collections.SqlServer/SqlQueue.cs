using System.Collections.Generic;

namespace MachinaAurum.Collections.SqlServer
{
    public class SqlQueue
    {
        string ServiceOrigin;
        string ServiceDestination;
        string Contract;
        string MessageType;
        string QueueDestination;

        ISQLServer Server;

        public SqlQueue(string connectionString, string serviceOrigin, string serviceDestination, string contract, string messageType, string queueDestination)
            : this(new SQLServer(connectionString), serviceOrigin, serviceDestination, contract, messageType, queueDestination)
        {
        }

        public SqlQueue(ISQLServer server, string serviceOrigin, string serviceDestination, string contract, string messageType, string queueDestination)
        {
            Server = server;
            ServiceOrigin = serviceOrigin;
            ServiceDestination = serviceDestination;
            Contract = contract;
            MessageType = messageType;
            QueueDestination = queueDestination;
        }

        public void CreateObjects(string queueOrigin)
        {
            Server.Execute($@"DECLARE @IsBroker int
DECLARE @SQL nvarchar(4000)
SELECT @IsBroker = is_broker_enabled FROM sys.databases WHERE [Name] = db_name()
IF (@IsBroker = 0)
BEGIN
	SET @SQL = 'ALTER DATABASE ' + db_name() + ' SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE'
	EXEC sp_executesql @SQL
END

IF NOT EXISTS(SELECT * FROM sys.service_message_types WHERE [Name] = '{MessageType}')
BEGIN
	CREATE MESSAGE TYPE {MessageType} VALIDATION = WELL_FORMED_XML
END

IF NOT EXISTS(SELECT * FROM sys.service_contracts WHERE [Name] = '{Contract}')
BEGIN
	CREATE CONTRACT {Contract}({MessageType} SENT BY ANY)
END

IF NOT EXISTS(SELECT * FROM sys.service_queues WHERE [Name] = '{queueOrigin}')
BEGIN
	CREATE QUEUE {queueOrigin}
END

IF NOT EXISTS(SELECT * FROM sys.service_queues WHERE [Name] = '{QueueDestination}')
BEGIN
	CREATE QUEUE {QueueDestination}
END

IF NOT EXISTS(SELECT * FROM sys.services WHERE [Name] ='{ServiceOrigin}')
BEGIN
	CREATE SERVICE {ServiceOrigin} ON QUEUE {queueOrigin}({Contract})
END

IF NOT EXISTS(SELECT * FROM sys.services WHERE [Name] ='{ServiceDestination}')
BEGIN
	CREATE SERVICE {ServiceDestination} ON QUEUE {QueueDestination}({Contract})
END
");
        }

        public void Enqueue(object item)
        {
            Server.Enqueue(ServiceOrigin, ServiceDestination, Contract, MessageType, item);
        }

        public T Dequeue<T>()
        {
            return (T)Server.Dequeue<T>(QueueDestination);
        }

        public IEnumerable<object> DequeueGroup()
        {
            return Server.DequeueGroup(QueueDestination);
        }
    }
}
