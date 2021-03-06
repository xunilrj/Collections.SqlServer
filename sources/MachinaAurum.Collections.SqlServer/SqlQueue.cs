﻿using System;
using System.Collections.Generic;

namespace MachinaAurum.Collections.SqlServer
{
    public class SqlQueue
    {
        ISQLServer Server;
        SqlQueueParameters Parameters;

        public SqlQueue(SqlQueueParameters parameters)
            : this(new SQLServer(parameters.ConnectionString), parameters)
        {

        }

        public SqlQueue(ISQLServer server, SqlQueueParameters parameters)
        {
            Server = server;
            Parameters = parameters;
        }

        public void CreateObjects()
        {
            Server.Execute($@"DECLARE @IsBroker int
DECLARE @SQL nvarchar(4000)
SELECT @IsBroker = is_broker_enabled FROM sys.databases WHERE [Name] = db_name()
IF (@IsBroker = 0)
BEGIN
	SET @SQL = 'ALTER DATABASE ' + db_name() + ' SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE'
	EXEC sp_executesql @SQL
END

IF NOT EXISTS(SELECT * FROM sys.service_message_types WHERE [Name] = '{Parameters.MessageType}')
BEGIN
	CREATE MESSAGE TYPE {Parameters.MessageType} VALIDATION = WELL_FORMED_XML
END

IF NOT EXISTS(SELECT * FROM sys.service_contracts WHERE [Name] = '{Parameters.Contract}')
BEGIN
	CREATE CONTRACT {Parameters.Contract}({Parameters.MessageType} SENT BY ANY)
END

IF NOT EXISTS(SELECT * FROM sys.service_queues WHERE [Name] = '{Parameters.QueueOrigin}')
BEGIN
	CREATE QUEUE {Parameters.QueueOrigin}
END

IF NOT EXISTS(SELECT * FROM sys.service_queues WHERE [Name] = '{Parameters.QueueDestination}')
BEGIN
	CREATE QUEUE {Parameters.QueueDestination}
END

IF NOT EXISTS(SELECT * FROM sys.services WHERE [Name] ='{Parameters.ServiceOrigin}')
BEGIN
	CREATE SERVICE {Parameters.ServiceOrigin} ON QUEUE {Parameters.QueueOrigin}({Parameters.Contract})
END

IF NOT EXISTS(SELECT * FROM sys.services WHERE [Name] ='{Parameters.ServiceDestination}')
BEGIN
	CREATE SERVICE {Parameters.ServiceDestination} ON QUEUE {Parameters.QueueDestination}({Parameters.Contract})
END

IF OBJECT_ID('{Parameters.BaggageTable}') IS NULL
BEGIN
	CREATE TABLE [{Parameters.BaggageTable}]([Uri] varchar(50), [Data] varbinary(MAX))
END");
        }

        public void Enqueue(object item)
        {
            Server.Enqueue(Parameters.ServiceOrigin, Parameters.ServiceDestination, Parameters.Contract, Parameters.MessageType, Parameters.BaggageTable, new[] { item });
        }

        public void Enqueue(IEnumerable<object> items)
        {
            Server.Enqueue(Parameters.ServiceOrigin, Parameters.ServiceDestination, Parameters.Contract, Parameters.MessageType, Parameters.BaggageTable, items);
        }

        public T Dequeue<T>()
        {
            T item = default(T);

            do
            {
                item = (T)Server.Dequeue<T>(Parameters.QueueDestination, Parameters.BaggageTable);
            } while (item == null);

            return item;
        }

        public IEnumerable<object> DequeueGroup()
        {
            var group = default(IEnumerable<object>);

            do
            {
                group = Server.DequeueGroup(Parameters.QueueDestination, Parameters.BaggageTable, x => { });
            } while (group == null);

            return group;
        }

        public void Clear()
        {
            Server.Execute($@"DECLARE @handle UNIQUEIDENTIFIER;
WHILE(SELECT COUNT(*) FROM {Parameters.QueueDestination}) > 0
BEGIN
    RECEIVE TOP(1) @handle = conversation_handle FROM {Parameters.QueueDestination};
    END CONVERSATION @handle WITH CLEANUP
END");
        }

        public void DequeueGroup(Action<IEnumerable<object>> process)
        {
            var group = default(IEnumerable<object>);

            do
            {
                group = Server.DequeueGroup(Parameters.QueueDestination, Parameters.BaggageTable, process);
            } while (group == null);
        }

        public void DequeueGroup<TKey>(IDictionary<TKey, QueuItemEnvelope> envelopeDictionary, Func<object, TKey> getKey, Action<object> process)
        {
            DequeueGroup(messages =>
            {
                foreach (var message in messages)
                {
                    var key = getKey(message);

                    QueuItemEnvelope envelope = null;
                    if (envelopeDictionary.TryGetValue(key, out envelope) == false)
                    {
                        envelope = new QueuItemEnvelope(message);
                    }

                    if (envelope.Status == QueuItemStatus.Enqueued || envelope.Status == QueuItemStatus.Processing)
                    {
                        envelope.StartProcessing();
                        envelopeDictionary[key] = envelope;

                        process(message);

                        envelope.FinishProcessing();
                        envelopeDictionary[key] = envelope;
                    }
                }
            });
        }

        public void EnableQueue(bool enabled = true)
        {
            if(enabled)
            {
                this.Server.Execute($"ALTER QUEUE dbo.{Parameters.QueueOrigin} WITH STATUS = ON");
                this.Server.Execute($"ALTER QUEUE dbo.{Parameters.QueueDestination} WITH STATUS = ON");
            }
            else
            {
                this.Server.Execute($"ALTER QUEUE dbo.{Parameters.QueueOrigin} WITH STATUS = OFF");
                this.Server.Execute($"ALTER QUEUE dbo.{Parameters.QueueDestination} WITH STATUS = OFF");
            }
        }
    }
}
