using System;
using System.Collections.Generic;

namespace MachinaAurum.Collections.SqlServer.Tests
{
    public class FakeSqlServer<TInnerKey, TInnerValue> : ISQLServer
    {
        IDictionary<TInnerKey, TInnerValue> Inner;

        public FakeSqlServer(IDictionary<TInnerKey, TInnerValue> inner)
        {
            Inner = inner;
        }

        public void Add<TKey, TValue>(string table, string keyColumn, string valueColumn, TKey key, TValue value, Action onSuccess, Action onError)
        {
            Inner.Add((TInnerKey)(object)key, (TInnerValue)(object)value);
            onSuccess();
        }

        public void Clear(string table, Action onSuccess)
        {
            Inner.Clear();
            onSuccess();
        }

        public TItem Dequeue<TItem>(string queue, string baggageTable)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<object> DequeueGroup(string queue, string baggageTable, Action<IEnumerable<object>> process)
        {
            throw new NotImplementedException();
        }

        public void Enqueue<TItem>(string serviceOrigin, string serviceDestination, string contract, string messageType, string baggageTable, IEnumerable<TItem> item)
        {
            throw new NotImplementedException();
        }

        public void Execute(string sql)
        {
            throw new NotImplementedException();
        }

        public TValue GetKeyValue<TKey, TValue>(string table, string columnKey, string columnValue, TKey key)
        {
            throw new NotImplementedException();
        }

        public void Prepare<TKey, TValue>(string table, string keyColumn, string valueColumn)
        {
            throw new NotImplementedException();
        }

        public bool Remove<TKey>(string table, string keyColumn, TKey key, Action onSuccess)
        {
            Inner.Remove((TInnerKey)(object)key);
            onSuccess();
            return true;
        }

        public void Start<TKey, TValue>(string table, string keyColumn, string valueColumn, Action<TKey, TValue> addKeyValue)
        {
            foreach (var item in Inner)
            {
                addKeyValue((TKey)(object)item.Key, (TValue)(object)item.Value);
            }
        }

        public void Upsert<TKey, TValue>(string table, string keyColumn, string valueColumn, TKey key, TValue value, Action onSuccess, Action onError)
        {
            Inner[(TInnerKey)(object)key] = (TInnerValue)(object)value;
            onSuccess();
        }
    }

    public class FakeSqlServerBroker : ISQLServer
    {
        IDictionary<string, Queue<object>> Queues;
        string QueueDestination;

        public FakeSqlServerBroker(IDictionary<string, Queue<object>> queues, string queueDestination)
        {
            Queues = queues;
            QueueDestination = queueDestination;
        }

        public void Add<TKey, TValue>(string table, string keyColumn, string valueColumn, TKey key, TValue value, Action onSuccess, Action onError)
        {
            throw new NotImplementedException();
        }

        public void Clear(string table, Action onSuccess)
        {
            throw new NotImplementedException();
        }

        public void Enqueue<TItem>(string serviceOrigin, string serviceDestination, string contract, string messageType, string baggageTable, IEnumerable<TItem> item)
        {
            Queue<object> queue = null;
            if (Queues.TryGetValue(QueueDestination, out queue) == false)
            {
                queue = new Queue<object>();
                Queues.Add(QueueDestination, queue);
            }

            queue.Enqueue(item);
        }

        public bool Remove<TKey>(string table, string keyColumn, TKey key, Action onSuccess)
        {
            throw new NotImplementedException();
        }

        public void Start<TKey, TValue>(string table, string keyColumn, string valueColumn, Action<TKey, TValue> addKeyValue)
        {
            throw new NotImplementedException();
        }

        public void Upsert<TKey, TValue>(string table, string keyColumn, string valueColumn, TKey key, TValue value, Action onSuccess, Action onError)
        {
            throw new NotImplementedException();
        }

        public void Execute(string sql)
        {
            throw new NotImplementedException();
        }

        public TItem Dequeue<TItem>(string queue, string baggageTable)
        {
            Queue<object> q = null;
            if (Queues.TryGetValue(queue, out q) == false)
            {
                q = new Queue<object>();
                Queues.Add(queue, q);
            }

            return (TItem)q.Dequeue();
        }

        public IEnumerable<object> DequeueGroup(string queue, string baggageTable, Action<IEnumerable<object>> process)
        {
            throw new NotImplementedException();
        }

        public void Prepare<TKey, TValue>(string table, string keyColumn, string valueColumn)
        {
            throw new NotImplementedException();
        }

        public TValue GetKeyValue<TKey, TValue>(string table, string columnKey, string columnValue, TKey key)
        {
            throw new NotImplementedException();
        }
    }
}
