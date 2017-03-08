using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace MachinaAurum.Collections.SqlServer
{
    public class SqlNoMemoryDictionary<TKey, TValue> : IDictionary<TKey, TValue>
    {
        public TValue this[TKey key]
        {
            get
            {
                return Server.GetKeyValue<TKey, TValue>(TableName, ColumnKey, ColumnValue, key);
            }
            set
            {
                Server.Upsert<TKey, TValue>(TableName, ColumnKey, ColumnValue, key, value, () => { }, () => { throw new Exception(); });
            }
        }

        public int Count
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public bool IsReadOnly
        {
            get
            {
                return false;
            }
        }

        public ICollection<TKey> Keys
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public ICollection<TValue> Values
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        string ConnectionString;
        string TableName;
        string ColumnKey;
        string ColumnValue;

        ISQLServer Server;

        public SqlNoMemoryDictionary()
        {
        }

        public SqlNoMemoryDictionary(string connectionstring, string table, string columnKey, string columnValue)
            : this()
        {
            Prepare(connectionstring, table, columnKey, columnValue);
        }

        public SqlNoMemoryDictionary(ISQLServer server)
        {
            Server = server;
        }

        public void Prepare(string connectionstring, string table, string columnKey, string columnValue)
        {
            ConnectionString = connectionstring;
            TableName = table;
            ColumnKey = columnKey;
            ColumnValue = columnValue;

            Server = new SQLServer(connectionstring);

            if (Regex.IsMatch(TableName, @"^\[\w +\]\.") == false)
            {
                TableName = TableName.Trim('[', ']');
                TableName = $"[dbo].[{TableName}]";
            }

            if (Server == null)
            {
                Server = new SQLServer(ConnectionString);
            }

            Server.Prepare<TKey, TValue>(TableName, ColumnKey, ColumnValue);
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Add(item.Key, item.Value);
        }

        public void Add(TKey key, TValue value)
        {
            Server.Add(TableName, ColumnKey, ColumnValue, key, value, () => { }, () => { throw new Exception(); });
        }

        public void Clear()
        {
            Server.Clear(TableName, () => { });
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            throw new NotImplementedException();
        }

        public bool ContainsKey(TKey key)
        {
            return Server.GetKeyValue<TKey, TValue>(TableName, ColumnKey, ColumnValue, key) != null;
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return Remove(item.Key);
        }

        public bool Remove(TKey key)
        {
            Server.Remove(TableName, ColumnKey, key, () => { });
            return true;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            var serverValue = Server.GetKeyValue<TKey, TValue>(TableName, ColumnKey, ColumnValue, key);
            if (serverValue != null)
            {
                value = serverValue;
                return true;
            }
            else
            {
                value = default(TValue);
                return false;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
}
