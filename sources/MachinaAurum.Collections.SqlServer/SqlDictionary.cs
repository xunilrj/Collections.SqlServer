using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace MachinaAurum.Collections.SqlServer
{
    public class SqlDictionary<TKey, TValue> : IDictionary<TKey, TValue>
    {
        Dictionary<TKey, TValue> Inner;
        string ConnectionString;
        string TableName;
        string ColumnKey;
        string ColumnValue;

        ISQLServer Server;

        public SqlDictionary()
        {
        }

        public SqlDictionary(string connectionstring, string table, string columnKey, string columnValue)
        {
            Load(connectionstring, table, columnKey, columnValue);
        }

        public SqlDictionary(ISQLServer server)
        {
            Server = server;
        }

        public void Load(string connectionstring, string table, string columnKey, string columnValue)
        {
            ConnectionString = connectionstring;
            TableName = table;
            ColumnKey = columnKey;
            ColumnValue = columnValue;

            if (Regex.IsMatch(TableName, @"^\[\w +\]\.") == false)
            {
                TableName = TableName.Trim('[', ']');
                TableName = $"[dbo].[{TableName}]";
            }

            Inner = new Dictionary<TKey, TValue>();

            if (Server == null)
            {
                Server = new SQLServer(ConnectionString);
            }

            Server.Start<TKey, TValue>(TableName, ColumnKey, ColumnValue, (k, v) => Inner.Add(k, v));
        }

        public TValue this[TKey key]
        {
            get
            {
                return Inner[key];
            }
            set
            {
                Server.Upsert<TKey, TValue>(TableName, ColumnKey, ColumnValue, key, value, () => { Inner[key] = value; }, () => { throw new Exception(); });
            }
        }

        public int Count
        {
            get
            {
                return Inner.Count;
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
                return Inner.Keys;
            }
        }

        public ICollection<TValue> Values
        {
            get
            {
                return Inner.Values;
            }
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Add(item.Key, item.Value);
        }

        public void Add(TKey key, TValue value)
        {
            Server.Add(TableName, ColumnKey, ColumnValue, key, value, () => Inner.Add(key, value), () => { throw new Exception(); });
        }

        public void Clear()
        {
            Server.Clear(TableName, () => Inner.Clear());
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return Inner.Contains(item);
        }

        public bool ContainsKey(TKey key)
        {
            return Inner.ContainsKey(key);
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return Inner.GetEnumerator();
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return Remove(item.Key);
        }

        public bool Remove(TKey key)
        {
            Server.Remove(TableName, ColumnKey, key, () =>
            {
                if (Inner.ContainsKey(key))
                {
                    Inner.Remove(key);
                }
            });

            return true;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            return Inner.TryGetValue(key, out value);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return Inner.GetEnumerator();
        }
    }
}
