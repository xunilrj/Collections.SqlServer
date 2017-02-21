﻿using System;

namespace MachinaAurum.Collections.SqlServer
{
    public interface ISQLServer
    {
        void Execute(string sql);
        void Start<TKey, TValue>(string table, string keyColumn, string valueColumn, Action<TKey, TValue> addKeyValue);
        void Add<TKey, TValue>(string table, string keyColumn, string valueColumn, TKey key, TValue value, Action onSuccess, Action onError);
        void Clear(string table, Action onSuccess);
        bool Remove<TKey>(string table, string keyColumn, TKey key, Action onSuccess);
        void Upsert<TKey, TValue>(string table, string keyColumn, string valueColumn, TKey key, TValue value, Action onSuccess, Action onError);

        void Enqueue<TItem>(string serviceOrigin, string serviceDestination, string contract, string messageType, TItem item);
        TItem Dequeue<TItem>(string queue);
    }
}
