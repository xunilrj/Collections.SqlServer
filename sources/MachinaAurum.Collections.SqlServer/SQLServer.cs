using MachinaAurum.Collections.SqlServer.Serializers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace MachinaAurum.Collections.SqlServer
{
    public class SQLServer : ISQLServer
    {
        Func<IDbConnection> GetConnection;

        public SQLServer(string connectionString)
        {
            GetConnection = new Func<IDbConnection>(() => new SqlConnection(connectionString));
        }

        public SQLServer(Func<IDbConnection> getConnection)
        {
            GetConnection = getConnection;
        }

        public void Start<TKey, TValue>(string table, string columnKey, string columnValue, Action<TKey, TValue> addKeyValue)
        {
            var pktablename = Regex.Replace(table, @"[^\w]", "");
            var pkcolumnKey = Regex.Replace(columnKey, @"[^\w]", "");

            var keycolumnType = GetColumnKeyType<TKey>();
            var valueColumnType = GetColumnValueType<TValue>();

            using (var connection = GetConnection())
            {
                GuaranteeOpen(connection);

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"IF OBJECT_ID('{table}') is not null
    SELECT [{columnKey}], [{columnValue}] FROM {table}
ELSE
    CREATE TABLE {table}([{columnKey}] {keycolumnType} not null, [{columnValue}] {valueColumnType}, CONSTRAINT PK_{pktablename}_{pkcolumnKey} PRIMARY KEY CLUSTERED ([{columnKey}]))";

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var key = (TKey)reader.GetValue(0);

                            TValue value = default(TValue);
                            if (NeedDeserialization<TValue>())
                            {
                                var jsonFromDB = (string)reader.GetValue(1);
                                value = JsonConvert.Deserialize<TValue>(jsonFromDB);
                            }
                            else
                            {
                                value = (TValue)reader.GetValue(1);
                            }

                            addKeyValue(key, value);
                        }

                        reader.Close();
                    }
                }
            }
        }

        public TValue GetKeyValue<TKey, TValue>(string table, string columnKey, string columnValue, TKey key)
        {
            using (var connection = GetConnection())
            {
                GuaranteeOpen(connection);

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"SELECT [{columnKey}], [{columnValue}] FROM {table} WHERE [{columnKey}] = @key";
                    AddWithValue(command, "key", key);

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            TValue value = default(TValue);
                            if (NeedDeserialization<TValue>())
                            {
                                var jsonFromDB = (string)reader.GetValue(1);
                                value = JsonConvert.Deserialize<TValue>(jsonFromDB);
                            }
                            else
                            {
                                value = (TValue)reader.GetValue(1);
                            }

                            return value;
                        }
                    }
                }
            }

            return default(TValue);
        }

        public void Prepare<TKey, TValue>(string table, string columnKey, string columnValue)
        {
            var pktablename = Regex.Replace(table, @"[^\w]", "");
            var pkcolumnKey = Regex.Replace(columnKey, @"[^\w]", "");

            var keycolumnType = GetColumnKeyType<TKey>();
            var valueColumnType = GetColumnValueType<TValue>();

            using (var connection = GetConnection())
            {
                GuaranteeOpen(connection);

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"IF OBJECT_ID('{table}') IS NULL
BEGIN
CREATE TABLE {table}([{columnKey}] {keycolumnType} not null, [{columnValue}] {valueColumnType}, CONSTRAINT PK_{pktablename}_{pkcolumnKey} PRIMARY KEY CLUSTERED ([{columnKey}]))
END";
                    command.ExecuteNonQuery();
                }
            }
        }

        private bool NeedDeserialization<T>()
        {
            if (typeof(T) == typeof(int))
            {
                return false;
            }
            else if (typeof(T) == typeof(string))
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        private string GetColumnValueType<T>()
        {
            if (typeof(T) == typeof(int))
            {
                return "int";
            }
            else if (typeof(T) == typeof(string))
            {
                return "ntext";
            }
            else
            {
                return "ntext";
            }
        }

        private string GetColumnKeyType<T>()
        {
            if (typeof(T) == typeof(int))
            {
                return "int";
            }
            else if (typeof(T) == typeof(Guid))
            {
                return "uniqueidentifier";
            }
            else
            {
                return "nvarchar(50)";
            }
        }

        private static void GuaranteeOpen(IDbConnection connection)
        {
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
        }

        public void Add<TKey, TValue>(string table, string keyColumn, string valueColumn, TKey key, TValue value, Action onSuccess, Action onError)
        {
            using (var connection = GetConnection())
            {
                GuaranteeOpen(connection);

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"INSERT INTO {table}([{keyColumn}],[{valueColumn}]) values (@key,@value)";
                    AddWithValue(command, "key", key);

                    if (NeedDeserialization<TValue>())
                    {
                        AddWithValue(command, "value", JsonConvert.Serialize(value));
                    }
                    else
                    {
                        AddWithValue(command, "value", value);
                    }

                    try
                    {
                        var rowsAffected = command.ExecuteNonQuery();

                        if (rowsAffected == 1)
                        {
                            onSuccess();
                        }
                        else
                        {
                            onError();
                        }
                    }
                    catch (DbException)
                    {
                        onError();
                    }
                }
            }
        }

        void AddWithValue(IDbCommand command, string name, object value)
        {
            var keyParameter = command.CreateParameter();
            keyParameter.ParameterName = name;
            keyParameter.Value = value;
            command.Parameters.Add(keyParameter);
        }

        public void Clear(string table, Action onSuccess)
        {
            using (var connection = GetConnection())
            {
                GuaranteeOpen(connection);

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"DELETE FROM {table}";
                    var rowsAffected = command.ExecuteNonQuery();
                    onSuccess();
                }
            }
        }

        public bool Remove<TKey>(string table, string keyColumn, TKey key, Action onSuccess)
        {
            using (var connection = GetConnection())
            {
                GuaranteeOpen(connection);

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"DELETE FROM {table} WHERE [{keyColumn}] = @key";
                    AddWithValue(command, "key", key);

                    var rowsAffected = command.ExecuteNonQuery();
                    onSuccess();
                }
            }
            return true;
        }

        public void Upsert<TKey, TValue>(string table, string keyColumn, string valueColumn, TKey key, TValue value, Action onSuccess, Action onError)
        {
            using (var connection = GetConnection())
            {
                GuaranteeOpen(connection);

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"MERGE INTO {table} AS [Target] 
USING (SELECT @key AS [{keyColumn}]) AS [Source] 
ON [Target].[{keyColumn}] = [Source].[{keyColumn}]
WHEN MATCHED THEN 
    UPDATE SET [Target].[{valueColumn}] = @value
WHEN NOT MATCHED THEN 
    INSERT ([{keyColumn}], [{valueColumn}]) VALUES(@key, @value);";
                    AddWithValue(command, "key", key);

                    if (NeedDeserialization<TValue>())
                    {
                        AddWithValue(command, "value", JsonConvert.Serialize(value));
                    }
                    else
                    {
                        AddWithValue(command, "value", value);
                    }

                    var rowsAffected = command.ExecuteNonQuery();

                    if (rowsAffected == 1)
                    {
                        onSuccess();
                    }
                    else
                    {
                        onError();
                    }
                }
            }
        }

        public void Enqueue<TItem>(string serviceOrigin, string serviceDestination, string contract, string messageType, string baggageTable, IEnumerable<TItem> items)
        {
            var xmlSerializer = new MachinaAurum.Collections.SqlServer.Serializers.XmlSerializer();

            var baggage = new Dictionary<string, byte[]>();
            var xmls = items.Select(x => xmlSerializer.SerializeXml(x, baggage)).ToArray();

            if (baggage.Count == 0)
            {
                var sendCmd = $@"BEGIN TRANSACTION; 
DECLARE @cid UNIQUEIDENTIFIER;
{string.Join(Environment.NewLine, xmls.Select((x, i) => $"DECLARE @xml{i} XML = @message{i};"))}
BEGIN DIALOG @cid FROM SERVICE [{serviceOrigin}] TO SERVICE N'{serviceDestination}' ON CONTRACT [{contract}] WITH ENCRYPTION = OFF; 
{string.Join(Environment.NewLine, xmls.Select((x, i) => $"SEND ON CONVERSATION @cid MESSAGE TYPE [{messageType}] (@xml{i});"))}
END CONVERSATION @cid; 
COMMIT TRANSACTION;";
                using (var connection = GetConnection())
                {
                    GuaranteeOpen(connection);
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = sendCmd;

                        int i = 0;
                        foreach (var xml in xmls)
                        {
                            AddWithValue(command, "@message" + i, xml);
                            ++i;
                        }
                        command.ExecuteNonQuery();
                    }
                }
            }
            else
            {
                var sendCmd = $@"BEGIN TRANSACTION; 
DECLARE @cid UNIQUEIDENTIFIER;
{string.Join(Environment.NewLine, xmls.Select((x, i) => $"DECLARE @xml{i} XML = @message{i};"))}
BEGIN DIALOG @cid FROM SERVICE [{serviceOrigin}] TO SERVICE N'{serviceDestination}' ON CONTRACT [{contract}] WITH ENCRYPTION = OFF; 
{string.Join(Environment.NewLine, xmls.Select((x, i) => $"SEND ON CONVERSATION @cid MESSAGE TYPE [{messageType}] (@xml{i});"))}
END CONVERSATION @cid;
{string.Join("\n", baggage.Select((x, i) => $"INSERT INTO [{baggageTable}](Uri,Data) VALUES (@BaggageId{i}, @BaggageData{i})"))}
COMMIT TRANSACTION;";
                using (var connection = GetConnection())
                {
                    GuaranteeOpen(connection);
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = sendCmd;

                        int i = 0;
                        foreach (var xml in xmls)
                        {
                            AddWithValue(command, "@message" + i, xml);
                            ++i;
                        }

                        i = 0;
                        foreach (var baggageItem in baggage)
                        {
                            AddWithValue(command, $"BaggageId{i}", baggageItem.Key);
                            AddWithValue(command, $"BaggageData{i}", baggageItem.Value);
                            ++i;
                        }

                        command.ExecuteNonQuery();
                    }
                }
            }
        }

        XmlDeserializer CreateDeserializer(string baggageTable, IDbConnection connection, IDbTransaction transaction)
        {
            return new XmlDeserializer(uri => QuerySql<byte[]>($"DELETE FROM [{baggageTable}] OUTPUT deleted.[Data] WHERE Uri = @Param1", uri, connection, transaction));
        }

        public TItem Dequeue<TItem>(string queue, string baggageTable)
        {
            string xml = string.Empty;

            using (var connection = GetConnection())
            {
                GuaranteeOpen(connection);
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"WAITFOR (RECEIVE Top(1) * FROM {queue})";

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var t = reader.GetString(10);

                            if (t == "http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog")
                            {
                                return default(TItem);
                            }

                            xml = ReadMessageBody(reader);
                        }

                        reader.Close();
                    }
                }

                if (string.IsNullOrEmpty(xml) == false)
                {
                    var xmlDeserializer = CreateDeserializer(baggageTable, connection, null);
                    return xmlDeserializer.Deserialize<TItem>(xml);
                }
                else
                {
                    return default(TItem);
                }
            }
        }

        public IEnumerable<object> DequeueGroup(string queue, string baggageTable, Action<IEnumerable<object>> processGroup)
        {
            var xmls = default(IEnumerable<string>);

            using (var connection = GetConnection())
            {
                GuaranteeOpen(connection);

                var transaction = connection.BeginTransaction();

                using (var command = connection.CreateCommand())
                {
                    command.Transaction = transaction;
                    xmls = ProcessGroup(connection, transaction, command, queue, baggageTable);
                }

                if (xmls != null)
                {
                    var messages = ParseMessages(connection, transaction, xmls, baggageTable).ToArray();

                    try
                    {
                        if (messages != null)
                        {
                            processGroup(messages);
                        }
                        transaction.Commit();
                    }
                    catch
                    {
                        transaction.Rollback();
                    }

                    return messages;
                }
                else
                {
                    transaction.Commit();
                    return null;
                }
            }
        }

        IEnumerable<string> ProcessGroup(IDbConnection connection, IDbTransaction transaction, IDbCommand command, string queue, string baggageTable)
        {
            bool foundMessage = false;
            command.CommandText = $@"WAITFOR (RECEIVE Top(100) * FROM {queue})";

            var toProcess = new List<string>();

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var t = reader.GetString(10);

                    if (t == "http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog")
                    {
                        if (!foundMessage)
                        {
                            return null;
                        }
                        else
                        {
                            continue;
                        }
                    }
                    else
                    {
                        foundMessage = true;
                        var body = "";
                        body = ReadMessageBody(reader);
                        toProcess.Add(body);
                    }
                }

                reader.Close();
            }

            return toProcess;
        }

        private static string ReadMessageBody(IDataReader reader)
        {
            var buffer = new byte[4000];

            using (var mem = new MemoryStream())
            {
                if (reader.IsDBNull(13) == false)
                {
                    bool read = true;
                    while (read)
                    {
                        var sizeread = reader.GetBytes(13, mem.Position, buffer, 0, 4000);
                        if (sizeread < 1)
                        {
                            break;
                        }
                        mem.Write(buffer, 0, (int)sizeread);
                    }
                    return System.Text.Encoding.Unicode.GetString(mem.ToArray(), 2, (int)mem.Position - 2);
                }
            }

            return string.Empty;
        }

        public IEnumerable<object> ParseMessages(IDbConnection connection, IDbTransaction transaction, IEnumerable<string> messages, string baggageTable)
        {
            foreach (var message in messages)
            {
                var deserializer = CreateDeserializer(baggageTable, connection, transaction);
                yield return deserializer.Deserialize(message);

            }
        }

        private T QuerySql<T>(string sql, string uri, IDbConnection connection, IDbTransaction transaction)
        {
            if (connection == null)
            {
                using (connection = GetConnection())
                {
                    return InnerQuerySql<T>(sql, uri, connection, transaction);
                }
            }
            else
            {
                return InnerQuerySql<T>(sql, uri, connection, transaction);
            }
        }

        private T InnerQuerySql<T>(string sql, string uri, IDbConnection connection, IDbTransaction transaction)
        {
            GuaranteeOpen(connection);
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = sql;
                AddWithValue(command, "Param1", uri);

                return (T)command.ExecuteScalar();
            }
        }

        public void Execute(string sql)
        {
            using (var connection = GetConnection())
            {
                GuaranteeOpen(connection);
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sql;
                    command.ExecuteNonQuery();
                }
            }
        }
    }



    public static class JsonConvert
    {
        static Func<object, string> SerializeFunc;
        static Func<string, Type, object> DeserializeFunc;

        static JsonConvert()
        {
            try
            {
                var convert = Type.GetType("Newtonsoft.Json.JsonConvert, Newtonsoft.Json");
                var serializeMethod = convert.GetMethod("SerializeObject", BindingFlags.Static | BindingFlags.Public, null, new Type[] { typeof(object) }, null);
                var deserializeMethod = convert.GetMethod("DeserializeObject", BindingFlags.Static | BindingFlags.Public, null, new Type[] { typeof(string), typeof(Type) }, null);

                SerializeFunc = new Func<object, string>(obj =>
                {
                    return (string)serializeMethod.Invoke(null, new object[] { obj });
                });
                DeserializeFunc = new Func<string, Type, object>((json, type) =>
                {
                    return deserializeMethod.Invoke(null, new object[] { json, type });
                });
            }
            catch
            {

            }
        }

        public static string Serialize<T>(T obj)
        {
            if (SerializeFunc == null) throw new Exception("Install NewtonsoftJson.");
            return SerializeFunc(obj);
        }

        public static T Deserialize<T>(string json)
        {
            if (DeserializeFunc == null) throw new Exception("Install NewtonsoftJson.");
            return (T)DeserializeFunc(json, typeof(T));
        }
    }
}
