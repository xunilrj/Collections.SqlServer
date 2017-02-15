using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
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
            return SerializeFunc(obj);
        }

        public static T Deserialize<T>(string json)
        {
            return (T)DeserializeFunc(json, typeof(T));
        }
    }
}
