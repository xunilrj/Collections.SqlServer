using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Serialization;

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

        public void Enqueue<TItem>(string serviceOrigin, string serviceDestination, string contract, string messageType, TItem item)
        {
            var sendCmd = $@"BEGIN TRANSACTION; 
DECLARE @cid UNIQUEIDENTIFIER;
DECLARE @xml XML = @message;
BEGIN DIALOG @cid FROM SERVICE [{serviceOrigin}] TO SERVICE N'{serviceDestination}' ON CONTRACT [{contract}] WITH ENCRYPTION = OFF; 
SEND ON CONVERSATION @cid MESSAGE TYPE [{messageType}] (@xml); 
END CONVERSATION @cid; 
COMMIT TRANSACTION;";
            using (var connection = GetConnection())
            {
                GuaranteeOpen(connection);
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sendCmd;
                    AddWithValue(command, "@message", SerializeXml(item));
                    command.ExecuteNonQuery();
                }
            }
        }

        public TItem Dequeue<TItem>(string queue)
        {
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
                                return Dequeue<TItem>(queue);
                            }

                            var body = "";
                            var buffer = new byte[4000];
                            if (reader.IsDBNull(13) == false)
                            {
                                var size = reader.GetBytes(13, 0, buffer, 0, 4000);
                                body = System.Text.Encoding.Unicode.GetString(buffer, 2, (int)(size - 2));
                                return DeserializeXml<TItem>(body);
                            }
                        }

                        reader.Close();
                    }
                }
            }

            return default(TItem);
        }

        public IEnumerable<object> DequeueGroup(string queue)
        {
            var list = new List<object>();

            bool foundMessage = false;
            using (var connection = GetConnection())
            {
                GuaranteeOpen(connection);
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"WAITFOR (RECEIVE Top(100) * FROM {queue})";

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var t = reader.GetString(10);

                            if (t == "http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog")
                            {
                                if (!foundMessage)
                                {
                                    return DequeueGroup(queue);
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
                                var buffer = new byte[4000];
                                if (reader.IsDBNull(13) == false)
                                {
                                    var size = reader.GetBytes(13, 0, buffer, 0, 4000);
                                    body = System.Text.Encoding.Unicode.GetString(buffer, 2, (int)(size - 2));

                                    var doc = new XmlDocument();
                                    doc.LoadXml(body);
                                    var tagName = doc.FirstChild.Name;
                                    var type = FindType(tagName);
                                    var item = DeserializeXml(type, body);
                                    list.Add(item);
                                }
                            }
                        }

                        reader.Close();
                    }
                }
            }

            return list;
        }

        private static Type FindType(string typeName)
        {
            return AppDomain.CurrentDomain.GetAssemblies().SelectMany(x =>
            {
                try { return x.GetTypes(); }
                catch (ReflectionTypeLoadException e) { return e.Types.Where(t => t != null).ToArray(); }
            }).Where(x => x.Name == typeName && x.GetCustomAttribute<SerializableAttribute>() != null)
              .FirstOrDefault();
        }

        string SerializeXml(object item)
        {
            using (var stringWriter = new StringWriter())
            {
                var writer = new XmlTextWriter(stringWriter);

                WriteObject(null, item, writer);

                var xml = stringWriter.GetStringBuilder().ToString();
                return xml;
            }
        }

        private void WriteObject(string name, object item, XmlTextWriter writer)
        {
            var type = item.GetType();
            var properties = type.GetProperties();

            if (string.IsNullOrEmpty(name))
            {
                writer.WriteStartElement(type.Name);
            }
            else
            {
                writer.WriteStartElement(name);
            }

            foreach (var property in properties)
            {
                if (property.CanRead)
                {
                    if (WriteAsAttribute(property))
                    {
                        writer.WriteAttributeString(property.Name, property.GetValue(item).ToString());
                    }
                    else
                    {
                        WriteObject(property.Name, property.GetValue(item), writer);
                    }
                }
            }
            writer.WriteEndElement();
        }

        bool WriteAsAttribute(PropertyInfo info)
        {
            if (info.PropertyType.IsPrimitive)
            {
                return true;
            }
            else if (info.PropertyType == typeof(string))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        object DeserializeXml(Type type, string xml)
        {
            using (var stringReader = new StringReader(xml))
            {
                var reader = new XmlTextReader(stringReader);
                reader.Read();
                return ReadObject(reader, null, 0);
            }
        }

        private object ReadObject(XmlTextReader reader, object parent, int depth)
        {
            string propertyName = reader.Name;

            Type type = null;

            if (parent == null)
            {
                type = FindType(propertyName);
            }
            else
            {
                type = parent.GetType().GetProperty(propertyName).PropertyType;
            }

            var obj = System.Runtime.Serialization.FormatterServices.GetUninitializedObject(type);

            if (parent == null)
            {
                parent = obj;
            }
            else
            {
                parent.GetType().GetProperty(propertyName).SetValue(parent, obj);
            }

            reader.MoveToFirstAttribute();

            do
            {
                var attributeName = reader.Name;
                var value = reader.Value;

                var property = type.GetProperty(attributeName);
                if (property.CanWrite)
                {
                    property.SetValue(obj, Convert(property.PropertyType, value));
                }
            } while (reader.MoveToNextAttribute());

            reader.MoveToElement();
            reader.Read();

            if (reader.Depth > depth)
            {
                var childobj = ReadObject(reader, obj, depth + 1);
            }

            return obj;
        }

        object Convert(Type target, string value)
        {
            if (target == typeof(string))
            {
                return value;
            }
            else
            {
                var parse = target.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static, null, new[] { typeof(string) }, null);
                return parse.Invoke(null, new[] { value });
            }
        }

        T DeserializeXml<T>(string xml)
        {
            return (T)DeserializeXml(typeof(T), xml);
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
            return SerializeFunc(obj);
        }

        public static T Deserialize<T>(string json)
        {
            return (T)DeserializeFunc(json, typeof(T));
        }
    }
}
