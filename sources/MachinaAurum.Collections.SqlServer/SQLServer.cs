using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
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

        public void Enqueue<TItem>(string serviceOrigin, string serviceDestination, string contract, string messageType, string baggageTable, TItem item)
        {
            var baggage = new Dictionary<string, byte[]>();
            var xml = SerializeXml(item, baggage);

            if (baggage.Count == 0)
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
                        AddWithValue(command, "@message", xml);
                        command.ExecuteNonQuery();
                    }
                }
            }
            else
            {
                var sendCmd = $@"BEGIN TRANSACTION; 
DECLARE @cid UNIQUEIDENTIFIER;
DECLARE @xml XML = @message;
BEGIN DIALOG @cid FROM SERVICE [{serviceOrigin}] TO SERVICE N'{serviceDestination}' ON CONTRACT [{contract}] WITH ENCRYPTION = OFF; 
SEND ON CONVERSATION @cid MESSAGE TYPE [{messageType}] (@xml); 
END CONVERSATION @cid;
{string.Join("\n", baggage.Select((x, i) => $"INSERT INTO [{baggageTable}](Uri,Data) VALUES (@BaggageId{i}, @BaggageData{i})"))}
COMMIT TRANSACTION;";
                using (var connection = GetConnection())
                {
                    GuaranteeOpen(connection);
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = sendCmd;
                        AddWithValue(command, "@message", xml);

                        int i = 0;
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

        public TItem Dequeue<TItem>(string queue, string baggageTable)
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
                                return Dequeue<TItem>(queue, baggageTable);
                            }

                            var body = "";
                            var buffer = new byte[4000];
                            if (reader.IsDBNull(13) == false)
                            {
                                var size = reader.GetBytes(13, 0, buffer, 0, 4000);
                                body = System.Text.Encoding.Unicode.GetString(buffer, 2, (int)(size - 2));
                                return DeserializeXml<TItem>(body, baggageTable);
                            }
                        }

                        reader.Close();
                    }
                }
            }

            return default(TItem);
        }

        public IEnumerable<object> DequeueGroup(string queue, string baggageTable)
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
                                    return DequeueGroup(queue, baggageTable);
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
                                    var item = DeserializeXml(type, body, baggageTable);
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

        string SerializeXml(object item, IDictionary<string, byte[]> baggage)
        {
            using (var stringWriter = new StringWriter())
            {
                var writer = new XmlTextWriter(stringWriter);

                WriteObject(null, item, writer, 0, baggage);

                var xml = stringWriter.GetStringBuilder().ToString();
                return xml;
            }
        }

        private void WriteObject(PropertyInfo currentProperty, object item, XmlTextWriter writer, int depth, IDictionary<string, byte[]> baggage)
        {
            if (depth > 10)
            {
                return;
            }

            if (item == null)
            {
                writer.WriteStartElement(currentProperty.Name);
                writer.WriteEndElement();
                return;
            }

            var type = item.GetType();
            var properties = type.GetProperties();

            if (currentProperty == null)
            {
                writer.WriteStartElement(type.Name);
            }
            else
            {

                string name = currentProperty.Name;
                writer.WriteStartElement(name);
            }

            if (item.GetType().IsArray)
            {
                var elementType = item.GetType().GetElementType();
                if (elementType == typeof(string))
                {
                    foreach (var arrayitem in (string[])item)
                    {
                        writer.WriteStartElement("string");
                        writer.WriteCData(arrayitem);
                        writer.WriteEndElement();
                    }
                }
                else if (elementType == typeof(byte))
                {
                    var baggageid = Guid.NewGuid();
                    var uri = $"baggage://{baggageid}";
                    baggage.Add(uri, (byte[])item);

                    writer.WriteStartElement("proxy");
                    writer.WriteAttributeString("uri", uri);
                    writer.WriteEndElement();
                }

                writer.WriteEndElement();

                return;
            }
            else if (item.GetType().IsGenericType && item.GetType().GetGenericTypeDefinition() == typeof(Dictionary<,>))
            {
                var genericArguments = item.GetType().GetGenericArguments();

                var items = (IEnumerable)item;
                foreach (dynamic i in items)
                {
                    writer.WriteStartElement("item");

                    if (genericArguments[0].IsPrimitive)
                    {
                        writer.WriteAttributeString("key", i.Key);
                    }
                    else if (genericArguments[0] == typeof(string))
                    {
                        writer.WriteAttributeString("key", i.Key);
                    }

                    if (genericArguments[1].IsPrimitive)
                    {
                        writer.WriteAttributeString("value", i.Value);
                    }
                    else if (genericArguments[1].IsArray && genericArguments[1].GetElementType() == typeof(byte))
                    {
                        byte[] data = (byte[])i.Value;

                        var baggageid = Guid.NewGuid();
                        var uri = $"baggage://{baggageid}";
                        baggage.Add(uri, data);

                        writer.WriteStartElement("value");
                        writer.WriteAttributeString("uri", uri);
                        //writer.WriteCData(System.Convert.ToBase64String(data));
                        writer.WriteEndElement();
                    }

                    writer.WriteEndElement();
                }

                writer.WriteEndElement();

                return;
            }

            var list = new List<PropertyInfo>();
            foreach (var property in properties)
            {
                var propertyValue = property.GetValue(item);
                if (property.CanRead && propertyValue != null)
                {
                    if (WriteAsAttribute(property))
                    {
                        writer.WriteAttributeString(property.Name, ToString(property, propertyValue));
                    }
                    else
                    {
                        list.Add(property);
                    }
                }
            }

            foreach (var property in list)
            {
                WriteObject(property, property.GetValue(item), writer, depth + 1, baggage);
            }

            writer.WriteEndElement();
        }

        string ToString(PropertyInfo info, object value)
        {
            if (value == null)
            {
                return string.Empty;
            }

            if (info.PropertyType.IsArray)
            {
                var elementType = info.PropertyType.GetElementType();
                if (elementType.IsEnum)
                {
                    return string.Join(",", ((IEnumerable)value).OfType<object>().Select(x => x.ToString()).ToArray());
                }
                else if (elementType.IsPrimitive)
                {
                    return string.Join(",", ((IEnumerable)value).OfType<object>().Select(x => x.ToString()).ToArray());
                }
                else
                {
                    throw new InvalidProgramException();
                }
            }
            else if (info.PropertyType.IsGenericType && info.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                return value.ToString();
            }
            else if (info.PropertyType.IsPrimitive)
            {
                return value.ToString();
            }
            else if (info.PropertyType.IsEnum)
            {
                return value.ToString();
            }
            else if (info.PropertyType == typeof(string))
            {
                return value.ToString();
            }
            else if (info.PropertyType == typeof(DateTime))
            {
                return ((DateTime)value).ToString("o");
            }
            else if (info.PropertyType == typeof(DateTimeOffset))
            {
                return ((DateTime)value).ToString("o");
            }
            else if (info.PropertyType == typeof(Guid))
            {
                return value.ToString();
            }

            throw new InvalidProgramException();
        }

        bool WriteAsAttribute(PropertyInfo info)
        {
            if (info.PropertyType.IsArray)
            {
                var elementType = info.PropertyType.GetElementType();
                if (elementType.IsEnum)
                {
                    return true;
                }
                else if (elementType == typeof(byte))
                {
                    return false;
                }
                else if (elementType.IsPrimitive)
                {
                    return true;
                }
                else if (elementType == typeof(string))
                {
                    return false;
                }
                else
                {
                    return false;
                }
            }
            else if (info.PropertyType.IsGenericType && info.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                return true;
            }
            else if (info.PropertyType.IsPrimitive)
            {
                return true;
            }
            else if (info.PropertyType.IsEnum)
            {
                return true;
            }
            else if (info.PropertyType == typeof(string))
            {
                return true;
            }
            else if (info.PropertyType == typeof(DateTime))
            {
                return true;
            }
            else if (info.PropertyType == typeof(DateTimeOffset))
            {
                return true;
            }
            else if (info.PropertyType == typeof(Guid))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        object DeserializeXml(Type type, string xml, string baggageTable)
        {
            using (var stringReader = new StringReader(xml))
            {
                var reader = new XmlTextReader(stringReader);
                reader.Read();
                return ReadObject(reader, null, 0, baggageTable);
            }
        }

        private object ReadObject(XmlTextReader reader, object parent, int depth, string baggageTable)
        {
            CheckIsStart(reader);

            try
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

                if (type.IsArray)
                {
                    var elementType = type.GetElementType();

                    if (elementType == typeof(string))
                    {
                        var listofstring = new List<string>();

                        if (reader.IsEmptyElement == false)
                        {
                            reader.ReadStartElement();
                            while (reader.Name == "string")
                            {
                                reader.ReadStartElement("string");
                                var text = reader.ReadContentAsString();
                                listofstring.Add(text);
                                reader.ReadEndElement();
                            }
                        }

                        parent.GetType().GetProperty(propertyName).SetValue(parent, listofstring.ToArray());
                    }
                    else if (elementType == typeof(byte))
                    {
                        reader.Read();
                        reader.MoveToAttribute("uri");
                        var uri = reader.Value;
                        var data = QuerySql<byte[]>($"DELETE FROM [{baggageTable}] OUTPUT deleted.[Data] WHERE Uri = @Param1", uri);

                        reader.MoveToElement();
                        reader.Read();

                        parent.GetType().GetProperty(propertyName).SetValue(parent, data);
                    }

                    return null;
                }
                else if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>))
                {
                    var parameters = type.GetGenericArguments();

                    var listofkv = new System.Collections.ArrayList();

                    if (reader.IsEmptyElement == false)
                    {
                        reader.Read();
                        while (reader.Name == "item")
                        {
                            string key = null;
                            string value = null;

                            if (reader.MoveToAttribute("key"))
                            {
                                key = reader.Value;
                            }

                            if (reader.MoveToAttribute("value"))
                            {
                                value = reader.Value;
                            }
                            else
                            {
                                reader.MoveToElement();
                                reader.Read();
                                //value = reader.ReadElementContentAsString();

                                reader.MoveToAttribute("uri");
                                var uri = reader.Value;
                                var buffer = QuerySql<byte[]>($"DELETE FROM [{baggageTable}] OUTPUT deleted.[Data] WHERE Uri = @Param1", uri);
                                value = System.Convert.ToBase64String(buffer);

                                reader.MoveToElement();
                                reader.Read();

                                reader.Read();
                            }

                            var kvtype = typeof(KeyValuePair<,>).MakeGenericType(parameters);
                            var kv = Activator.CreateInstance(kvtype, new[] { Convert(parameters[0], key), Convert(parameters[1], value) });
                            listofkv.Add(kv);
                        }
                    }

                    var dic = (IDictionary)Activator.CreateInstance(type);

                    foreach (dynamic item in listofkv)
                    {
                        dic.Add(item.Key, item.Value);
                    }

                    parent.GetType().GetProperty(propertyName).SetValue(parent, dic);

                    return null;
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

                if (reader.MoveToFirstAttribute())
                {
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
                }

                if (depth < 10)
                {
                    do
                    {
                        reader.MoveToElement();
                        reader.Read();

                        if (reader.Depth > depth)
                        {
                            var childobj = ReadObject(reader, obj, depth + 1, baggageTable);
                        }
                    } while (reader.Depth > depth);
                }

                return obj;
            }
            finally
            {
                CheckIsEnd(reader);
            }
        }

        [Conditional("Debug")]
        private static void CheckIsStart(XmlTextReader reader)
        {
            try
            {
                if (reader.NodeType != XmlNodeType.Element) throw new Exception();
            }
            catch
            {

            }
        }

        [Conditional("Debug")]
        private static void CheckIsEnd(XmlTextReader reader)
        {
            try
            {
                if (!(reader.EOF || reader.IsEmptyElement || reader.NodeType == XmlNodeType.EndElement)) throw new Exception();
            }
            catch
            {

            }
        }

        private T QuerySql<T>(string sql, string uri)
        {
            using (var connection = GetConnection())
            {
                GuaranteeOpen(connection);
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sql;
                    AddWithValue(command, "Param1", uri);

                    return (T)command.ExecuteScalar();
                }
            }
        }

        object Convert(Type target, string value)
        {
            if (target.IsArray)
            {
                var elementType = target.GetElementType();

                if (elementType == typeof(byte))
                {
                    var asbyte = System.Convert.FromBase64String(value);
                    return asbyte;
                }
                else if (elementType.IsEnum)
                {
                    var values = value.Split(',');
                    var cast = typeof(Enumerable).GetMethod("Cast").MakeGenericMethod(elementType);
                    var toarray = typeof(Enumerable).GetMethod("ToArray").MakeGenericMethod(elementType);
                    return toarray.Invoke(null, new[] { cast.Invoke(null, new[] { values.Select(x => Enum.Parse(elementType, x)) }) });
                }
                else if (elementType.IsPrimitive)
                {
                    var values = value.Split(',');

                    return values.Select(x =>
                    {
                        var parse = target.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static, null, new[] { typeof(string) }, null);
                        return parse.Invoke(null, new[] { value });
                    }).ToArray();
                }
                else
                {
                    throw new InvalidOperationException();
                }
            }
            else if (target.IsEnum)
            {
                return Enum.Parse(target, value);
            }
            else if (target == typeof(string))
            {
                return value;
            }
            else
            {
                if (target.IsGenericType && target.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    var nullabletarget = target.GetGenericArguments()[0];
                    var parse = nullabletarget.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static, null, new[] { typeof(string) }, null);
                    return Activator.CreateInstance(target, parse.Invoke(null, new[] { value }));
                }
                else
                {
                    var parse = target.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static, null, new[] { typeof(string) }, null);
                    return parse.Invoke(null, new[] { value });
                }
            }
        }

        T DeserializeXml<T>(string xml, string baggageTable)
        {
            return (T)DeserializeXml(typeof(T), xml, baggageTable);
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
