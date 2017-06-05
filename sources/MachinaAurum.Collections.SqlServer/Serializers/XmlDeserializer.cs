using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Xml;

namespace MachinaAurum.Collections.SqlServer.Serializers
{
    public class XmlDeserializer
    {
        Func<string, byte[]> GetBaggage;

        public XmlDeserializer()
        {
            GetBaggage = x => new byte[0];
        }

        public XmlDeserializer(Func<string, byte[]> getBaggage)
        {
            GetBaggage = getBaggage;
        }

        public object Deserialize(string xml)
        {
            return DeserializeXml(xml);
        }

        public T Deserialize<T>(string xml)
        {
            return (T)DeserializeXml(xml);
        }

        object DeserializeXml(string xml)
        {
            using (var stringReader = new StringReader(xml))
            {
                var reader = new XmlTextReader(stringReader);
                reader.Read();
                return ReadObject(reader, null, 0);
            }
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

        private object ReadObject(XmlTextReader reader, object parent, int depth)
        {
            CheckIsStart(reader);

            try
            {
                string propertyName = reader.Name;

                Type type = null;

                if (parent == null)
                {
                    type = FindType(propertyName);

                    if (type == null)
                    {
                        throw new Exception($"Type {propertyName} not found. Check it has [Serializable] attribute.");
                    }
                }
                else
                {
                    if (reader.MoveToAttribute("TYPE"))
                    {
                        type = Type.GetType(reader.Value);
                        reader.MoveToElement();
                    }
                    else
                    {
                        type = parent.GetType().GetProperty(propertyName).PropertyType;
                    }
                }

                if (type.IsArray)
                {
                    return ReadArray(reader, parent, depth, propertyName, type);
                }
                else if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>))
                {
                    CheckIsStart(reader);
                    return ReadDictionary(reader, parent, depth, propertyName, type);
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

                ReadAttributes(reader, type, obj);
                ReadChildObjects(reader, depth, obj);

                return obj;
            }
            finally
            {
                CheckIsEnd(reader);
            }
        }

        private void ReadChildObjects(XmlTextReader reader, int depth, object obj)
        {
            if (depth < 10)
            {
                do
                {
                    reader.Read();

                    if (reader.Depth > depth)
                    {
                        CheckIsStart(reader);
                        var childobj = ReadObject(reader, obj, depth + 1);
                        CheckIsEnd(reader);
                    }
                } while (reader.Depth > depth);
            }
        }

        private void ReadAttributes(XmlTextReader reader, Type type, object obj)
        {
            CheckIsStart(reader);

            if (reader.MoveToFirstAttribute())
            {
                do
                {
                    var attributeName = reader.Name;
                    var value = reader.Value;

                    var property = type.GetProperty(attributeName);
                    if (property != null && property.CanWrite)
                    {
                        property.SetValue(obj, Convert(property.PropertyType, value));
                    }
                } while (reader.MoveToNextAttribute());
            }
            reader.MoveToElement();

            CheckIsStart(reader);
        }

        private object ReadArray(XmlTextReader reader, object parent, int depth, string propertyName, Type type)
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
                var data = GetBaggage(uri);// QuerySql<byte[]>($"DELETE FROM [{baggageTable}] OUTPUT deleted.[Data] WHERE Uri = @Param1", uri, connection, transaction);

                reader.MoveToElement();
                reader.Read();

                parent.GetType().GetProperty(propertyName).SetValue(parent, data);
            }
            else
            {
                var list = new ArrayList();

                reader.Read();

                while (reader.Depth != depth)
                {
                    var arrayitem = ReadObject(reader, null, depth + 1);
                    list.Add(arrayitem);
                }

                var cast = typeof(Enumerable).GetMethod("Cast").MakeGenericMethod(elementType);
                var toarray = typeof(Enumerable).GetMethod("ToArray").MakeGenericMethod(elementType);
                var propertyValue = toarray.Invoke(null, new[] { cast.Invoke(null, new[] { list }) });

                parent.GetType().GetProperty(propertyName).SetValue(parent, propertyValue);
            }

            return null;
        }

        private object ReadDictionary(XmlTextReader reader, object parent, int depth, string propertyName, Type type)
        {
            CheckIsStart(reader);

            try
            {

                var parameters = type.GetGenericArguments();

                var listofkv = new System.Collections.ArrayList();

                if (reader.IsEmptyElement == false)
                {
                    reader.Read();
                    while (reader.Name == "item")
                    {
                        object key = ReadKey(reader, parameters[0], depth);
                        object value = ReadValue(parameters[1], reader, depth);

                        var kvtype = typeof(KeyValuePair<,>).MakeGenericType(parameters);
                        var kv = Activator.CreateInstance(kvtype, new[] { key, value });
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
            finally
            {
                CheckIsEnd(reader);
            }
        }

        private object ReadValue(Type valueType, XmlTextReader reader, int depth)
        {
            CheckIsItemStart(reader);

            var startDepth = reader.Depth;

            object value;
            if (reader.MoveToAttribute("value"))
            {
                //no value
                //<item ... value="..." />
                value = reader.Value;
                reader.MoveToElement(); // read back to item

                value = Convert(valueType, value.ToString());
                reader.Read(); // read item
                // reader = <item />
            }
            else
            {
                //with value
                //<item>
                //  <value>
                reader.MoveToElement();
                reader.Read(); //read item or read key

                //reader at value
                if (reader.MoveToAttribute("uri"))
                {
                    var uri = reader.Value;
                    value = GetBaggage(uri);// QuerySql<byte[]>($"DELETE FROM [{baggageTable}] OUTPUT deleted.[Data] WHERE Uri = @Param1", uri, connection, transaction);

                    reader.MoveToElement(); // read back to value
                    reader.Read(); //read value end
                }
                else if (valueType == typeof(string))
                {
                    reader.Read(); //read value start
                    value = reader.ReadContentAsString(); //read CDATA
                    reader.Read(); //read value end
                }
                else
                {
                    reader.Read(); //read value start
                    value = ReadObject(reader, null, depth + 2);
                    reader.Read(); //read value end
                }

                reader.Read(); //read item end
            }

            var isAnotherItem = (reader.Name == "item");
            var isUp = reader.Depth < startDepth;
            if (!isAnotherItem && !isUp) throw new Exception();

            return value;
        }

        private object ReadKey(XmlTextReader reader, Type keyType, int depth)
        {
            CheckIsItemStart(reader);

            object key = null;

            if (reader.MoveToAttribute("key"))
            {
                key = Convert(keyType, reader.Value);
            }
            else
            {
                reader.Read(); // read item
                reader.Read(); // read key
                key = ReadObject(reader, null, depth + 2);
            }

            reader.MoveToElement();

            CheckIsItemStart(reader);

            return key;
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
                    var listSeparator = CultureInfo.InvariantCulture.TextInfo.ListSeparator;
                    var values = value.Split(new string[] { listSeparator }, StringSplitOptions.None);

                    var cast = typeof(Enumerable).GetMethod("Cast").MakeGenericMethod(elementType);
                    var toarray = typeof(Enumerable).GetMethod("ToArray").MakeGenericMethod(elementType);
                    var objects = values.Select(x => Enum.Parse(elementType, x));

                    return toarray.Invoke(null, new[] { cast.Invoke(null, new[] { objects }) });
                }
                else if (elementType.IsPrimitive)
                {
                    var listSeparator = CultureInfo.InvariantCulture.TextInfo.ListSeparator;
                    var values = value.Split(new string[] { listSeparator }, StringSplitOptions.None);

                    var cast = typeof(Enumerable).GetMethod("Cast").MakeGenericMethod(elementType);
                    var toarray = typeof(Enumerable).GetMethod("ToArray").MakeGenericMethod(elementType);
                    var objects = values.Select(x =>
                    {
                        var parse = elementType.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static, null, new[] { typeof(string), typeof(IFormatProvider) }, null);
                        return parse.Invoke(null, new object[] { x, CultureInfo.InvariantCulture });
                    });

                    return toarray.Invoke(null, new[] { cast.Invoke(null, new[] { objects }) });
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

        //[Conditional("Debug")]
        private static void CheckIsItemStart(XmlTextReader reader)
        {
            if ((reader.NodeType != XmlNodeType.Element && reader.Name == "item") || (reader.NodeType != XmlNodeType.EndElement && reader.Name == "key")) throw new Exception("reader.NodeType != XmlNodeType.Element && reader.Name == \"item\"");
        }

        //[Conditional("Debug")]
        private static void CheckIsItemEnd(XmlTextReader reader)
        {
            if ((reader.NodeType != XmlNodeType.Element && reader.Name == "item") || (reader.NodeType != XmlNodeType.EndElement && reader.Name == "item")) throw new Exception("reader.NodeType != XmlNodeType.EndElement && reader.Name == \"value\"");
        }

        //[Conditional("Debug")]
        private static void CheckIsStart(XmlTextReader reader)
        {
            if (reader.NodeType != XmlNodeType.Element) throw new Exception("reader.NodeType != XmlNodeType.Element");
        }

        //[Conditional("Debug")]
        private static void CheckIsEnd(XmlTextReader reader)
        {
            if (!(reader.EOF || reader.IsEmptyElement || reader.NodeType == XmlNodeType.EndElement)) throw new Exception("!(reader.EOF || reader.IsEmptyElement || reader.NodeType == XmlNodeType.EndElement)");
        }
    }
}
