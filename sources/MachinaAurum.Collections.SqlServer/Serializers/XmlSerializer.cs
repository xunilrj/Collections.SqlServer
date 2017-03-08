using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Xml;

namespace MachinaAurum.Collections.SqlServer.Serializers
{
    public class XmlSerializer
    {
        public string SerializeXml(object item)
        {
            var baggage = new Dictionary<string, byte[]>();
            return SerializeXml(item, baggage);
        }

        public string SerializeXml(object item, IDictionary<string, byte[]> baggage)
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
                else
                {
                    foreach (var arrayitem in (IEnumerable<object>)item)
                    {
                        WriteObject(null, arrayitem, writer, depth + 1, baggage);
                    }
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
                    WriteKey(writer, depth, baggage, genericArguments, i);
                    WriteValue(writer, depth, baggage, genericArguments, i);
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
                    if (WriteAsAttribute(property.PropertyType))
                    {
                        writer.WriteAttributeString(property.Name, ToString(property.PropertyType, propertyValue));
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

        private void WriteValue(XmlTextWriter writer, int depth, IDictionary<string, byte[]> baggage, Type[] genericArguments, dynamic i)
        {
            if (genericArguments[1] == typeof(string))
            {
                writer.WriteStartElement("value");
                writer.WriteCData(i.Value);
                writer.WriteEndElement();
            }
            else if (WriteAsAttribute(genericArguments[1]))
            {
                writer.WriteAttributeString("value", ToString(genericArguments[1], i.Value));
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
            else
            {
                writer.WriteStartElement("value");
                WriteObject(null, i.Value, writer, depth + 1, baggage);
                writer.WriteEndElement();
            }
        }

        private void WriteKey(XmlTextWriter writer, int depth, IDictionary<string, byte[]> baggage, Type[] genericArguments, dynamic i)
        {
            if (WriteAsAttribute(genericArguments[0]))
            {
                writer.WriteAttributeString("key", ToString(genericArguments[0], i.Key));
            }
            else if (genericArguments[0] == typeof(string))
            {
                writer.WriteAttributeString("key", ToString(genericArguments[0], i.Key));
            }
            else
            {
                writer.WriteStartElement("key");
                WriteObject(null, i.Key, writer, depth + 1, baggage);
                writer.WriteEndElement();
            }
        }

        bool WriteAsAttribute(Type type)
        {
            if (type.IsArray)
            {
                var elementType = type.GetElementType();
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
            else if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                return true;
            }
            else if (type.IsPrimitive)
            {
                return true;
            }
            else if (type.IsEnum)
            {
                return true;
            }
            else if (type == typeof(string))
            {
                return true;
            }
            else if (type == typeof(DateTime))
            {
                return true;
            }
            else if (type == typeof(DateTimeOffset))
            {
                return true;
            }
            else if (type == typeof(Guid))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        string ToString(Type propertyType, object value)
        {
            if (value == null)
            {
                return string.Empty;
            }

            if (propertyType.IsArray)
            {
                var elementType = propertyType.GetElementType();
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
            else if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                return value.ToString();
            }
            else if (propertyType.IsPrimitive)
            {
                return value.ToString();
            }
            else if (propertyType.IsEnum)
            {
                return value.ToString();
            }
            else if (propertyType == typeof(string))
            {
                return value.ToString();
            }
            else if (propertyType == typeof(DateTime))
            {
                return ((DateTime)value).ToString("o");
            }
            else if (propertyType == typeof(DateTimeOffset))
            {
                return ((DateTime)value).ToString("o");
            }
            else if (propertyType == typeof(Guid))
            {
                return value.ToString();
            }

            throw new InvalidProgramException();
        }
    }
}
