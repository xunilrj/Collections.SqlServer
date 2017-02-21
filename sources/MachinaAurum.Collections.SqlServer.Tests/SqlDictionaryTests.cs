using System;
using System.Collections.Generic;
using Xunit;

namespace MachinaAurum.Collections.SqlServer.Tests
{
    public class SqlDictionaryTests
    {
        [Fact]
        public void SqlDictionaryMustLoadValuesFromSqlServer()
        {
            var dic = CreateDictionary();

            Assert.Equal("value1", dic["key1"]);
        }

        [Fact]
        public void SqlDictionaryMustSaveKeyValueToSqlServer()
        {
            IDictionary<string, string> inner;
            var dic = CreateDictionary(out inner);

            dic.Add("newKey", "newValue");

            Assert.Equal("newValue", dic["newKey"]);
            Assert.Equal("newValue", inner["newKey"]);
        }

        [Fact]
        public void SqlDictionaryMustInsertKeyUsingIndex()
        {
            IDictionary<string, string> inner;
            var dic = CreateDictionary(out inner);

            dic["newKey"] = "newValue";

            Assert.Equal("newValue", dic["newKey"]);
            Assert.Equal("newValue", inner["newKey"]);
        }

        [Fact]
        public void SqlDictionaryMustUpdateKeyUsingIndex()
        {
            IDictionary<string, string> inner;
            var dic = CreateDictionary(out inner);

            dic["key1"] = "newValue";

            Assert.Equal("newValue", dic["key1"]);
            Assert.Equal("newValue", inner["key1"]);
        }

        [Fact]
        public void SqlDictionaryMustRemoveKeyValueFromSqlServer()
        {
            IDictionary<string, string> inner;
            var dic = CreateDictionary(out inner);

            dic.Remove("key1");

            Assert.False(dic.ContainsKey("key1"));
            Assert.False(inner.ContainsKey("key1"));
        }

        [Fact]
        public void SqlDictionaryMustClearValueFromSqlServer()
        {
            IDictionary<string, string> inner;
            var dic = CreateDictionary(out inner);

            dic.Clear();

            Assert.Equal(0, dic.Count);
            Assert.Equal(0, inner.Count);
        }

        private static SqlDictionary<string, string> CreateDictionary()
        {
            var inner = new Dictionary<string, string>();
            inner.Add("key1", "value1");
            var server = new FakeSqlServer<string, string>(inner);
            var dic = new SqlDictionary<string, string>(server);
            dic.Load("SOMECONNECTIONSTRING", "SOMETABLE", "Key", "Value");
            return dic;
        }

        private static SqlDictionary<string, string> CreateDictionary(out IDictionary<string, string> inner)
        {
            inner = new Dictionary<string, string>();
            inner.Add("key1", "value1");
            var server = new FakeSqlServer<string, string>(inner);
            var dic = new SqlDictionary<string, string>(server);
            dic.Load("SOMECONNECTIONSTRING", "SOMETABLE", "Key", "Value");
            return dic;
        }
    }
}
