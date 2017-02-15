using MachinaAurum.Collections.SqlServer;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlDictionaryTest
{
    class Program
    {
        static void Main(string[] args)
        {
            string connectionString = null;
            string tableName = null;
            string keyName = null;
            string valueName = null;

            var parser = new Fclp.FluentCommandLineParser();
            parser.Setup<string>('c', "connectionString").Callback(x => connectionString = x);
            parser.Setup<string>('t', "tableName").Callback(x => tableName = x);
            parser.Setup<string>('k', "key").Callback(x => keyName = x);
            parser.Setup<string>('v', "value").Callback(x => valueName = x);
            parser.Parse(args);

            Console.WriteLine("Connecting...");

            var dic1 = new SqlDictionary<int, int>();
            dic1.Load(connectionString, "intint", keyName, valueName);
            dic1.Clear();
            dic1.Add(1, 2);
            dic1.Remove(1);
            dic1[1] = 3;
            dic1[1] = 4;

            var dic2 = new SqlDictionary<int, string>();
            dic2.Load(connectionString, "intstring", keyName, valueName);
            dic2.Clear();
            dic2.Add(1, "2");
            dic2.Remove(1);
            dic2[1] = "3";
            dic2[1] = "4";

            var dic3 = new SqlDictionary<string, string>();
            dic3.Load(connectionString, "stringstring", keyName, valueName);
            dic3.Clear();
            dic3.Add("1", "2");
            dic3.Remove("1");
            dic3["1"] = "3";
            dic3["1"] = "4";

            var dic4 = new SqlDictionary<string, SomeDto>();
            dic4.Load(connectionString, "stringdto", keyName, valueName);
            dic4.Clear();
            dic4.Add("1", new SomeDto() { Id = 1, Name = "SomeName" });
            dic4.Remove("1");
            dic4["1"] = new SomeDto() { Id = 2, Name = "SomeName" };
            dic4["1"] = new SomeDto() { Id = 3, Name = "SomeName" };
        }

        public class SomeDto
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }
    }
}
