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

            var dic = new SqlDictionary<string, string>();
            dic.Load(connectionString, tableName, keyName, valueName);

            Console.WriteLine("Current Content");
            Console.WriteLine("---------------");
            foreach (var item in dic)
            {
                Console.WriteLine($"{item.Key}:{item.Value}");
            }
            Console.WriteLine("---------------");

            Console.WriteLine("Reading Commands");
            Console.WriteLine("---------------");

            var streamin = Console.OpenStandardInput();
            var reader = new StreamReader(streamin);
            while (reader.EndOfStream == false)
            {
                var line = reader.ReadLine();
                if (line == null || line.ToLower() == "end")
                {
                    return;
                }

                var p = line.Split(':');
                if (p[0].ToLower() == "clear")
                {
                    Console.WriteLine("Clearing...");
                    dic.Clear();
                }
                else if (p[0].ToLower() == "add")
                {
                    Console.WriteLine($"Adding {p[1]}:{p[2]}");
                    dic.Add(p[1], p[2]);
                }
                else if (p[0].ToLower() == "remove")
                {
                    Console.WriteLine($"Removing {p[1]}");
                    dic.Remove(p[1]);
                }
                else if (p[0].ToLower() == "set")
                {
                    Console.WriteLine($"Setting {p[1]}:{p[2]}");
                    dic[p[1]] = p[2];
                }
            }

            Console.WriteLine("---------------");
        }
    }
}
