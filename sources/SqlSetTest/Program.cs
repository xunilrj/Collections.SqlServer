using MachinaAurum.Collections.SqlServer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlSetTest
{
    class Program
    {
        static void Main(string[] args)
        {
            string connectionString = null;
            string tableName = null;
            string[] columnsName = null;

            var parser = new Fclp.FluentCommandLineParser();
            parser.Setup<string>("connectionString").Callback(x => connectionString = x);
            parser.Setup<string>("tableName").Callback(x => tableName = x);
            parser.Setup<string>("columnsName").Callback(x => columnsName = x.Split(','));
            parser.Parse(args);

            Console.WriteLine("Connecting...");

            var parameters = new SqlSetParameters(connectionString, tableName, columnsName);
            var set = new SqlSet(parameters);
            set.CreateObjects();

            set.AddIfNotExists(new[] { new ItemDto() });
            set.AddIfNotExists(new[] { new ItemDto() { Int = 2 } });
        }

        public class ItemDto
        {
            public int Int { get; set; }
            public string Text { get; set; }

            public ItemDto()
            {
                Int = 1;
                Text = "TEXT";
            }
        }
    }
}
