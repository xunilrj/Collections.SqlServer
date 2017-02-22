using MachinaAurum.Collections.SqlServer;
using System;
using System.Diagnostics;
using System.Linq;

namespace SqlQueueTest
{
    class Program
    {
        static void Main(string[] args)
        {
            string connectionString = null;
            string serviceOrigin = null;
            string serviceDestination = null;
            string contract = null;
            string messageType = null;
            string queueOrigin = null;
            string queueDestination = null;
            string baggageTable = null;

            var parser = new Fclp.FluentCommandLineParser();
            parser.Setup<string>("connectionString").Callback(x => connectionString = x);
            parser.Setup<string>("serviceOrigin").Callback(x => serviceOrigin = x);
            parser.Setup<string>("serviceDestination").Callback(x => serviceDestination = x);
            parser.Setup<string>("contract").Callback(x => contract = x);
            parser.Setup<string>("messageType").Callback(x => messageType = x);
            parser.Setup<string>("queueOrigin").Callback(x => queueOrigin = x);
            parser.Setup<string>("queueDestination").Callback(x => queueDestination = x);
            parser.Setup<string>("baggageTable").Callback(x => baggageTable = x);
            parser.Parse(args);

            Console.WriteLine("Connecting...");

            var parameters = new SqlQueueParameters(connectionString, serviceOrigin, serviceDestination, contract, messageType, queueOrigin, queueDestination, baggageTable);
            var queue = new SqlQueue(parameters);
            queue.CreateObjects();

            queue.Clear();

            var item1 = new ItemDto(1);
            var item2 = new ItemDto(2);
            var item3 = new ItemDto(3);

            queue.Enqueue(item1);
            queue.Enqueue(item2);
            queue.Enqueue(item3);

            item1 = queue.Dequeue<ItemDto>();
            item2 = queue.Dequeue<ItemDto>();
            item3 = queue.Dequeue<ItemDto>();

            Console.WriteLine("item1.Id == 1");
            Debug.Assert(item1.Int == 1);
            Debug.Assert(item1.Long == 5);
            Debug.Assert(item1.Options.Count() == 2);
            Debug.Assert(item1.Options[0] == ENUM.A);
            Debug.Assert(item1.Options[1] == ENUM.B);
            Debug.Assert(item1.Strings.Count() == 2);
            Debug.Assert(item1.Strings[0] == "abc");
            Debug.Assert(item1.Strings[1] == "def");
            Debug.Assert(item1.UniqueID == Guid.Parse("c060ee98-2527-4a47-88cb-e65263ed4277"));
            Debug.Assert(System.Text.Encoding.UTF8.GetString(item1.VeryBigBuffer) == "VERYBIGTEXT");
            Console.WriteLine("item1.Id == 2");
            Debug.Assert(item2.Int == 2);
            Console.WriteLine("item1.Id == 3");
            Debug.Assert(item3.Int == 3);

            queue.Enqueue(item1);
            var items = queue.DequeueGroup();

            Debug.Assert(items.Count() == 1);
            Debug.Assert((items.Single() as ItemDto).Int == 1);

            Console.WriteLine("OK!");
        }
    }

    [Serializable]
    public class ItemDto
    {
        public int Int { get; set; }
        public long Long { get; set; }
        public double Double { get; set; }
        public float Float { get; set; }
        public string Text { get; set; }

        public Guid UniqueID { get; set; }

        public DateTime DateTime { get; set; }

        public ChildDto Child { get; set; }

        public ENUM[] Options { get; set; }
        public string[] Strings { get; set; }

        public byte[] VeryBigBuffer { get; set; }

        public ItemDto(int id)
        {
            Int = id;
            Long = 5;
            Float = 3.1f;
            Double = 4.2;
            Text = "TEXT";

            UniqueID = Guid.Parse("c060ee98-2527-4a47-88cb-e65263ed4277");
            DateTime = DateTime.UtcNow;

            Options = new[] { ENUM.A, ENUM.B };
            Strings = new[] { "abc", "def" };

            Child = new ChildDto(99);

            VeryBigBuffer = System.Text.Encoding.UTF8.GetBytes("VERYBIGTEXT");
        }
    }

    public enum ENUM
    {
        A, B, C
    }

    public class ChildDto
    {
        public int Int { get; set; }

        public Child2Dto Child2 { get; set; }

        public ChildDto(int id)
        {
            Int = id;
            Child2 = new Child2Dto("CHILD2");
        }
    }

    public class Child2Dto
    {
        public string Text { get; set; }
        public Child2Dto(string text)
        {
            Text = text;
        }
    }
}
