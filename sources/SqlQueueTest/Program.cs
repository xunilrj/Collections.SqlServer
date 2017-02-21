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
            string queueDestination = null;

            var parser = new Fclp.FluentCommandLineParser();
            parser.Setup<string>('c', "connectionString").Callback(x => connectionString = x);
            parser.Setup<string>('o', "serviceOrigin").Callback(x => serviceOrigin = x);
            parser.Setup<string>('d', "serviceDestination").Callback(x => serviceDestination = x);
            parser.Setup<string>('n', "contract").Callback(x => contract = x);
            parser.Setup<string>('t', "messageType").Callback(x => messageType = x);
            parser.Setup<string>('q', "queueDestination").Callback(x => queueDestination = x);
            parser.Parse(args);

            Console.WriteLine("Connecting...");

            var queue = new SqlQueue(connectionString, serviceOrigin, serviceDestination, contract, messageType, queueDestination);
            queue.CreateObjects("QUEUEORIGIN");

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

        public DateTime DateTime { get; set; }

        public ChildDto Child { get; set; }

        public ENUM[] Options { get; set; }

        public ItemDto(int id)
        {
            Int = id;
            Long = 5;
            Float = 3.1f;
            Double = 4.2;
            Text = "TEXT";

            DateTime = DateTime.UtcNow;

            Options = new[] { ENUM.A, ENUM.B };

            Child = new ChildDto(99);
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
