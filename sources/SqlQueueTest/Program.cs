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

            var item1 = new ItemDto() { Id = 1 };
            var item2 = new ItemDto() { Id = 2 };
            var item3 = new ItemDto() { Id = 3 };

            queue.Enqueue(item1);
            queue.Enqueue(item2);
            queue.Enqueue(item3);

            item1 = queue.Dequeue<ItemDto>();
            item2 = queue.Dequeue<ItemDto>();
            item3 = queue.Dequeue<ItemDto>();

            Console.WriteLine("item1.Id == 1");
            Debug.Assert(item1.Id == 1);
            Console.WriteLine("item1.Id == 2");
            Debug.Assert(item2.Id == 2);
            Console.WriteLine("item1.Id == 3");
            Debug.Assert(item3.Id == 3);

            queue.Enqueue(item1);
            var items = queue.DequeueGroup();

            Debug.Assert(items.Count() == 1);
            Debug.Assert((items.Single() as ItemDto).Id == 1);

            Console.WriteLine("OK!");
        }
    }

    public class ItemDto
    {
        public int Id { get; set; }
    }
}
