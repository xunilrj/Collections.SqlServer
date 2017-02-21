using System.Collections.Generic;
using Xunit;

namespace MachinaAurum.Collections.SqlServer.Tests
{
    public class SqlQueueTests
    {
        [Fact]
        public void SqlQueueMustEnqueueItemsOnSqlServer()
        {
            Dictionary<string, Queue<object>> queues;
            SqlQueue queue;
            CreateQueue(out queues, out queue);

            queue.Enqueue(2);

            Assert.Equal(2, queues["QUEUEDESTINATION"].Count);
        }

        [Fact]
        public void SqlQueueMustDequeueItemsOnSqlServer()
        {
            Dictionary<string, Queue<object>> queues;
            SqlQueue queue;
            CreateQueue(out queues, out queue);

            var item = queue.Dequeue<int>();
            Assert.Equal(1, item);
            Assert.Equal(0, queues["QUEUEDESTINATION"].Count);
        }

        private static void CreateQueue(out Dictionary<string, Queue<object>> queues, out SqlQueue queue)
        {
            var q = new Queue<object>();
            q.Enqueue(1);
            queues = new Dictionary<string, Queue<object>>();
            queues.Add("QUEUEDESTINATION", q);
            var fake = new FakeSqlServerBroker(queues, "QUEUEDESTINATION");

            queue = new SqlQueue(fake, "SERVICEORIGIN", "SERVICEDESTINATION", "CONTRACT", "MESSAGETYPE", "QUEUEDESTINATION");
        }
    }
}
