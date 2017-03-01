using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MachinaAurum.Collections.SqlServer.Generic
{
    public interface IQueue : IQueueEnqueuer, IQueueDequeuer
    {
    }

    public interface IQueueEnqueuer
    {
        void Enqueue();
    }

    public interface IQueueDequeuer
    {
        void Dequeue();
    }
}
