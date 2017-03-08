using System.ComponentModel;

namespace MachinaAurum.Collections.SqlServer
{
    public class QueuItemEnvelope : INotifyPropertyChanged
    {
        object _Item;
        QueuItemStatus _Status;
        int _TriesCount;

        public object Item
        {
            get
            {
                return _Item;
            }
            set
            {
                _Item = value;
                RaiseNotifyPropertyChanged(nameof(Item));
            }
        }

        public QueuItemStatus Status
        {
            get
            {
                return _Status;
            }
            set
            {
                _Status = value;
                RaiseNotifyPropertyChanged(nameof(Status));
            }
        }

        public int TriesCount
        {
            get
            {
                return _TriesCount;
            }
            set
            {
                _TriesCount = value;
                RaiseNotifyPropertyChanged(nameof(TriesCount));
            }
        }

        public event PropertyChangedEventHandler PropertyChanged;

        public QueuItemEnvelope(object item)
        {
            Item = item;
            Status = QueuItemStatus.Enqueued;
        }

        void RaiseNotifyPropertyChanged(string propertyName)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        public void StartProcessing()
        {
            Status = QueuItemStatus.Processing;
            TriesCount++;
        }

        public void FinishProcessing()
        {
            Status = QueuItemStatus.Finished;
        }

        public void Fail()
        {
            Status = QueuItemStatus.Failed;
        }
    }

    public enum QueuItemStatus
    {
        Enqueued = 0,
        Processing = 1,
        Finished = 2,
        Failed = 3
    }
}
