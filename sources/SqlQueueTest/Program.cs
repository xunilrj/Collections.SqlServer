using MachinaAurum.Collections.SqlServer;
using System;
using System.Collections.Generic;
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

            var item1 = new ItemDto(1)
            {
                InnerDto = new BaseDto.InnerBaseDto(18, "InnerDtoText")
            };

            var item2 = new ItemDto(2);
            var item3 = new ItemDto(3);


            var ea = new SomeDomainEvent("a", "b", "c", new[] { "d" })
            {
                User = new DomainEventArgs.UserInfo(143,"name")
            };
            queue.Enqueue(ea);
            ea = queue.Dequeue<SomeDomainEvent>();
            Debug.Assert(ea.User.Id == 143);
            Debug.Assert(ea.User.Name == "name");

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
            Debug.Assert(System.Text.Encoding.UTF8.GetString(item1.DictionaryBuffers["buffer1"]) == "BUFFER1");
            Debug.Assert(System.Text.Encoding.UTF8.GetString(item1.DictionaryBuffers["buffer2"]) == "BUFFER2");
            Debug.Assert(item1.BaseInt == 98);
            Debug.Assert(item1.BaseChild.Int == 99);
            Debug.Assert(item1.InnerDto.InnerDtoInt == 18);
            Debug.Assert(item1.InnerDto.InnerDtoText == "InnerDtoText");
            Console.WriteLine("item1.Id == 2");
            Debug.Assert(item2.Int == 2);
            Console.WriteLine("item1.Id == 3");
            Debug.Assert(item3.Int == 3);

            queue.Enqueue(item1);
            var items = queue.DequeueGroup();

            Debug.Assert(items.Count() == 1);
            Debug.Assert((items.Single() as ItemDto).Int == 1);

            queue.Enqueue(new NullDto());
            var nullDto = queue.Dequeue<NullDto>();
            Debug.Assert(nullDto.Child == null);
            Debug.Assert(nullDto.DateTime == null);
            Debug.Assert(nullDto.DictionaryBuffers == null);
            Debug.Assert(nullDto.Double == null);
            Debug.Assert(nullDto.Float == null);
            Debug.Assert(nullDto.Int == null);
            Debug.Assert(nullDto.Float == null);
            Debug.Assert(nullDto.Long == null);
            Debug.Assert(nullDto.Options == null);
            Debug.Assert(nullDto.Strings == null);
            Debug.Assert(nullDto.Text == null);
            Debug.Assert(nullDto.UniqueID == null);
            Debug.Assert(nullDto.VeryBigBuffer == null);

            queue.Enqueue(new NullDto() { Int = 1 });
            nullDto = queue.Dequeue<NullDto>();
            Debug.Assert(nullDto.Child == null);
            Debug.Assert(nullDto.DateTime == null);
            Debug.Assert(nullDto.DictionaryBuffers == null);
            Debug.Assert(nullDto.Double == null);
            Debug.Assert(nullDto.Float == null);
            Debug.Assert(nullDto.Int.HasValue);
            Debug.Assert(nullDto.Int == 1);
            Debug.Assert(nullDto.Float == null);
            Debug.Assert(nullDto.Long == null);
            Debug.Assert(nullDto.Options == null);
            Debug.Assert(nullDto.Strings == null);
            Debug.Assert(nullDto.Text == null);
            Debug.Assert(nullDto.UniqueID == null);
            Debug.Assert(nullDto.VeryBigBuffer == null);

            queue.Enqueue(new NullDto() { Text = "SOMESTRING" });
            nullDto = queue.Dequeue<NullDto>();
            Debug.Assert(nullDto.Child == null);
            Debug.Assert(nullDto.DateTime == null);
            Debug.Assert(nullDto.DictionaryBuffers == null);
            Debug.Assert(nullDto.Double == null);
            Debug.Assert(nullDto.Float == null);
            Debug.Assert(nullDto.Int == null);
            Debug.Assert(nullDto.Float == null);
            Debug.Assert(nullDto.Long == null);
            Debug.Assert(nullDto.Options == null);
            Debug.Assert(nullDto.Strings == null);
            Debug.Assert(nullDto.Text == "SOMESTRING");
            Debug.Assert(nullDto.UniqueID == null);
            Debug.Assert(nullDto.VeryBigBuffer == null);

            Console.WriteLine("OK!");
        }
    }

    [Serializable]
    public class NullDto
    {
        public int? Int { get; set; }
        public long? Long { get; set; }
        public double? Double { get; set; }
        public float? Float { get; set; }
        public string Text { get; set; }
        public Guid? UniqueID { get; set; }
        public DateTime? DateTime { get; set; }
        public ChildDto Child { get; set; }
        public ENUM[] Options { get; set; }
        public string[] Strings { get; set; }
        public byte[] VeryBigBuffer { get; set; }
        public Dictionary<string, byte[]> DictionaryBuffers { get; set; }

        public NullDto()
        {
            Int = null;
            Long = null;
            Double = null;
            Float = null;
            Text = null;
            UniqueID = null;
            DateTime = null;
            Child = null;
            Options = null;
            Strings = null;
            VeryBigBuffer = null;
            DictionaryBuffers = null;
        }
    }

    public class BaseDto : EventArgs
    {
        public class InnerBaseDto
        {
            public int InnerDtoInt { get; private set; }
            public string InnerDtoText { get; private set; }

            public InnerBaseDto(int innerDtoInt, string innerDtoText)
            {
                InnerDtoInt = innerDtoInt;
                InnerDtoText = innerDtoText;
            }
        }

        public int BaseInt { get; set; }

        public ChildDto BaseChild { get; set; }

        public InnerBaseDto InnerDto { get; set; }

        public BaseDto()
        {
            BaseInt = 98;
            BaseChild = new ChildDto(99);
        }
    }

    [Serializable]
    public class ItemDto : BaseDto
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

        public Dictionary<string, byte[]> DictionaryBuffers { get; set; }

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

            DictionaryBuffers = new Dictionary<string, byte[]>();
            DictionaryBuffers.Add("buffer1", System.Text.Encoding.UTF8.GetBytes("BUFFER1"));
            DictionaryBuffers.Add("buffer2", System.Text.Encoding.UTF8.GetBytes("BUFFER2"));
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



    public class DomainEventArgs : EventArgs
    {
        public class UserInfo
        {
            public int Id { get; private set; }
            public string Name { get; private set; }

            public UserInfo(int id, string name)
            {
                Id = id;
                Name = name;
            }
        }

        public Guid Id { get; set; }
        public DateTime RaisedOn { get; set; }
        public UserInfo User { get; set; }

        public DomainEventArgs()
        {
            Id = Guid.NewGuid();
            RaisedOn = DateTime.UtcNow;
        }
    }

    [Serializable]
    public class SomeDomainEvent : DomainEventArgs
    {
        public string PropertyOne { get; private set; }
        public string PropertyTwo { get; private set; }

        public string[] Target1 { get; private set; }
        public string[] Target2 { get; private set; }
        public string[] Target3 { get; private set; }

        public Dictionary<string, byte[]> SomeDic { get; private set; }

        public string Data { get; private set; }

        public SomeDomainEvent(string propertyOne, string propertyTwo, string propertyThree, IEnumerable<string> target)
        {
            PropertyOne = propertyOne;
            PropertyTwo = propertyThree;
            Target1 = target.ToArray();
            Data = propertyTwo;

            Target2 = Enumerable.Empty<string>().ToArray();
            Target3 = Enumerable.Empty<string>().ToArray();
            SomeDic = new Dictionary<string, byte[]>();
        }
    }
}
