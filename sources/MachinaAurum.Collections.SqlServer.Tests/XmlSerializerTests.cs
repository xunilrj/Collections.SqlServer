using MachinaAurum.Collections.SqlServer.Serializers;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace MachinaAurum.Collections.SqlServer.Tests
{
    public class XmlSerializerTests
    {
        [Fact]
        public void MustWorkForPrimitives()
        {
            var dto = new PrimitivesDto()
            {
                Int = 1,
                Float = 2.0f,
                Double = 3.0,
                String = "SOMESTRING",
                Guid = Guid.Parse("6f501719-4312-45dd-9f94-2fd5574537fa")
            };

            var xmlSerializer = new XmlSerializer();
            var xml = xmlSerializer.SerializeXml(dto);

            Assert.Equal("<PrimitivesDto Int=\"1\" Float=\"2\" Double=\"3\" String=\"SOMESTRING\" Guid=\"6f501719-4312-45dd-9f94-2fd5574537fa\" />", xml);
        }

        [Fact]
        public void MustWorkForEnums()
        {
            var dto = new EnumDto()
            {
                Enum = SomeEnum.EnumValue2
            };

            var xmlSerializer = new XmlSerializer();
            var xml = xmlSerializer.SerializeXml(dto);

            Assert.Equal("<EnumDto Enum=\"EnumValue2\" />", xml);
        }

        [Fact]
        public void MustWorkForPrimitivesArrays()
        {
            var dto = new PrimitivesArrayDto()
            {
                Int = new[] { 1, 2 },
                Float = new[] { 2.0f, 3.0f },
                Double = new[] { 4.0, 5.0 },
                String = new[] { "SOMESTRING1", "SOMESTRING2" }
            };

            var xmlSerializer = new XmlSerializer();
            var xml = xmlSerializer.SerializeXml(dto);

            Assert.Equal("<PrimitivesArrayDto Int=\"1,2\" Float=\"2,3\" Double=\"4,5\"><String><string><![CDATA[SOMESTRING1]]></string><string><![CDATA[SOMESTRING2]]></string></String></PrimitivesArrayDto>", xml);
        }

        [Fact]
        public void MustWorkForEnumArrays()
        {
            var dto = new EnumArrayDto()
            {
                Enums = new[] { SomeEnum.EnumValue1, SomeEnum.EnumValue2 }
            };

            var xmlSerializer = new XmlSerializer();
            var xml = xmlSerializer.SerializeXml(dto);

            Assert.Equal("<EnumArrayDto Enums=\"EnumValue1,EnumValue2\" />", xml);
        }

        [Fact]
        public void MustWorkForByteArrays()
        {
            var dto = new ByteBufferDto()
            {
                Buffer = new byte[] { 1, 2, 3 }
            };

            var baggage = new Dictionary<string, byte[]>();
            var xmlSerializer = new XmlSerializer();
            var xml = xmlSerializer.SerializeXml(dto, baggage);

            Assert.Matches("<ByteBufferDto><Buffer><proxy uri=\"baggage:\\/\\/........-....-....-....-............\" \\/><\\/Buffer><\\/ByteBufferDto>", xml);
            Assert.Equal(1, baggage.Count);
            Assert.Contains(baggage.First().Key, xml);
            Assert.Equal(baggage.First().Value, dto.Buffer);
        }

        [Fact]
        public void MustWorkForDictionaries()
        {
            var dto = new DictionariesDto()
            {
                StringString = new Dictionary<string, string>()
                {
                    ["key1"] = "value1",
                    ["key2"] = "value2"
                },
                StringInt = new Dictionary<string, int>()
                {
                    ["key1"] = 10,
                    ["key2"] = 20
                },
                IntString = new Dictionary<int, string>()
                {
                    [1] = "value1",
                    [2] = "value2"
                },
                IntInt = new Dictionary<int, int>()
                {
                    [1] = 10,
                    [2] = 20
                }
            };

            var xmlSerializer = new XmlSerializer();
            var xml = xmlSerializer.SerializeXml(dto);

            Assert.Equal("<DictionariesDto><StringString><item key=\"key1\"><value><![CDATA[value1]]></value></item><item key=\"key2\"><value><![CDATA[value2]]></value></item></StringString><StringInt><item key=\"key1\" value=\"10\" /><item key=\"key2\" value=\"20\" /></StringInt><IntString><item key=\"1\"><value><![CDATA[value1]]></value></item><item key=\"2\"><value><![CDATA[value2]]></value></item></IntString><IntInt><item key=\"1\" value=\"10\" /><item key=\"2\" value=\"20\" /></IntInt></DictionariesDto>", xml);
        }

        [Fact]
        public void MustWorkForDictionariesEnumDto()
        {
            var dto = new DictionariesEnumDto()
            {
                SomeEnumString = new Dictionary<SomeEnum, string>()
                {
                    [SomeEnum.EnumValue1] = "SOMEVALUE1",
                    [SomeEnum.EnumValue2] = "SOMEVALUE2"
                },
                StringSomeEnum = new Dictionary<string, SomeEnum>()
                {
                    ["SOMEVALUE1"] = SomeEnum.EnumValue1,
                    ["SOMEVALUE2"] = SomeEnum.EnumValue2,
                },
                SomeEnumSomeEnum = new Dictionary<SomeEnum, SomeEnum>()
                {
                    [SomeEnum.EnumValue1] = SomeEnum.EnumValue1,
                    [SomeEnum.EnumValue2] = SomeEnum.EnumValue2
                }
            };

            var xmlSerializer = new XmlSerializer();
            var xml = xmlSerializer.SerializeXml(dto);

            Assert.Equal("<DictionariesEnumDto><SomeEnumString><item key=\"EnumValue1\"><value><![CDATA[SOMEVALUE1]]></value></item><item key=\"EnumValue2\"><value><![CDATA[SOMEVALUE2]]></value></item></SomeEnumString><StringSomeEnum><item key=\"SOMEVALUE1\" value=\"EnumValue1\" /><item key=\"SOMEVALUE2\" value=\"EnumValue2\" /></StringSomeEnum><SomeEnumSomeEnum><item key=\"EnumValue1\" value=\"EnumValue1\" /><item key=\"EnumValue2\" value=\"EnumValue2\" /></SomeEnumSomeEnum></DictionariesEnumDto>", xml);
        }

        [Fact]
        public void MustWorkForDictionaryStringByteDto()
        {
            var dto = new DictionaryStringByteDto()
            {
                StringByteBuffer = new Dictionary<string, byte[]>()
                {
                    ["KEY1"] = new byte[] { 1, 2, 3 },
                    ["KEY2"] = new byte[] { 4, 5, 6 }
                }
            };

            var baggage = new Dictionary<string, byte[]>();
            var xmlSerializer = new XmlSerializer();
            var xml = xmlSerializer.SerializeXml(dto, baggage);

            Assert.Matches("<DictionaryStringByteDto><StringByteBuffer><item key=\"KEY1\"><value uri=\"baggage:\\/\\/........-....-....-....-............\" \\/><\\/item><item key=\"KEY2\"><value uri=\"baggage:\\/\\/........-....-....-....-............\" \\/><\\/item><\\/StringByteBuffer><\\/DictionaryStringByteDto>", xml);
            Assert.Equal(2, baggage.Count);

            Assert.Contains(baggage, x => x.Value == dto.StringByteBuffer["KEY1"]);
            Assert.Contains(baggage, x => x.Value == dto.StringByteBuffer["KEY2"]);
        }

        [Fact]
        public void MustWorkForTreeOfObjects()
        {
            var dto = new RootDto()
            {
                Leaf = new LeafDto()
                {
                    Id = 12
                }
            };

            var xmlSerializer = new XmlSerializer();
            var xml = xmlSerializer.SerializeXml(dto);

            Assert.Matches("<RootDto><Leaf Id=\"12\" /></RootDto>", xml);
        }

        [Fact]
        public void MustWorkForClassesWithBase()
        {
            var dto = new ClassWithBaseDto()
            {
                Leaf = new LeafDto()
                {
                    Id = 12
                }
            };

            var xmlSerializer = new XmlSerializer();
            var xml = xmlSerializer.SerializeXml(dto);

            Assert.Matches("<ClassWithBaseDto><Leaf Id=\"12\" /></ClassWithBaseDto>", xml);
        }

        [Fact]
        public void MustWorkForClassesWithClassArray()
        {
            var dto = new ClassWithArrayOfClass()
            {
                Leafs = new[]
                {
                    new LeafDto() { Id = 2 },
                    new LeafDto() { Id = 3 }
                }
            };

            var xmlSerializer = new XmlSerializer();
            var xml = xmlSerializer.SerializeXml(dto);

            Assert.Matches("<ClassWithArrayOfClass><Leafs><LeafDto Id=\"2\" /><LeafDto Id=\"3\" /></Leafs></ClassWithArrayOfClass>", xml);
        }

        [Fact]
        public void MustWorkForDictionaryWithClass()
        {
            var dto = new DictionaryWithClass()
            {
                StringLeafDto = new Dictionary<string, LeafDto>()
                {
                    ["LEAF1"] = new LeafDto() { Id = 5 },
                    ["LEAF2"] = new LeafDto() { Id = 6 }
                },
                LeafDtoString = new Dictionary<LeafDto, string>()
                {
                    [new LeafDto() { Id = 5 }] = "VALUE1",
                    [new LeafDto() { Id = 6 }] = "VALUE2"
                }
            };

            var xmlSerializer = new XmlSerializer();
            var xml = xmlSerializer.SerializeXml(dto);

            Assert.Equal("<DictionaryWithClass><StringLeafDto><item key=\"LEAF1\"><value><LeafDto Id=\"5\" /></value></item><item key=\"LEAF2\"><value><LeafDto Id=\"6\" /></value></item></StringLeafDto><LeafDtoString><item><key><LeafDto Id=\"5\" /></key><value><![CDATA[VALUE1]]></value></item><item><key><LeafDto Id=\"6\" /></key><value><![CDATA[VALUE2]]></value></item></LeafDtoString></DictionaryWithClass>", xml);
        }
    }

    [Serializable]
    public class PrimitivesDto
    {
        public int Int { get; set; }
        public float Float { get; set; }
        public double Double { get; set; }
        public string String { get; set; }
        public Guid Guid { get; set; }
    }

    public enum SomeEnum
    {
        EnumValue1,
        EnumValue2
    }

    [Serializable]
    public class EnumDto
    {
        public SomeEnum Enum { get; set; }
    }

    [Serializable]
    public class PrimitivesArrayDto
    {
        public int[] Int { get; set; }
        public float[] Float { get; set; }
        public double[] Double { get; set; }
        public string[] String { get; set; }
    }

    [Serializable]
    public class EnumArrayDto
    {
        public SomeEnum[] Enums { get; set; }
    }

    [Serializable]
    public class ByteBufferDto
    {
        public byte[] Buffer { get; set; }
    }

    [Serializable]
    public class DictionariesDto
    {
        public Dictionary<string, string> StringString { get; set; }
        public Dictionary<string, int> StringInt { get; set; }
        public Dictionary<int, string> IntString { get; set; }
        public Dictionary<int, int> IntInt { get; set; }
    }

    [Serializable]
    public class DictionariesEnumDto
    {
        public Dictionary<SomeEnum, string> SomeEnumString { get; set; }
        public Dictionary<string, SomeEnum> StringSomeEnum { get; set; }
        public Dictionary<SomeEnum, SomeEnum> SomeEnumSomeEnum { get; set; }
    }

    [Serializable]
    public class DictionaryStringByteDto
    {
        public Dictionary<string, byte[]> StringByteBuffer { get; set; }
    }

    [Serializable]
    public class RootDto
    {
        public LeafDto Leaf { get; set; }
    }

    [Serializable]
    public class LeafDto
    {
        public int Id { get; set; }
    }

    [Serializable]
    public class ClassWithBaseDto : RootDto
    {

    }

    [Serializable]
    public class ClassWithArrayOfClass
    {
        public LeafDto[] Leafs { get; set; }
    }

    [Serializable]
    public class DictionaryWithClass
    {
        public Dictionary<string, LeafDto> StringLeafDto { get; set; }
        public Dictionary<LeafDto, string> LeafDtoString { get; set; }
    }
}
