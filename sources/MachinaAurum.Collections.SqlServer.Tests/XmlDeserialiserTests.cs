using MachinaAurum.Collections.SqlServer.Serializers;
using System;
using System.Linq;
using Xunit;

namespace MachinaAurum.Collections.SqlServer.Tests
{
    public class XmlDeserialiserTests
    {
        [Fact]
        public void MustWorkForPrimitives()
        {
            var xml = "<PrimitivesDto Int=\"1\" Float=\"2\" Double=\"3\" String=\"SOMESTRING\" Guid=\"6f501719-4312-45dd-9f94-2fd5574537fa\" />";

            var deserializer = new XmlDeserializer();
            var dto = deserializer.Deserialize<PrimitivesDto>(xml);

            Assert.Equal(1, dto.Int);
            Assert.Equal(2.0f, dto.Float);
            Assert.Equal(3.0, dto.Double);
            Assert.Equal("SOMESTRING", dto.String);
            Assert.Equal(Guid.Parse("6f501719-4312-45dd-9f94-2fd5574537fa"), dto.Guid);
        }

        [Fact]
        public void MustWorkForEnums()
        {
            var xml = "<EnumDto Enum=\"EnumValue2\" />";

            var deserializer = new XmlDeserializer();
            var dto = deserializer.Deserialize<EnumDto>(xml);

            Assert.Equal(SomeEnum.EnumValue2, dto.Enum);
        }

        [Fact]
        public void MustWorkForPrimitivesArrays()
        {
            var xml = "<PrimitivesArrayDto Int=\"1,2\" Float=\"2,3\" Double=\"4,5\"><String><string><![CDATA[SOMESTRING1]]></string><string><![CDATA[SOMESTRING2]]></string></String></PrimitivesArrayDto>";

            var deserializer = new XmlDeserializer();
            var dto = deserializer.Deserialize<PrimitivesArrayDto>(xml);

            Assert.Equal(1, dto.Int[0]);
            Assert.Equal(2, dto.Int[1]);
            Assert.Equal(2.0f, dto.Float[0]);
            Assert.Equal(3.0f, dto.Float[1]);
            Assert.Equal(4.0, dto.Double[0]);
            Assert.Equal(5.0, dto.Double[1]);
            Assert.Equal("SOMESTRING1", dto.String[0]);
            Assert.Equal("SOMESTRING2", dto.String[1]);
        }

        [Fact]
        public void MustWorkForEnumArrays()
        {
            var xml = "<EnumArrayDto Enums=\"EnumValue1,EnumValue2\" />";

            var deserializer = new XmlDeserializer();
            var dto = deserializer.Deserialize<EnumArrayDto>(xml);

            Assert.Equal(SomeEnum.EnumValue1, dto.Enums[0]);
            Assert.Equal(SomeEnum.EnumValue2, dto.Enums[1]);
        }

        [Fact]
        public void MustWorkForByteArrays()
        {
            var xml = "<ByteBufferDto><Buffer><proxy uri=\"baggage://00000000-0000-0000-0000-000000000000\" /></Buffer></ByteBufferDto>";

            var deserializer = new XmlDeserializer(x =>
            {
                Assert.Equal("baggage://00000000-0000-0000-0000-000000000000", x);
                return new byte[3] { 1, 2, 3 };
            });
            var dto = deserializer.Deserialize<ByteBufferDto>(xml);

            Assert.Equal(1, dto.Buffer[0]);
            Assert.Equal(2, dto.Buffer[1]);
            Assert.Equal(3, dto.Buffer[2]);
        }

        [Fact]
        public void MustWorkForDictionaries()
        {
            var xml = "<DictionariesDto><StringString><item key=\"key1\"><value><![CDATA[value1]]></value></item><item key=\"key2\"><value><![CDATA[value2]]></value></item></StringString><StringInt><item key=\"key1\" value=\"10\" /><item key=\"key2\" value=\"20\" /></StringInt><IntString><item key=\"1\"><value><![CDATA[value1]]></value></item><item key=\"2\"><value><![CDATA[value2]]></value></item></IntString><IntInt><item key=\"1\" value=\"10\" /><item key=\"2\" value=\"20\" /></IntInt></DictionariesDto>";

            var deserializer = new XmlDeserializer();
            var dto = deserializer.Deserialize<DictionariesDto>(xml);

            Assert.Equal("value1", dto.StringString["key1"]);
            Assert.Equal("value2", dto.StringString["key2"]);

            Assert.Equal(10, dto.StringInt["key1"]);
            Assert.Equal(20, dto.StringInt["key2"]);

            Assert.Equal("value1", dto.IntString[1]);
            Assert.Equal("value2", dto.IntString[2]);

            Assert.Equal(10, dto.IntInt[1]);
            Assert.Equal(20, dto.IntInt[2]);
        }

        [Fact]
        public void MustWorkForDictionariesEnumDto()
        {
            var xml = "<DictionariesEnumDto><SomeEnumString><item key=\"EnumValue1\"><value><![CDATA[SOMEVALUE1]]></value></item><item key=\"EnumValue2\"><value><![CDATA[SOMEVALUE2]]></value></item></SomeEnumString><StringSomeEnum><item key=\"SOMEVALUE1\" value=\"EnumValue1\" /><item key=\"SOMEVALUE2\" value=\"EnumValue2\" /></StringSomeEnum><SomeEnumSomeEnum><item key=\"EnumValue1\" value=\"EnumValue1\" /><item key=\"EnumValue2\" value=\"EnumValue2\" /></SomeEnumSomeEnum></DictionariesEnumDto>";

            var deserializer = new XmlDeserializer();
            var dto = deserializer.Deserialize<DictionariesEnumDto>(xml);

            Assert.Equal("SOMEVALUE1", dto.SomeEnumString[SomeEnum.EnumValue1]);
            Assert.Equal("SOMEVALUE2", dto.SomeEnumString[SomeEnum.EnumValue2]);

            Assert.Equal(SomeEnum.EnumValue1, dto.StringSomeEnum["SOMEVALUE1"]);
            Assert.Equal(SomeEnum.EnumValue2, dto.StringSomeEnum["SOMEVALUE2"]);

            Assert.Equal(SomeEnum.EnumValue1, dto.SomeEnumSomeEnum[SomeEnum.EnumValue1]);
            Assert.Equal(SomeEnum.EnumValue2, dto.SomeEnumSomeEnum[SomeEnum.EnumValue2]);
        }

        [Fact]
        public void MustWorkForDictionaryStringByteDto()
        {
            var xml = "<DictionaryStringByteDto><StringByteBuffer><item key=\"KEY1\"><value uri=\"baggage://11111111-1111-1111-1111-111111111111\" /></item><item key=\"KEY2\"><value uri=\"baggage://22222222-2222-2222-2222-222222222222\" /></item></StringByteBuffer></DictionaryStringByteDto>";

            var deserializer = new XmlDeserializer(x =>
            {
                if (x == "baggage://11111111-1111-1111-1111-111111111111")
                {
                    return new byte[3] { 1, 2, 3 };
                }
                else if (x == "baggage://22222222-2222-2222-2222-222222222222")
                {
                    return new byte[3] { 4, 5, 6 };
                }

                throw new Exception();
            });
            var dto = deserializer.Deserialize<DictionaryStringByteDto>(xml);

            Assert.Equal(1, dto.StringByteBuffer["KEY1"][0]);
            Assert.Equal(2, dto.StringByteBuffer["KEY1"][1]);
            Assert.Equal(3, dto.StringByteBuffer["KEY1"][2]);

            Assert.Equal(4, dto.StringByteBuffer["KEY2"][0]);
            Assert.Equal(5, dto.StringByteBuffer["KEY2"][1]);
            Assert.Equal(6, dto.StringByteBuffer["KEY2"][2]);
        }

        [Fact]
        public void MustWorkForTreeOfObjects()
        {
            var xml = "<RootDto><Leaf Id=\"12\" /></RootDto>";

            var deserializer = new XmlDeserializer();
            var dto = deserializer.Deserialize<RootDto>(xml);

            Assert.Equal(12, dto.Leaf.Id);
        }

        [Fact]
        public void MustWorkForClassesWithBase()
        {
            var xml = "<ClassWithBaseDto><Leaf Id=\"12\" /></ClassWithBaseDto>";

            var deserializer = new XmlDeserializer();
            var dto = deserializer.Deserialize<RootDto>(xml);

            Assert.Equal(12, dto.Leaf.Id);
        }

        [Fact]
        public void MustWorkForClassesWithClassArray()
        {
            var xml = "<ClassWithArrayOfClass><Leafs><LeafDto Id=\"2\" /><LeafDto Id=\"3\" /></Leafs></ClassWithArrayOfClass>";

            var deserializer = new XmlDeserializer();
            var dto = deserializer.Deserialize<ClassWithArrayOfClass>(xml);

            Assert.Equal(2, dto.Leafs[0].Id);
            Assert.Equal(3, dto.Leafs[1].Id);
        }

        [Fact]
        public void MustWorkForDictionaryWithClass()
        {
            var xml = "<DictionaryWithClass><StringLeafDto><item key=\"LEAF1\"><value><LeafDto Id=\"5\" /></value></item><item key=\"LEAF2\"><value><LeafDto Id=\"6\" /></value></item></StringLeafDto><LeafDtoString><item><key><LeafDto Id=\"5\" /></key><value><![CDATA[VALUE1]]></value></item><item><key><LeafDto Id=\"6\" /></key><value><![CDATA[VALUE2]]></value></item></LeafDtoString></DictionaryWithClass>";

            var deserializer = new XmlDeserializer();
            var dto = deserializer.Deserialize<DictionaryWithClass>(xml);

            Assert.Equal(5, dto.StringLeafDto["LEAF1"].Id);
            Assert.Equal(6, dto.StringLeafDto["LEAF2"].Id);

            var first = dto.LeafDtoString.First().Key;
            var second = dto.LeafDtoString.Skip(1).First().Key;

            Assert.Equal("VALUE1", dto.LeafDtoString[first]);
            Assert.Equal("VALUE2", dto.LeafDtoString[second]);
        }
    }
}
