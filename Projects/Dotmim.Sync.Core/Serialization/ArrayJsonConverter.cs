using System;
using System.Collections.Generic;
using System.Formats.Asn1;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Dotmim.Sync.Serialization
{
    public class ArrayJsonConverter : JsonConverter<List<object[]>>
    {
        public override List<object[]> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartArray)
                throw new JsonException("Reading an array of objects array is not starting with a StartArray node.");

            var buffer = new List<object[]>();

            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
            {
                if (reader.TokenType != JsonTokenType.StartArray)
                    throw new JsonException("The node inside the array should be an array of objects");

                var array = new List<object>();
                
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    array.Add(ObjectToInferredTypesConverter.ReadValue(ref reader));

                buffer.Add(array.ToArray());
            }

            return buffer;
        }
        public override void Write(Utf8JsonWriter writer, List<object[]> values, JsonSerializerOptions options)
        {
            writer.WriteStartArray();
            foreach (var value in values)
            {
                writer.WriteStartArray();
                foreach (var v in value)
                    ObjectToInferredTypesConverter.WriteValue(writer, v, options);

                writer.WriteEndArray();

            }
            writer.WriteEndArray();
        }
    }

}
