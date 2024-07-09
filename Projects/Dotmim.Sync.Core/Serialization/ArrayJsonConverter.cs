using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Dotmim.Sync.Serialization
{
    /// <summary>
    /// ArrayJsonConverter used to convert an array of objects to a json array.
    /// </summary>
    public class ArrayJsonConverter : JsonConverter<List<object[]>>
    {

        /// <summary>
        /// Reads an array of objects from the reader.
        /// </summary>
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

                buffer.Add([.. array]);
            }

            return buffer;
        }

        /// <summary>
        /// Writes an array of objects to the writer.
        /// </summary>
        public override void Write(Utf8JsonWriter writer, List<object[]> value, JsonSerializerOptions options)
        {
            Guard.ThrowIfNull(writer);

            if (value == null || value.Count == 0)
                return;

            writer.WriteStartArray();
            foreach (var valueArray in value)
            {
                writer.WriteStartArray();
                foreach (var v in valueArray)
                    ObjectToInferredTypesConverter.WriteValue(writer, v, options);

                writer.WriteEndArray();
            }

            writer.WriteEndArray();
        }
    }
}