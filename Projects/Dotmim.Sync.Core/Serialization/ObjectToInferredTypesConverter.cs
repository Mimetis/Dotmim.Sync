using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json.Serialization;
using System.Text.Json;

namespace Dotmim.Sync.Serialization
{
    public class ObjectToInferredTypesConverter : JsonConverter<object>
    {
        public override object Read(
            ref Utf8JsonReader reader,
            Type typeToConvert,
            JsonSerializerOptions options) => ReadValue(ref reader);


        public override void Write(
            Utf8JsonWriter writer,
            object objectToWrite,
            JsonSerializerOptions options) => WriteValue(writer, objectToWrite, options);


        /// <summary>
        /// Read the next value and infer the type between string, bool, long, double or datetimeoffset
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        public static object ReadValue(ref Utf8JsonReader reader)
        {

            object value = null;

            if (reader.TokenType == JsonTokenType.Null || reader.TokenType == JsonTokenType.None)
                value = null;
            else if (reader.TokenType == JsonTokenType.String && reader.TryGetDateTimeOffset(out DateTimeOffset datetimeOffset))
                value = datetimeOffset;
            else if (reader.TokenType == JsonTokenType.String)
                value = reader.GetString();
            else if (reader.TokenType == JsonTokenType.False || reader.TokenType == JsonTokenType.True)
                value = reader.GetBoolean();
            else if (reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out long l))
                value = l;
            else if (reader.TokenType == JsonTokenType.Number)
                value = reader.GetDouble();


            return value;
        }

        /// <summary>
        /// Write value using options 
        /// </summary>
        public static void WriteValue(Utf8JsonWriter writer, object objectToWrite, JsonSerializerOptions options)
        {
            if (objectToWrite == null)
                writer.WriteNullValue();
            else
                JsonSerializer.Serialize(writer, objectToWrite, objectToWrite.GetType(), options);
        }

    }
}
