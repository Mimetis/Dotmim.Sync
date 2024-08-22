using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Dotmim.Sync.Serialization
{
    /// <summary>
    /// Convert an object to an inferred type (string, bool, long, double or datetimeoffset).
    /// </summary>
    public class ObjectToInferredTypesConverter : JsonConverter<object>
    {
        /// <summary>
        /// Read the next value and infer the type between string, bool, long, double or datetimeoffset.
        /// </summary>
        public static object ReadValue(ref Utf8JsonReader reader)
        {

            object value = null;

            if (reader.TokenType == JsonTokenType.Null || reader.TokenType == JsonTokenType.None)
                value = null;
            else if (reader.TokenType == JsonTokenType.String && reader.TryGetDateTimeOffset(out var datetimeOffset))
                value = datetimeOffset;
            else if (reader.TokenType == JsonTokenType.String)
                value = reader.GetString();
            else if (reader.TokenType == JsonTokenType.False || reader.TokenType == JsonTokenType.True)
                value = reader.GetBoolean();
            else if (reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out var l))
                value = l;
            else if (reader.TokenType == JsonTokenType.Number)
                value = reader.GetDouble();

            return value;
        }

        /// <summary>
        /// Write value using options.
        /// </summary>
        public static void WriteValue(Utf8JsonWriter writer, object objectToWrite, JsonSerializerOptions options)
        {
            Guard.ThrowIfNull(writer);

            if (objectToWrite == null)
                writer.WriteNullValue();
            else
                JsonSerializer.Serialize(writer, objectToWrite, objectToWrite.GetType(), options);
        }

        /// <summary>
        /// Read the next value and infer the type between string, bool, long, double or datetimeoffset.
        /// </summary>
        public override object Read(
            ref Utf8JsonReader reader,
            Type typeToConvert,
            JsonSerializerOptions options) => ReadValue(ref reader);

        /// <summary>
        /// Write value using options.
        /// </summary>
        public override void Write(
            Utf8JsonWriter writer,
            object value,
            JsonSerializerOptions options) => WriteValue(writer, value, options);
    }
}