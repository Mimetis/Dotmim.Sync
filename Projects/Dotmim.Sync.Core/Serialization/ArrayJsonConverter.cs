using System;
using System.Collections.Generic;
using System.Formats.Asn1;
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
                throw new JsonException("must be an array");

            var buffer = new List<object[]>();

            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
            {
                if (reader.TokenType != JsonTokenType.StartArray)
                    throw new JsonException("must be an array");

                var array = new List<object>();
                
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    object value = null;

                    if (reader.TokenType == JsonTokenType.Null || reader.TokenType == JsonTokenType.None)
                        value = null;
                    else if (reader.TokenType == JsonTokenType.String && reader.TryGetDateTimeOffset(out DateTimeOffset datetimeOffset))
                        value = datetimeOffset;
                    else if (reader.TokenType == JsonTokenType.String && reader.TryGetDateTime(out DateTime datetime))
                        value = datetime;
                    else if (reader.TokenType == JsonTokenType.String)
                        value = reader.GetString();
                    else if (reader.TokenType == JsonTokenType.False || reader.TokenType == JsonTokenType.True)
                        value = reader.GetBoolean();
                    else if (reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out long l))
                        value = l;
                    else if (reader.TokenType == JsonTokenType.Number)
                        value = reader.GetDouble();

                    array.Add(value);
                }

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
                {
                    if (v == null)
                        writer.WriteNullValue();
                    else if (v is string s)
                        writer.WriteStringValue(s);
                    else if (v is byte by)
                        writer.WriteNumberValue(by);
                    else if (v is bool b)
                        writer.WriteBooleanValue(b);
                    else if (v is double d)
                        writer.WriteNumberValue(d);
                    else if (v is int i)
                        writer.WriteNumberValue(i);
                    else if (v is long l)
                        writer.WriteNumberValue(l);
                    else if (v is float f)
                        writer.WriteNumberValue(f);
                    else if (v is decimal dec)
                        writer.WriteNumberValue(dec);
                    else if (v is DateTime dt)
                        writer.WriteStringValue(dt.ToString("o"));
                    else if (v is DateTimeOffset dto)
                        writer.WriteStringValue(dto.ToString("o"));
                    else if (v is Guid g)
                        writer.WriteStringValue(g.ToString());
                    else if (v is byte[] bytes)
                        writer.WriteBase64StringValue(bytes.AsSpan());
                    else if (v is short sh)
                        writer.WriteNumberValue(sh);
                    else if (v is uint ui)
                        writer.WriteNumberValue(ui);
                    else if (v is ulong ul)
                        writer.WriteNumberValue(ul);
                    else if (v is ushort us)
                        writer.WriteNumberValue(us);
                    else if (v is sbyte sb)
                        writer.WriteNumberValue(sb);
                    else if (v is char c)
                        writer.WriteStringValue(c.ToString());
                    else if (v is TimeSpan ts)
                        writer.WriteStringValue(ts.ToString());
                    else
                        throw new NotSupportedException($"Type {v.GetType()} is not supported");
                }

                writer.WriteEndArray();

            }
            writer.WriteEndArray();

        }
    }

}
