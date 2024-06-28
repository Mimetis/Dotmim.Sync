using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Dotmim.Sync.Serialization
{

    public class JsonReader : IDisposable
    {

        // encoding used to convert bytes to string
        private static readonly UTF8Encoding utf8Encoding = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

        /// <summary>
        /// Stream to read
        /// </summary>
        public Stream Stream { get; }

        // buffer size
        private readonly JsonReaderOptions jsonReaderOptions;
        private const int MaxTokenGap = 1024 * 1024;

        /// <summary>
        /// Create a fast forward json reader
        /// </summary>
        /// <param name="stream">Stream to read</param>
        /// <param name="jsonReaderOptions">options</param>
        /// <param name="bufferSize">buffer size. will adapt if needed</param>
        /// <exception cref="Exception">If stream is not readable</exception>
        public JsonReader(Stream stream, JsonReaderOptions jsonReaderOptions = default, int bufferSize = 1024)
        {
            this.Stream = stream;
            this.jsonReaderOptions = jsonReaderOptions;

            if (!this.Stream.CanRead)
                throw new Exception("Stream is not readable");

            // create a buffer to read the stream into
            this.buffer = new byte[bufferSize];
            this.dataLen = 0;
            this.dataPos = 0;
            this.isFinalBlock = false;
            this.currentState = new JsonReaderState(jsonReaderOptions);
            this.tokensFound = 0;
            this.bytesConsumed = 0;
        }

        // buffer used by Utf8JsonReader to read values
        private byte[] buffer;

        private int dataLen;
        private int dataPos;

        // number of tokens found in the buffer.
        // if this is 0, it means we need to read more data from the stream
        private int tokensFound;

        // if this is true, it means we have reached the end of the stream
        private bool isFinalBlock;

        // state object used internally by Utf8JsonReader
        private JsonReaderState currentState;

        // bytes consumed by Utf8JsonReader each time it reads a token
        private int bytesConsumed;

        // more tokens to be found
        private bool hasMore;

        private bool disposedValue;


        ///// <summary>
        ///// Current value
        ///// </summary>
        //public JsonReaderValue Current { get; private set; }

        /// <summary>
        /// Gets the token value. Can be a value or a property name
        /// </summary>
        public ReadOnlyMemory<byte> Value { get; private set; }

        /// <summary>
        /// Gets the token type
        /// </summary>
        public JsonTokenType TokenType { get; private set; } = JsonTokenType.None;

        /// <summary>
        /// Gets the current depth
        /// </summary>
        public int Depth { get; private set; } = 0;



        /// <summary>
        /// Read the next value. Can be any TokenType
        /// </summary>
        /// <returns>true if a token has been read otherwise false</returns>
        /// <exception cref="JsonException"></exception>
        public bool Read()
        {
            if (this.buffer == null)
                throw new ArgumentNullException(nameof(this.buffer));

            // if we don't have any more bytes and in final block, we can exit
            if (this.dataLen <= 0 && this.isFinalBlock)
                return false;

            bool foundToken = false;

            while (!foundToken)
            {
                // at this point, if there's already any data in the buffer, it has been shifted to start at index 0
                if (this.dataLen < this.buffer.Length && !this.isFinalBlock && !this.hasMore)
                {
                    // there's space left in the buffer, try to fill it with new data
                    int todo = this.buffer.Length - this.dataLen;
                    int done = this.Stream.Read(this.buffer, this.dataLen, todo);
                    this.dataLen += done;
                    this.isFinalBlock = done < todo;
                    this.bytesConsumed = 0;
                    this.tokensFound = 0;
                }

                this.dataPos += this.bytesConsumed;
                this.dataLen -= this.bytesConsumed;

                // create a new ref struct json reader
                var spanBuffer = new ReadOnlySpan<byte>(this.buffer, this.dataPos, this.dataLen);
                // Trace.WriteLine($"span starting from {dataPos} : {BitConverter.ToString(spanBuffer.ToArray())}");

                var reader = new Utf8JsonReader(spanBuffer, this.isFinalBlock, state: this.currentState);

                // try to read nex token
                foundToken = reader.Read();

                // we have a valid token
                if (foundToken)
                {
                    this.currentState = reader.CurrentState;
                    this.bytesConsumed = (int)reader.BytesConsumed;
                    this.tokensFound++;
                    this.hasMore = true;
                    this.TokenType = reader.TokenType;
                    this.Depth = reader.CurrentDepth;
                    this.Value = new ReadOnlyMemory<byte>(reader.ValueSpan.ToArray());
                    return true;
                }

                // if we don't have any more bytes and in final block, we can exit
                if (this.dataLen <= 0 && this.isFinalBlock)
                    break;

                if (!this.isFinalBlock)
                {
                    // regardless if we found tokens or not, there may be data for a partial token remaining at the end.
                    if (this.dataPos > 0)
                    {
                        // Shift partial token data to the start of the buffer
                        Array.Copy(this.buffer, this.dataPos, this.buffer, 0, this.dataLen);
                        this.dataPos = 0;
                    }

                    if (this.tokensFound == 0)
                    {
                        // we didn't find any tokens in the current buffer, so it needs to expand.
                        if (this.buffer.Length > MaxTokenGap)
                            throw new JsonException($"sanity check on input stream failed, json token gap of more than {MaxTokenGap} bytes");

                        Array.Resize(ref this.buffer, this.buffer.Length * 2);
                    }

                    this.hasMore = false;
                }
                else
                {
                    foundToken = false;
                }
            }
            return false;
        }

        /// <summary>
        /// Enumerate over the stream and read the properties
        /// </summary>
        /// <returns></returns>
        public IEnumerable<JsonReaderValue> Values()
        {
            while (this.Read())
            {
                JsonReaderValue jsonReaderValue = new() { TokenType = this.TokenType, Depth = this.Depth };
                if (this.TokenType == JsonTokenType.PropertyName)
                    jsonReaderValue.Value = JsonValue.Create(this.GetString());
                else if (this.TokenType == JsonTokenType.Null || this.TokenType == JsonTokenType.None)
                    jsonReaderValue.Value = null;
                else if (this.TokenType == JsonTokenType.String)
                    jsonReaderValue.Value = JsonValue.Create(this.GetString());
                else if (this.TokenType == JsonTokenType.False || this.TokenType == JsonTokenType.True)
                    jsonReaderValue.Value = JsonValue.Create(this.GetBoolean());
                else if (this.TokenType == JsonTokenType.Number)
                    jsonReaderValue.Value = JsonValue.Create(this.GetDouble());

                yield return jsonReaderValue;
            }
        }

        /// <summary>
        /// Skips the children of the current token.
        /// </summary>
        public bool Skip()
        {
            if (this.TokenType == JsonTokenType.PropertyName)
                return this.Read();

            if (this.TokenType == JsonTokenType.StartObject || this.TokenType == JsonTokenType.StartArray)
            {
                int depth = this.Depth;
                do
                {
                    bool hasRead = this.Read();

                    if (!hasRead)
                        return false;
                }
                while (depth < this.Depth);

                return true;
            }
            return false;
        }


        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposedValue)
            {
                if (disposing)
                {
                    if (this.buffer != null)
                    {
#if NET6_0_OR_GREATER
                        Array.Clear(this.buffer);
#else
                        Array.Clear(this.buffer, 0, this.buffer.Length);
#endif
                        this.buffer = null;
                    }
                }

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                this.disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            this.Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public string ReadAsString()
        {
            this.Read();
            return this.GetString();
        }
        public string GetString()
        {
            if (this.TokenType != JsonTokenType.PropertyName && this.TokenType != JsonTokenType.String)
                return null;

#if NET6_0_OR_GREATER
            var str = utf8Encoding.GetString(this.Value.Span);
#else   
            var str = utf8Encoding.GetString(this.Value.ToArray());
#endif

            return Regex.Unescape(str);
        }
        public bool TryGetString(out string value)
        {
            if (this.TokenType != JsonTokenType.PropertyName && this.TokenType != JsonTokenType.String)
            {
                value = string.Empty;
                return false;
            }
#if NET6_0_OR_GREATER
            var str = utf8Encoding.GetString(this.Value.Span);
#else   
            var str = utf8Encoding.GetString(this.Value.ToArray());
#endif

            value = Regex.Unescape(str); ;
            return true;
        }

        public string ReadAsEscapedString()
        {
            this.Read();
            return this.GetEscapedString();
        }
        public string GetEscapedString()
        {
            if (this.TokenType != JsonTokenType.PropertyName && this.TokenType != JsonTokenType.String)
                return null;

#if NET6_0_OR_GREATER
            var str = utf8Encoding.GetString(this.Value.Span);
#else   
            var str = utf8Encoding.GetString(this.Value.ToArray());
#endif

            return str;
        }
        public bool TryGetEscapedString(out string value)
        {
            if (this.TokenType != JsonTokenType.PropertyName && this.TokenType != JsonTokenType.String)
            {
                value = string.Empty;
                return false;
            }

#if NET6_0_OR_GREATER
            var str = utf8Encoding.GetString(this.Value.Span);
#else   
            var str = utf8Encoding.GetString(this.Value.ToArray());
#endif

            value = str;
            return true;
        }

        public Guid? ReadAsGuid()
        {
            this.Read();
            return this.GetGuid();
        }
        public Guid? GetGuid()
        {
            if (this.TokenType != JsonTokenType.String)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out Guid tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;

            throw new FormatException("Can't parse double");
        }
        public bool TryGetGuid(out Guid value)
        {
            if (this.TokenType != JsonTokenType.String)
            {
                value = Guid.Empty;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out Guid tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = Guid.Empty;
            return false;
        }

        public TimeSpan? ReadAsTimeSpan()
        {
            this.Read();
            return this.GetTimeSpan();
        }
        public TimeSpan? GetTimeSpan()
        {
            if (this.TokenType != JsonTokenType.String)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out TimeSpan tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;

            throw new FormatException("Can't parse TimeSpan");
        }
        public bool TryGetTimeSpan(out TimeSpan value)
        {
            if (this.TokenType != JsonTokenType.String)
            {
                value = TimeSpan.Zero;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out TimeSpan tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = TimeSpan.Zero;
            return false;
        }

        public DateTimeOffset? ReadAsDateTimeOffset()
        {
            this.Read();
            return this.GetDateTimeOffset();
        }
        public DateTimeOffset? GetDateTimeOffset()
        {
            if (this.TokenType != JsonTokenType.String)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out DateTimeOffset tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;

            throw new FormatException("Can't parse DateTimeOffset");
        }
        public bool TryGetDateTimeOffset(out DateTimeOffset value)
        {
            if (this.TokenType != JsonTokenType.String)
            {
                value = DateTimeOffset.MinValue;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out DateTimeOffset tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = DateTimeOffset.MinValue;
            return false;
        }

        public DateTime? ReadAsDateTime()
        {
            this.Read();
            return this.GetDateTime();
        }
        public DateTime? GetDateTime()
        {
            if (this.TokenType != JsonTokenType.String)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out DateTime tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;

            throw new FormatException("Can't parse GetDateTime");
        }
        public bool TryGetDateTime(out DateTime value)
        {
            if (this.TokenType != JsonTokenType.String)
            {
                value = DateTime.MinValue;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out DateTime tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = DateTime.MinValue;
            return false;
        }

        public double? ReadAsDouble()
        {
            this.Read();
            return this.GetDouble();
        }
        public double? GetDouble()
        {
            if (this.TokenType != JsonTokenType.Number)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out double tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;

            throw new FormatException("Can't parse double");
        }
        public bool TryGetDouble(out double value)
        {
            if (this.TokenType != JsonTokenType.Number)
            {
                value = 0;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out double tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = 0;
            return false;
        }

        public decimal? ReadAsDecimal()
        {
            this.Read();
            return this.GetDecimal();
        }
        public decimal? GetDecimal()
        {
            if (this.TokenType != JsonTokenType.Number)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out decimal tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;

            throw new FormatException("Can't parse decimal");
        }
        public bool TryGetDecimal(out decimal value)
        {
            if (this.TokenType != JsonTokenType.Number)
            {
                value = 0;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out decimal tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = 0;
            return false;
        }


        public float? ReadAsSingle()
        {
            this.Read();
            return this.GetSingle();
        }
        public float? GetSingle()
        {
            if (this.TokenType != JsonTokenType.Number)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out float tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;

            throw new FormatException("Can't parse float");
        }
        public bool TryGetSingle(out float value)
        {
            if (this.TokenType != JsonTokenType.Number)
            {
                value = 0;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out float tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = 0;
            return false;
        }


        public long? ReadAsInt64()
        {
            this.Read();
            return this.GetInt64();
        }
        public long? GetInt64()
        {
            if (this.TokenType != JsonTokenType.Number)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out long tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;

            throw new FormatException("Can't parse long");
        }
        public bool TryGetInt64(out long value)
        {
            if (this.TokenType != JsonTokenType.Number)
            {
                value = 0;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out long tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = 0;
            return false;
        }


        public int? ReadAsInt32()
        {
            this.Read();
            return this.GetInt32();
        }
        public int? GetInt32()
        {
            if (this.TokenType != JsonTokenType.Number)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out int tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;

            throw new FormatException("Can't parse int");
        }
        public bool TryGetInt32(out int value)
        {
            if (this.TokenType != JsonTokenType.Number)
            {
                value = 0;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out int tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = 0;
            return false;
        }


        public short? ReadAsInt16()
        {
            this.Read();
            return this.GetInt16();
        }
        public short? GetInt16()
        {
            if (this.TokenType != JsonTokenType.Number)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out short tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;

            throw new FormatException("Can't parse short");
        }
        public bool TryGetInt16(out short value)
        {
            if (this.TokenType != JsonTokenType.Number)
            {
                value = 0;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out short tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = 0;
            return false;
        }


        public byte? ReadAsByte()
        {
            this.Read();
            return this.GetByte();
        }
        public byte? GetByte()
        {
            if (this.TokenType != JsonTokenType.Number)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out byte tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;

            throw new FormatException("Can't parse byte");
        }
        public bool TryGetByte(out byte value)
        {
            if (this.TokenType != JsonTokenType.Number)
            {
                value = 0;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out byte tmp, out int bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = 0;
            return false;
        }


        public bool? ReadAsBoolean()
        {
            this.Read();
            return this.GetBoolean();
        }
        public bool? GetBoolean()
        {
            if (this.TokenType == JsonTokenType.True)
                return true;
            else if (this.TokenType == JsonTokenType.False)
                return false;
            else
                return null;
        }
        public bool TryGetBoolean(out bool value)
        {
            if (this.TokenType == JsonTokenType.True)
            {
                value = true;
                return true;
            }
            else if (this.TokenType == JsonTokenType.False)
            {
                value = false;
                return true;
            }

            value = false;
            return false;
        }


        //public byte[] GetBytesFromBase64()
        //{
        //    return null;
        //}

    }

    public struct JsonReaderValue
    {
        public JsonValue Value { get; set; }
        public JsonTokenType TokenType { get; set; } = JsonTokenType.None;
        public int Depth { get; set; } = 0;
        public JsonReaderValue() { }
        public override readonly string ToString()
        {
            var sb = new StringBuilder($"Type: {this.TokenType} - Depth: {this.Depth}");

            if (this.TokenType == JsonTokenType.PropertyName)
                sb.Append(" - Property: ").Append(this.Value);

            if (this.Value != null)
                sb.Append(" - Value: ").Append(this.Value);

            return sb.ToString();
        }
    }
}