using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Dotmim.Sync.Serialization
{

    /// <summary>
    /// Json reader to read a stream and get the properties.
    /// </summary>
    public class JsonReader : IDisposable
    {
        // buffer size
        private const int MaxTokenGap = 1024 * 1024;

        // encoding used to convert bytes to string
        private static readonly UTF8Encoding Utf8Encoding = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

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

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonReader"/> class.
        /// Create a fast forward json reader.
        /// </summary>
        /// <param name="stream">Stream to read.</param>
        /// <param name="jsonReaderOptions">options.</param>
        /// <param name="bufferSize">buffer size. will adapt if needed.</param>
        /// <exception cref="Exception">If stream is not readable.</exception>
        public JsonReader(Stream stream, JsonReaderOptions jsonReaderOptions = default, int bufferSize = 1024)
        {
            this.Stream = stream;

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

        /// <summary>
        /// Gets stream to read.
        /// </summary>
        public Stream Stream { get; }

        ///// <summary>
        ///// Current value
        ///// </summary>
        // public JsonReaderValue Current { get; private set; }

        /// <summary>
        /// Gets the token value. Can be a value or a property name.
        /// </summary>
        public ReadOnlyMemory<byte> Value { get; private set; }

        /// <summary>
        /// Gets the token type.
        /// </summary>
        public JsonTokenType TokenType { get; private set; } = JsonTokenType.None;

        /// <summary>
        /// Gets the current depth.
        /// </summary>
        public int Depth { get; private set; }

        /// <summary>
        /// Read the next value. Can be any TokenType.
        /// </summary>
        /// <returns>true if a token has been read otherwise false.</returns>
        public bool Read()
        {
            if (this.buffer == null)
                throw new ArgumentNullException(nameof(this.buffer));

            // if we don't have any more bytes and in final block, we can exit
            if (this.dataLen <= 0 && this.isFinalBlock)
                return false;

            var foundToken = false;

            while (!foundToken)
            {
                // at this point, if there's already any data in the buffer, it has been shifted to start at index 0
                if (this.dataLen < this.buffer.Length && !this.isFinalBlock && !this.hasMore)
                {
                    // there's space left in the buffer, try to fill it with new data
                    var todo = this.buffer.Length - this.dataLen;
                    var done = this.Stream.Read(this.buffer, this.dataLen, todo);
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
        /// Enumerate over the stream and read the properties.
        /// </summary>
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
                var depth = this.Depth;
                do
                {
                    var hasRead = this.Read();

                    if (!hasRead)
                        return false;
                }
                while (depth < this.Depth);

                return true;
            }

            return false;
        }

        /// <summary>
        /// Read the next token and get the value as a string.
        /// </summary>
        public string ReadAsString()
        {
            this.Read();
            return this.GetString();
        }

        /// <summary>
        /// Gets the current token value as a string, if the token is a property name or a string.
        /// </summary>
        public string GetString()
        {
            if (this.TokenType != JsonTokenType.PropertyName && this.TokenType != JsonTokenType.String)
                return null;

#if NET6_0_OR_GREATER
            var str = Utf8Encoding.GetString(this.Value.Span);
#else
            var str = Utf8Encoding.GetString(this.Value.ToArray());
#endif

            return Regex.Unescape(str);
        }

        /// <summary>
        /// Try to get the current token value as a string, if the token is a property name or a string.
        /// </summary>
        public bool TryGetString(out string value)
        {
            if (this.TokenType != JsonTokenType.PropertyName && this.TokenType != JsonTokenType.String)
            {
                value = string.Empty;
                return false;
            }
#if NET6_0_OR_GREATER
            var str = Utf8Encoding.GetString(this.Value.Span);
#else
            var str = Utf8Encoding.GetString(this.Value.ToArray());
#endif

            value = Regex.Unescape(str);
            return true;
        }

        /// <summary>
        /// Read the next token and get the value as a string.
        /// </summary>
        public string ReadAsEscapedString()
        {
            this.Read();
            return this.GetEscapedString();
        }

        /// <summary>
        /// Gets the current token value as a string, if the token is a property name or a string.
        /// </summary>
        public string GetEscapedString()
        {
            if (this.TokenType != JsonTokenType.PropertyName && this.TokenType != JsonTokenType.String)
                return null;

#if NET6_0_OR_GREATER
            var str = Utf8Encoding.GetString(this.Value.Span);
#else
            var str = Utf8Encoding.GetString(this.Value.ToArray());
#endif

            return str;
        }

        /// <summary>
        /// Try to get the current token value as a string, if the token is a property name or a string.
        /// </summary>
        public bool TryGetEscapedString(out string value)
        {
            if (this.TokenType != JsonTokenType.PropertyName && this.TokenType != JsonTokenType.String)
            {
                value = string.Empty;
                return false;
            }

#if NET6_0_OR_GREATER
            var str = Utf8Encoding.GetString(this.Value.Span);
#else
            var str = Utf8Encoding.GetString(this.Value.ToArray());
#endif

            value = str;
            return true;
        }

        /// <summary>
        /// Read the next token and get the value as a Guid.
        /// </summary>
        public Guid? ReadAsGuid()
        {
            this.Read();
            return this.GetGuid();
        }

        /// <summary>
        /// Gets the current token value as a Guid, if the token is a string.
        /// </summary>
        public Guid? GetGuid()
        {
            if (this.TokenType != JsonTokenType.String)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out Guid tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;
            else

                throw new FormatException("Can't parse double");
        }

        /// <summary>
        /// Try to get the current token value as a Guid, if the token is a string.
        /// </summary>
        public bool TryGetGuid(out Guid value)
        {
            if (this.TokenType != JsonTokenType.String)
            {
                value = Guid.Empty;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out Guid tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = Guid.Empty;
            return false;
        }

        /// <summary>
        /// Read the next token and get the value as a TimeSpan.
        /// </summary>
        public TimeSpan? ReadAsTimeSpan()
        {
            this.Read();
            return this.GetTimeSpan();
        }

        /// <summary>
        /// Gets the current token value as a TimeSpan, if the token is a string.
        /// </summary>
        public TimeSpan? GetTimeSpan()
        {
            if (this.TokenType != JsonTokenType.String)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out TimeSpan tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;
            else

                throw new FormatException("Can't parse TimeSpan");
        }

        /// <summary>
        /// Try to get the current token value as a TimeSpan, if the token is a string.
        /// </summary>
        public bool TryGetTimeSpan(out TimeSpan value)
        {
            if (this.TokenType != JsonTokenType.String)
            {
                value = TimeSpan.Zero;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out TimeSpan tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = TimeSpan.Zero;
            return false;
        }

        /// <summary>
        /// Read the next token and get the value as a DateTimeOffset.
        /// </summary>
        public DateTimeOffset? ReadAsDateTimeOffset()
        {
            this.Read();
            return this.GetDateTimeOffset();
        }

        /// <summary>
        /// Gets the current token value as a DateTimeOffset, if the token is a string.
        /// </summary>
        public DateTimeOffset? GetDateTimeOffset()
        {
            if (this.TokenType != JsonTokenType.String)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out DateTimeOffset tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;
            else

                throw new FormatException("Can't parse DateTimeOffset");
        }

        /// <summary>
        /// Try to get the current token value as a DateTimeOffset, if the token is a string.
        /// </summary>
        public bool TryGetDateTimeOffset(out DateTimeOffset value)
        {
            if (this.TokenType != JsonTokenType.String)
            {
                value = DateTimeOffset.MinValue;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out DateTimeOffset tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = DateTimeOffset.MinValue;
            return false;
        }

        /// <summary>
        /// Read the next token and get the value as a DateTime.
        /// </summary>
        public DateTime? ReadAsDateTime()
        {
            this.Read();
            return this.GetDateTime();
        }

        /// <summary>
        /// Gets the current token value as a DateTime, if the token is a string.
        /// </summary>
        public DateTime? GetDateTime()
        {
            if (this.TokenType != JsonTokenType.String)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out DateTime tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;
            else

                throw new FormatException("Can't parse GetDateTime");
        }

        /// <summary>
        /// Try to get the current token value as a DateTime, if the token is a string.
        /// </summary>
        public bool TryGetDateTime(out DateTime value)
        {
            if (this.TokenType != JsonTokenType.String)
            {
                value = DateTime.MinValue;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out DateTime tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = DateTime.MinValue;
            return false;
        }

        /// <summary>
        /// Read the next token and get the value as a double.
        /// </summary>
        public double? ReadAsDouble()
        {
            this.Read();
            return this.GetDouble();
        }

        /// <summary>
        /// Gets the current token value as a double, if the token is a number.
        /// </summary>
        public double? GetDouble()
        {
            if (this.TokenType != JsonTokenType.Number)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out double tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;
            else
                throw new FormatException("Can't parse double");
        }

        /// <summary>
        /// Try to get the current token value as a double, if the token is a number.
        /// </summary>
        public bool TryGetDouble(out double value)
        {
            if (this.TokenType != JsonTokenType.Number)
            {
                value = 0;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out double tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = 0;
            return false;
        }

        /// <summary>
        /// Read the next token and get the value as a decimal.
        /// </summary>
        public decimal? ReadAsDecimal()
        {
            this.Read();
            return this.GetDecimal();
        }

        /// <summary>
        /// Gets the current token value as a decimal, if the token is a number.
        /// </summary>
        public decimal? GetDecimal()
        {
            if (this.TokenType != JsonTokenType.Number)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out decimal tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;
            else
                throw new FormatException("Can't parse decimal");
        }

        /// <summary>
        /// Try to get the current token value as a decimal, if the token is a number.
        /// </summary>
        public bool TryGetDecimal(out decimal value)
        {
            if (this.TokenType != JsonTokenType.Number)
            {
                value = 0;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out decimal tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = 0;
            return false;
        }

        /// <summary>
        /// Read the next token and get the value as a float.
        /// </summary>
        public float? ReadAsSingle()
        {
            this.Read();
            return this.GetSingle();
        }

        /// <summary>
        /// Gets the current token value as a float, if the token is a number.
        /// </summary>
        public float? GetSingle()
        {
            if (this.TokenType != JsonTokenType.Number)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out float tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;
            else

                throw new FormatException("Can't parse float");
        }

        /// <summary>
        /// Try to get the current token value as a float, if the token is a number.
        /// </summary>
        public bool TryGetSingle(out float value)
        {
            if (this.TokenType != JsonTokenType.Number)
            {
                value = 0;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out float tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = 0;
            return false;
        }

        /// <summary>
        /// Read the next token and get the value as a long.
        /// </summary>
        public long? ReadAsInt64()
        {
            this.Read();
            return this.GetInt64();
        }

        /// <summary>
        /// Try to get the current token value as a long, if the token is a number.
        /// </summary>
        public long? GetInt64()
        {
            if (this.TokenType != JsonTokenType.Number)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out long tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;
            else

                throw new FormatException("Can't parse long");
        }

        /// <summary>
        /// Try to get the current token value as a long, if the token is a number.
        /// </summary>
        public bool TryGetInt64(out long value)
        {
            if (this.TokenType != JsonTokenType.Number)
            {
                value = 0;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out long tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = 0;
            return false;
        }

        /// <summary>
        /// Read the next token and get the value as a int.
        /// </summary>
        public int? ReadAsInt32()
        {
            this.Read();
            return this.GetInt32();
        }

        /// <summary>
        /// Gets the current token value as a int, if the token is a number.
        /// </summary>
        public int? GetInt32()
        {
            if (this.TokenType != JsonTokenType.Number)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out int tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;
            else

                throw new FormatException("Can't parse int");
        }

        /// <summary>
        /// Try to get the current token value as a int, if the token is a number.
        /// </summary>
        public bool TryGetInt32(out int value)
        {
            if (this.TokenType != JsonTokenType.Number)
            {
                value = 0;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out int tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = 0;
            return false;
        }

        /// <summary>
        /// Read the next token and get the value as a short.
        /// </summary>
        public short? ReadAsInt16()
        {
            this.Read();
            return this.GetInt16();
        }

        /// <summary>
        /// Gets the current token value as a short, if the token is a number.
        /// </summary>
        public short? GetInt16()
        {
            if (this.TokenType != JsonTokenType.Number)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out short tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;
            else

                throw new FormatException("Can't parse short");
        }

        /// <summary>
        /// Try to get the current token value as a short, if the token is a number.
        /// </summary>
        public bool TryGetInt16(out short value)
        {
            if (this.TokenType != JsonTokenType.Number)
            {
                value = 0;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out short tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = 0;
            return false;
        }

        /// <summary>
        /// Read the next token and get the value as a byte.
        /// </summary>
        public byte? ReadAsByte()
        {
            this.Read();
            return this.GetByte();
        }

        /// <summary>
        /// Gets the current token value as a byte, if the token is a number.
        /// </summary>
        public byte? GetByte()
        {
            if (this.TokenType != JsonTokenType.Number)
                return null;

            if (Utf8Parser.TryParse(this.Value.Span, out byte tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
                return tmp;
            else
                throw new FormatException("Can't parse byte");
        }

        /// <summary>
        /// Try to get the current token value as a byte, if the token is a number.
        /// </summary>
        public bool TryGetByte(out byte value)
        {
            if (this.TokenType != JsonTokenType.Number)
            {
                value = 0;
                return false;
            }

            if (Utf8Parser.TryParse(this.Value.Span, out byte tmp, out var bytesConsumed) && this.Value.Span.Length == bytesConsumed)
            {
                value = tmp;
                return true;
            }

            value = 0;
            return false;
        }

        /// <summary>
        /// Read the next token and get the value as a bool.
        /// </summary>
        public bool? ReadAsBoolean()
        {
            this.Read();
            return this.GetBoolean();
        }

        /// <summary>
        /// Gets the current token value as a bool, if the token is a boolean.
        /// </summary>

#pragma warning disable CA1024 // Use properties where appropriate
        public bool? GetBoolean()
#pragma warning restore CA1024 // Use properties where appropriate
        {
            if (this.TokenType == JsonTokenType.True)
                return true;
            else if (this.TokenType == JsonTokenType.False)
                return false;
            else
                return null;
        }

        /// <summary>
        /// Try to get the current token value as a bool, if the token is a boolean.
        /// </summary>
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

        /// <summary>
        /// Read the next token as a base 64 string and convert the value as a byte array.
        /// </summary>
        public byte[] ReadAsBytesFromBase64()
        {
            this.Read();
            return this.GetBytesFromBase64();
        }

        /// <summary>
        /// Gets the current token value as a byte array, if the token is a base 64 string.
        /// </summary>
        public byte[] GetBytesFromBase64()
        {
            if (this.TokenType != JsonTokenType.PropertyName && this.TokenType != JsonTokenType.String)
                return null;

#if NET6_0_OR_GREATER
            var str = Utf8Encoding.GetString(this.Value.Span);
            const int bitsEncodedPerChar = 6;
            var bytesExpected = (str.Length * bitsEncodedPerChar) >> 3; // divide by 8 bits in a byte

            var buffer = ArrayPool<byte>.Shared.Rent(bytesExpected);

            if (Convert.TryFromBase64String(str, buffer, out var bytesWritten))
            {
                var array = new byte[bytesWritten];
                Array.Copy(buffer, array, bytesWritten);
                return array;
            }

            return null;
#else
            var str = Utf8Encoding.GetString(this.Value.ToArray());
            return Convert.FromBase64String(str);
#endif
        }

        /// <summary>
        /// Try to get the current token value as a byte array, if the token is a base 64 string.
        /// </summary>
        public bool TryGetBytesFromBase64(out byte[] value)
        {
            if (this.TokenType != JsonTokenType.PropertyName && this.TokenType != JsonTokenType.String)
            {
                value = null;
                return false;
            }

#if NET6_0_OR_GREATER
            var str = Utf8Encoding.GetString(this.Value.Span);
            const int bitsEncodedPerChar = 6;
            var bytesExpected = (str.Length * bitsEncodedPerChar) >> 3; // divide by 8 bits in a byte

            var buffer = ArrayPool<byte>.Shared.Rent(bytesExpected);

            if (Convert.TryFromBase64String(str, buffer, out var bytesWritten))
            {
                value = new byte[bytesWritten];
                Array.Copy(buffer, value, bytesWritten);
                return true;
            }

            value = null;
            return false;
#else
            var str = Utf8Encoding.GetString(this.Value.ToArray());
            try
            {
                value = Convert.FromBase64String(str);
                return true;
            }
            catch (Exception)
            {
                value = null;
                return false;
            }
#endif
        }

#if NET6_0_OR_GREATER

        /// <summary>
        /// Read the next token as a base 64 string and convert the value as a byte array.
        /// </summary>
        public Span<byte> ReadAsSpanFromBase64()
        {
            this.Read();
            return this.GetSpanFromBase64();
        }

        /// <summary>
        /// Gets the current token value as a byte array, if the token is a base 64 string.
        /// </summary>
        public Span<byte> GetSpanFromBase64()
        {
            if (this.TokenType != JsonTokenType.PropertyName && this.TokenType != JsonTokenType.String)
                return null;

            var str = Utf8Encoding.GetString(this.Value.Span);
            const int bitsEncodedPerChar = 6;
            var bytesExpected = (str.Length * bitsEncodedPerChar) >> 3; // divide by 8 bits in a byte

            var buffer = ArrayPool<byte>.Shared.Rent(bytesExpected);

            if (Convert.TryFromBase64String(str, buffer, out var bytesWritten))
                return buffer.AsSpan(0, bytesWritten);

            return null;
        }
#endif

        /// <summary>
        /// Dispose the reader.
        /// </summary>
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            this.Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Dispose the reader.
        /// </summary>
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
    }

    /// <summary>
    /// Json reader value.
    /// </summary>
    public struct JsonReaderValue : IEquatable<JsonReaderValue>
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonReaderValue"/> struct.
        /// </summary>
        public JsonReaderValue()
        {
        }

        /// <summary>
        /// Gets or sets the value.
        /// </summary>
        public JsonValue Value { get; set; }

        /// <summary>
        /// Gets or sets the token type.
        /// </summary>
        public JsonTokenType TokenType { get; set; } = JsonTokenType.None;

        /// <summary>
        /// Gets or sets the depth.
        /// </summary>
        public int Depth { get; set; } = 0;

        /// <summary>
        /// Compare two JsonReaderValue.
        /// </summary>
        public static bool operator ==(JsonReaderValue left, JsonReaderValue right) => left.Equals(right);

        /// <summary>
        /// Compare two JsonReaderValue.
        /// </summary>
        public static bool operator !=(JsonReaderValue left, JsonReaderValue right) => !(left == right);

        /// <summary>
        /// Returns the pair key value of the current token.
        /// </summary>
        public override readonly string ToString()
        {
            var sb = new StringBuilder($"Type: {this.TokenType} - Depth: {this.Depth}");

            if (this.TokenType == JsonTokenType.PropertyName)
                sb.Append(" - Property: ").Append(this.Value);

            if (this.Value != null)
                sb.Append(" - Value: ").Append(this.Value);

            return sb.ToString();
        }

        /// <summary>
        /// Compare two JsonReaderValue.
        /// </summary>
        public override readonly bool Equals(object obj) => obj is JsonReaderValue value &&
                   EqualityComparer<JsonValue>.Default.Equals(this.Value, value.Value) &&
                   this.TokenType == value.TokenType &&
                   this.Depth == value.Depth;

        /// <summary>
        /// Gets the hash code.
        /// </summary>
        public override readonly int GetHashCode() => base.GetHashCode();

        /// <summary>
        /// Compare two JsonReaderValue.
        /// </summary>
        public readonly bool Equals(JsonReaderValue other) => EqualityComparer<JsonValue>.Default.Equals(this.Value, other.Value) &&
            this.TokenType == other.TokenType &&
            this.Depth == other.Depth;
    }
}