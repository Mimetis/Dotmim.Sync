using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DmBinaryFormatter
{
    internal class DmBinaryReader : BinaryReader
    {
        public DmBinaryReader(Stream ms) : base(ms)
        {
        }
        public DmBinaryReader(Stream ms, Encoding encoding) : base(ms, encoding)
        {
        }

        public object Read(Type valueType)
        {
            if (valueType == typeof(bool))
                return base.ReadBoolean();
            else if (valueType == typeof(Byte))
                return base.ReadByte();
            else if (valueType == typeof(Char))
                return base.ReadChar();
            else if (valueType == typeof(Double))
                return base.ReadDouble();
            else if (valueType == typeof(Single))
                return base.ReadSingle();
            else if (valueType == typeof(Int32))
                return base.ReadInt32();
            else if (valueType == typeof(Int64))
                return base.ReadInt64();
            else if (valueType == typeof(Int16))
                return base.ReadInt16();
            else if (valueType == typeof(UInt32))
                return base.ReadUInt32();
            else if (valueType == typeof(UInt64))
                return base.ReadUInt64();
            else if (valueType == typeof(UInt16))
                return base.ReadUInt16();
            else if (valueType == typeof(Byte[]))
                return this.ReadBytes();
            else if (valueType == typeof(DateTime))
                return this.ReadDatetime();
            else if (valueType == typeof(DateTimeOffset))
                return this.ReadDatetimeOffset();
            else if (valueType == typeof(Decimal))
                return base.ReadDecimal();
            else if (valueType == typeof(Guid))
                return this.ReadGuid();
            else if (valueType == typeof(String))
                return this.ReadString();
            else if (valueType == typeof(SByte))
                return base.ReadSByte();
            else if (valueType == typeof(TimeSpan))
                return this.ReadTimeSpan();

            return null;
        }
  
        public override string ReadString()
        {
            var res = base.ReadString();

            return res;
        }

        /// <summary>
        /// Read bytes with the length availabe in the first byte 
        /// </summary>
        public Byte[] ReadBytes()
        {
            var byteLength = this.Read7BitEncodedInt();

            if (byteLength < 0)
                throw new ArgumentOutOfRangeException("Byte length not serialized, can't read this byte array");

            if (byteLength == 0)
                return new Byte[0];

            return base.ReadBytes(byteLength);
        }

        public TimeSpan ReadTimeSpan()
        {
            long value = base.ReadInt64();
            return TimeSpan.FromTicks(value);
        }

        public Guid ReadGuid()
        {
            Byte[] buffer = new Byte[16];

            var arrayByte = base.ReadBytes(buffer.Length);

            return new Guid(arrayByte);

        }

        public DateTime ReadDatetime()
        {
            long value = base.ReadInt64();
            return new DateTime(value);
        }

        public DateTimeOffset ReadDatetimeOffset()
        {
            long value = base.ReadInt64();
            long offset = base.ReadInt64();
            return new DateTimeOffset(value, TimeSpan.FromTicks(offset));
        }
    }
}
