using System;
using System.Collections.Generic;
using System.IO;
using System.Numerics;
using System.Text;

namespace Dotmim.Sync.Serialization
{
    internal class DmBinaryWriter : BinaryWriter
    {
        public DmBinaryWriter(Stream ms, Encoding encoding = null) : base(ms, encoding)
        {
        }

        /// <summary>
        /// Write primitive object 
        /// </summary>
        public void Write(object value, Type valueType)
        {

            if (valueType == typeof(bool))
                this.Write((bool)value);
            else if (valueType == typeof(byte))
                this.Write((byte)value);
            else if (valueType == typeof(char))
                this.Write((char)value);
            else if (valueType == typeof(double))
                this.Write((double)value);
            else if (valueType == typeof(float))
                this.Write((float)value);
            else if (valueType == typeof(int))
                this.Write((int)value);
            else if (valueType == typeof(long))
                this.Write((long)value);
            else if (valueType == typeof(short))
                this.Write((short)value);
            else if (valueType == typeof(uint))
                this.Write((uint)value);
            else if (valueType == typeof(ulong))
                this.Write((ulong)value);
            else if (valueType == typeof(ushort))
                this.Write((ushort)value);
            else if (valueType == typeof(byte[]))
                this.Write((byte[])value);
            else if (valueType == typeof(DateTime))
                this.Write((DateTime)value);
            else if (valueType == typeof(DateTimeOffset))
                this.Write((DateTimeOffset)value);
            else if (valueType == typeof(Decimal))
                this.Write((Decimal)value);
            else if (valueType == typeof(Guid))
                this.Write((Guid)value);
            else if (valueType == typeof(String))
                this.Write((String)value);
            else if (valueType == typeof(SByte))
                this.Write((SByte)value);
            else if (valueType == typeof(TimeSpan))
                this.Write((TimeSpan)value);
            else
                throw new ArgumentException("type not implemented");
        }



        public void WriteToConsole(Byte[] bytes)
        {
            return;
            //Console.Write("[");
            //foreach (var b in bytes)
            //    Console.Write(b);
            //Console.Write("]");
        }
        public override void Write(bool value) 
        {
            WriteToConsole(BitConverter.GetBytes(value));
            base.Write(value);
        }


        public override void Write(byte value)
        {
            WriteToConsole(BitConverter.GetBytes(value));
            base.Write(value);
        }

        public override void Write(char ch)
        {
            WriteToConsole(BitConverter.GetBytes(ch));
            base.Write(ch);
        }

        public override void Write(decimal value)
        {
            base.Write(value);
        }

        public override void Write(double value)
        {
            WriteToConsole(BitConverter.GetBytes(value));
            base.Write(value);
        }


        public override void Write(float value)
        {
            WriteToConsole(BitConverter.GetBytes(value));
            base.Write(value);
        }

        public override void Write(int value)
        {
            WriteToConsole(BitConverter.GetBytes(value));
            base.Write(value);
        }

        public override void Write(long value)
        {
            WriteToConsole(BitConverter.GetBytes(value));
            base.Write(value);
        }

        public override void Write(sbyte value)
        {
            WriteToConsole(BitConverter.GetBytes(value));
            base.Write(value);
        }

        public override void Write(short value)
        {
            WriteToConsole(BitConverter.GetBytes(value));
            base.Write(value);
        }

        public override void Write(string value)
        {
            WriteToConsole(Encoding.UTF8.GetBytes(value));
            base.Write(value);
        }

        public override void Write(uint value)
        {
            WriteToConsole(BitConverter.GetBytes(value));
            base.Write(value);
        }

        public override void Write(ulong value)
        {
            WriteToConsole(BitConverter.GetBytes(value));
            base.Write(value);
        }

        public override void Write(ushort value)
        {
            WriteToConsole(BitConverter.GetBytes(value));
            base.Write(value);
        }
        /// <summary>
        /// I need to write the buffer length (like Write(String) does)
        /// </summary>
        public override void Write(byte[] buffer)
        {
            WriteToConsole(buffer);
            var length = buffer.Length;
            this.Write7BitEncodedInt(length);
            base.Write(buffer);
        }

        public void Write(Guid value)
        {
            WriteToConsole(value.ToByteArray());
            var b = value.ToByteArray();
            base.Write(b);
        }


        public void Write(DateTimeOffset value)
        {
            WriteToConsole(BitConverter.GetBytes(value.Ticks));
            
            long ticks = value.Ticks;
            base.Write(ticks);
            long offset = value.Offset.Ticks;
            base.Write(offset);
        }

        public void Write(DateTime value)
        {
            WriteToConsole(BitConverter.GetBytes(value.Ticks));
            long ticks = value.Ticks;
            base.Write(ticks);
        }

        private void Write(TimeSpan value)
        {
            WriteToConsole(BitConverter.GetBytes(value.Ticks));
            long ticks = value.Ticks;
            base.Write(ticks);
        }

    }
}
