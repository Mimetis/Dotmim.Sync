using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync.Serialization.Serializers
{
    public class SerializableSerializer : TypeSerializer
    {
         public override object Deserialize(DmSerializer dmSerializer, Type objType, bool isDebugMode = false)
        {
            var br = dmSerializer.Reader;
            ConstructorInfo constructorInfo = objType.GetISerializableConstructor();

            var context = new StreamingContext();
            SerializationInfo serializationInfo = new SerializationInfo(objType, new FormatterConverter());

            var count = br.ReadInt32();


            for (int i = 0; i < count; i++)
            {
                var entryName = (string)dmSerializer.GetObject(isDebugMode);
                var entryValue = dmSerializer.GetObject(isDebugMode);

                serializationInfo.AddValue(entryName, entryValue, entryValue.GetType());
            }

            var obj = constructorInfo.Invoke(new object[] { serializationInfo, context });

            return obj;
        }

        public override void Serialize(DmSerializer dmSerializer, object obj, Type objType)
        {
            var context = new StreamingContext();
            var value = obj as ISerializable;
            SerializationInfo serializationInfo = new SerializationInfo(objType, new FormatterConverter());
            value.GetObjectData(serializationInfo, context);

            // write length
            dmSerializer.Writer.Write(serializationInfo.MemberCount);

            foreach (SerializationEntry serializationEntry in serializationInfo)
            {
                var entryName = serializationEntry.Name;
                var entryValue = serializationEntry.Value;

                dmSerializer.Serialize(entryName, entryName.GetType());
                dmSerializer.Serialize(entryValue, entryValue.GetType());

            }
        }
    }

   
    public class FormatterConverter : IFormatterConverter
    {
        public object Convert(object value, Type type)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ChangeType(value, type, CultureInfo.InvariantCulture);
        }

        public object Convert(object value, TypeCode typeCode)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ChangeType(value, typeCode, CultureInfo.InvariantCulture);
        }

        public bool ToBoolean(object value)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ToBoolean(value, CultureInfo.InvariantCulture);
        }

        public char ToChar(object value)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ToChar(value, CultureInfo.InvariantCulture);
        }

        public sbyte ToSByte(object value)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ToSByte(value, CultureInfo.InvariantCulture);
        }

        public byte ToByte(object value)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ToByte(value, CultureInfo.InvariantCulture);
        }

        public short ToInt16(object value)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ToInt16(value, CultureInfo.InvariantCulture);
        }

        public ushort ToUInt16(object value)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ToUInt16(value, CultureInfo.InvariantCulture);
        }

        public int ToInt32(object value)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }

        public uint ToUInt32(object value)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ToUInt32(value, CultureInfo.InvariantCulture);
        }

        public long ToInt64(object value)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ToInt64(value, CultureInfo.InvariantCulture);
        }

        public ulong ToUInt64(object value)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ToUInt64(value, CultureInfo.InvariantCulture);
        }

        public float ToSingle(object value)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ToSingle(value, CultureInfo.InvariantCulture);
        }

        public double ToDouble(object value)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ToDouble(value, CultureInfo.InvariantCulture);
        }

        public decimal ToDecimal(object value)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        }

        public DateTime ToDateTime(object value)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ToDateTime(value, CultureInfo.InvariantCulture);
        }

        public string ToString(object value)
        {
            if (value == null) ThrowValueNullException();
            return System.Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        private static void ThrowValueNullException()
        {
            throw new ArgumentNullException("value");
        }
    }


}
