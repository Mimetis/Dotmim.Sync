using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace Dotmim.Sync.Serialization.Serializers
{
    class CoreConverterSerializer : TypeSerializer
    {
        private TypeConverter converter;

        public CoreConverterSerializer(TypeConverter converter)
        {
            this.converter = converter;
        }

        public override void Serialize(DmSerializer dmSerializer, object obj, Type objType)
        {
            // write primitive
            var objTypeString = typeof(string);
            var objConverterd = this.converter.ConvertToString(obj);
            dmSerializer.Writer.Write(objConverterd, objTypeString);
        }

        public override object Deserialize(DmSerializer dmSerializer, Type objType, bool isDebugMode = false)
        {
            var objTypeString = typeof(string);

            // Get the value
            var typeString = dmSerializer.Reader.ReadString();

            var obj = this.converter.ConvertFromString(typeString);

            if (isDebugMode)
                dmSerializer.DebugWriter.Write(obj);

            return obj;
        }
    }
}
