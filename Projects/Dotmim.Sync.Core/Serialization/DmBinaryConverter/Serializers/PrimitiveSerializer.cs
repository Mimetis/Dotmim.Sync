using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Dotmim.Sync.Serialization.Serializers
{
    public class PrimitiveSerializer : TypeSerializer
    {

        public override void Serialize(DmSerializer dmSerializer, object obj, Type objType)
        {
            Type baseType = null;

            // Get the base type if Nullable<>
            baseType = objType.GetBaseType();

            // write primitive
            dmSerializer.Writer.Write(obj, baseType);
        }



        public override Object Deserialize(DmSerializer dmSerializer, Type objType, bool isDebugMode = false)
        {
            Type baseType = null;

            // Get the base type if Nullable<>
            baseType = objType.GetBaseType();

            // Get the value
            var value = dmSerializer.Reader.Read(baseType);

            if (isDebugMode)
                dmSerializer.DebugWriter.Write(value);
            
            return value;

        }
    }
}
