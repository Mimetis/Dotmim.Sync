using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;

namespace DmBinaryFormatter.Serializers
{
    public class DictionarySerializer : TypeSerializer
    {
        public override void Serialize(DmSerializer dmSerializer, object obj, Type objType)
        {
            IDictionary dictionary = obj as IDictionary;

            if (dictionary == null)
                return;

            var args = objType.GetGenericArguments();
            Type keyType = args[0].GetBaseType();
            Type valueType = args[1].GetBaseType();
            var keySerializer = TypeSerializer.GetSerializer(keyType);
            var valueSerializer = TypeSerializer.GetSerializer(valueType);

            // write length
            dmSerializer.Writer.Write(dictionary.Count);

            // Iterate through the dictionary
            foreach (DictionaryEntry entry in dictionary)
            {
                object value = entry.Value;
                object key = entry.Key;

                dmSerializer.Serialize(key, keyType);
                dmSerializer.Serialize(value, valueType);
            }

        }

        public override object Deserialize(DmSerializer dmSerializer, Type objType, bool isDebugMode = false)
        {
            var writer = dmSerializer.DebugWriter;
            var br = dmSerializer.Reader;

            // Get key / value base type
            var args = objType.GetGenericArguments();
            Type keyType = args[0].GetBaseType();
            Type valueType = args[1].GetBaseType();
            String keyName = keyType.Name;
            String valueName = valueType.Name;
            var keySerializer = TypeSerializer.GetSerializer(keyType);
            var valueSerializer = TypeSerializer.GetSerializer(valueType);

            // Create instance
            var arrayInstance = objType.CreateInstance() as IDictionary;

            var count = br.ReadInt32();

            for (int i = 0; i < count; i++)
            {
                var keyValue = dmSerializer.GetObject(isDebugMode);
                var itemValue = dmSerializer.GetObject(isDebugMode);

                if (keyValue != null)
                    arrayInstance.Add(keyValue, itemValue);
            }

            return arrayInstance;
        }
    }
}
