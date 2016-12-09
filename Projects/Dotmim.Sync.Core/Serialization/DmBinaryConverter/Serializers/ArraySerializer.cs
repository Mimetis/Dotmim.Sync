using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Linq;

namespace DmBinaryFormatter.Serializers
{
    public class ArraySerializer : TypeSerializer
    {

        private Type GetArrayItemValueType(Type arrayType)
        {
            // Get value base type
            Type valueType;

            var implementedInterfaces = arrayType.GetInterfaces();

            var args = implementedInterfaces.SelectMany(t => t.GetGenericArguments()).Distinct().ToArray();

            if (args.Length <= 0)
            {
                var objTypeInfo = arrayType.GetTypeInfo();

                // a simple [] object
                if (objTypeInfo.IsArray)
                    valueType = objTypeInfo.GetElementType();
                else
                    valueType = typeof(object);
            }
            else
            {
                valueType = args[0];
            }

            return valueType;

        }
        public override void Serialize(DmSerializer dmSerializer, object obj, Type objType)
        {
            IEnumerable list = obj as IEnumerable;

            if (list == null)
                return;

            var valueType = GetArrayItemValueType(objType);

            IEnumerator e = list.GetEnumerator();

            PropertyInfo pi = objType.GetProperties().Where(p => p.Name == "Count" || p.Name == "Length").FirstOrDefault();

            if (pi == null)
                throw new ArgumentException("an array should have a Count / Length property");

            int count = (int)pi.GetValue(obj);

            // write length
            dmSerializer.Writer.Write(count);

            // Iterate through the dictionary
            while (e.MoveNext())
            {
                object value = e.Current;
                dmSerializer.Serialize(value, valueType);
            }
        }


        public override object Deserialize(DmSerializer dmSerializer, Type objType, bool isDebugMode = false)
        {
            var br = dmSerializer.Reader;

            // DOnt try to get an ICollection (coz ICollection<T> doesn't inherit from it)
            object arrayInstance;
            // Create instance
            if (objType.IsArray)
                arrayInstance = new ArrayList();
            else
                arrayInstance = objType.CreateInstance();

            var count = br.ReadInt32();

            // Get the Add method if exist
            // Since ICollection<T> is not accessible
            var valueType = GetArrayItemValueType(objType);

            Func<object, int> addFunction = null;
            if (objType.IsArray)
            {
                addFunction = ((ArrayList)arrayInstance).Add;
            }
            else
            {
                MethodInfo mi = objType.GetMethod("Add", new Type[] { valueType });

                if (mi == null)
                    throw new ArgumentException("Deserializing an array must have a Add<T> method");

                addFunction = new Func<object, int>((value) =>
                {
                    mi.Invoke(arrayInstance, new object[] { value });
                    return 0;
                });
            }


            for (int i = 0; i < count; i++)
            {
                var item = dmSerializer.GetObject(isDebugMode);

                addFunction(item);
            }

            if (objType.IsArray)
                return ((ArrayList)arrayInstance).ToArray(objType.GetTypeInfo().GetElementType());
            else
                return arrayInstance;
        }

        public IEnumerator GetItem()
        {
            return null;
        }
    }
}
