using DmBinaryFormatter.Converters;
using DmBinaryFormatter.Serializers;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;

namespace DmBinaryFormatter
{

    /// <summary>
    /// Serializer. Use it to serialize and deserialize any kind of object
    /// </summary>
    public class DmSerializer
    {
        /// <summary>
        /// Gets or sets the Encoding used to serialize strings
        /// </summary>
        public Encoding Encoding { get; set; }

        /// <summary>
        /// Get the reader used to read the stream
        /// </summary>
        internal DmBinaryReader Reader { get; private set; }

        /// <summary>
        /// Get the writer used to write to the stream
        /// </summary>
        internal DmBinaryWriter Writer { get; private set; }

        /// <summary>
        /// A textwriter used to debug when trying to deserialize (try Console.Out)
        /// </summary>
        public TextWriter DebugWriter { get; set; }


        private enum DmState : byte
        {
            IsNull = 0,
            IsAloneOrValue = 1,
            IsReference = 2
        }

        /// used to managed references
        private int index = 0;
        private Dictionary<Object, int> fromObjects = new Dictionary<Object, int>();
        private Dictionary<int, Object> fromIndexes = new Dictionary<int, object>();

        /// <summary>
        /// get the state of an object, and get the new id (or the ref id)
        /// </summary>
        private DmState GetState(Object obj, Type objType, ref int id)
        {
            if (obj == null)
            {
                this.index++;
                id = this.index;
                return DmState.IsNull;
            }

            if (objType.IsPrimitiveManagedType())
            {
                this.index++;
                id = this.index;
                return DmState.IsAloneOrValue;
            }

            var ptrObj = fromObjects.ContainsKey(obj);

            if (ptrObj)
            {
                id = fromObjects[obj];
                return DmState.IsReference;
            }

            this.index++;
            fromObjects.Add(obj, this.index);
            id = this.index;
            return DmState.IsAloneOrValue;

        }

        /// <summary>
        /// Constrcutor. use Encoding.UTF8 by default
        /// </summary>
        public DmSerializer()
        {
            this.Encoding = Encoding.UTF8;
        }

        /// <summary>
        /// Constructuror, you have to define the Encoding used to serialize strings
        /// </summary>
        /// <param name="encoding"></param>
        public DmSerializer(Encoding encoding)
        {
            this.Encoding = encoding;
        }

        /// <summary>
        /// Register a converter
        /// </summary>
        public void RegisterConverter(Type type, ObjectConverter converter)
        {
            ObjectConverter.AddConverter(type, converter);
        }

        /// <summary>
        /// Serialize a T object in an open writable stream
        /// </summary>
        public void Serialize<T>(T obj, Stream s)
        {
            if (obj == null)
                throw new ArgumentException("Object is null");

            if (!s.CanRead || !s.CanWrite)
                throw new ArgumentException("Can't Read / write in the stram");

            this.Writer = new DmBinaryWriter(s, this.Encoding);

            // Clear references
            fromObjects.Clear();
            fromIndexes.Clear();

            index = 0;

            var objectType = obj.GetType();

            this.Serialize(obj, objectType);
        }

        /// <summary>
        /// Serialize a T object using an internal memorystream and returning a Byte array
        /// </summary>
        public byte[] Serialize<T>(T obj)
        {
            if (obj == null)
                throw new ArgumentException("Object is null");

            using (MemoryStream ms = new MemoryStream())
            {
                this.Serialize(obj, ms);

                return ms.ToArray();
            }
        }

        /// <summary>
        /// Recursive method, serializing every object in my objects graph
        /// </summary>
        internal void Serialize(object obj, Type objType)
        {

            // If it's Object, we get the underlying type
            if (objType == typeof(Object) && obj != null)
            {
                var baseType = obj.GetType().GetBaseType();
                var s = TypeSerializer.GetSerializer(baseType);
                Serialize(obj, baseType);
                return;
            }

            var serializer = TypeSerializer.GetSerializer(objType);

            // Write object type
            this.Writer.Write(objType.AssemblyQualifiedName);

            // Check if it's not a reference
            int refIndex = 0;
            DmState state = this.GetState(obj, objType, ref refIndex);

            // Write state
            this.Writer.Write((byte)state);

            // Write index;
            this.Writer.Write(refIndex);

            // if not null or ref
            if (state == DmState.IsAloneOrValue)
                serializer.Serialize(this, obj, objType);
        }

        /// <summary>
        /// Deserialize a byte array using an internal MemoryStream
        /// </summary>
        public Object Deserialize(Byte[] bytes)
        {
            using (MemoryStream s = new MemoryStream(bytes))
            {
                return Deserialize(s);
            }
        }

        /// <summary>
        /// Deserialize a stream containing a binary instance
        /// </summary>
        public object Deserialize(Stream s)
        {
            if (!s.CanRead || !s.CanWrite)
                throw new ArgumentException("Can't Read / write in the stram");

            Object obj = null;
            using (Reader = new DmBinaryReader(s, this.Encoding))
            {
                obj = this.GetObject(this.DebugWriter != null);
            }

            fromIndexes.Clear();
            fromObjects.Clear();

            return obj;
        }

        /// <summary>
        /// Deserialize a stream containing a binary instance of T
        /// </summary>
        public T Deserialize<T>(Stream s)
        {
            return (T)Deserialize(s);
        }

        /// <summary>
        /// Deserialize a byte array using an internal MemoryStream and returning an instance of T
        /// </summary>
        public T Deserialize<T>(Byte[] bytes)
        {
            return (T)Deserialize(bytes);
        }

        /// <summary>
        /// Recursive deserialise method to get every object from my stream
        /// </summary>
        internal Object GetObject(bool isDebugMode = false)
        {
            Object deserializedObject = null;


            int indent = 0;

            if (this.Reader.BaseStream.Position >= this.Reader.BaseStream.Length)
                throw new IndexOutOfRangeException("stream is ended !");


            if (isDebugMode)
            {
                DebugWriter.WriteLineIndent(indent);
                DebugWriter.Write("{");
            }

            // Get Type
            var objTypeFromStream = Reader.ReadString();
            var objType = Type.GetType(objTypeFromStream);

            if (isDebugMode)
            {
                DebugWriter.Write($"[{objType.Name}]");
            }


            // Get State and Index
            var state = (DmState)Reader.ReadByte();
            var index = Reader.ReadInt32();

            if (isDebugMode)
                DebugWriter.Write($"[{state}][{index}]");

            TypeSerializer serializer = null;
            serializer = TypeSerializer.GetSerializer(objType);

            if (state == DmState.IsReference)
            {
                deserializedObject = fromIndexes[index];
            }
            else if (state != DmState.IsNull)
            {
                deserializedObject = serializer.Deserialize(this, objType, isDebugMode);

                // Dont make a reference on value type
                if (!objType.IsPrimitiveManagedType())
                    fromIndexes.Add(index, deserializedObject);
            }

            if (isDebugMode)
            {
                // cosmetic
                if (serializer.GetType() != typeof(PrimitiveSerializer))
                {
                    indent--;
                    DebugWriter.WriteLineIndent(indent);
                }
                DebugWriter.Write("}");
            }

            return deserializedObject;

        }

    }
}
