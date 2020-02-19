using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.Serialization
{

    public class ContractSerializerFactory : ISerializerFactory
    {
        public string Key => "dc";
        private static ContractSerializerFactory instance = null;
        public static ContractSerializerFactory Current => instance ?? new ContractSerializerFactory();

        public ISerializer<T> GetSerializer<T>() => new ContractSerializer<T>();

    }


    public delegate void DeserialiazeCallback<T>(T result, TaskCompletionSource<T> tcs);


    public class OldFashionWay<T>
    {
        public Stream Ms { get; }
        private DeserialiazeCallback<T> callback;
        private readonly TaskCompletionSource<T> tcs;

        public OldFashionWay(DeserialiazeCallback<T> callback, Stream ms, TaskCompletionSource<T> tcs)
        {
            this.callback = callback;
            this.Ms = ms;
            this.tcs = tcs;
        }

        public void Deserialiaze()
        {
            var instanceCaller = new Thread(new ParameterizedThreadStart(InternalDeserialize));
            instanceCaller.Start(this.Ms);

        }

        public void InternalDeserialize(object ms)
        {
            var serializer = new DataContractSerializer(typeof(T));
            var res = (T)serializer.ReadObject(ms as Stream);

            callback?.Invoke(res, tcs);
        }


    }

    public class ContractSerializer<T> : ISerializer<T>
    {

        public ContractSerializer()
        {
        }

        public async Task<T> DeserializeAsync(Stream ms)
        {
            using (var ims = new MemoryStream())
            {
                // Quick fix to not being IO Synchronous, new refused feature from .Net Core 3.1
                // Even if you try a Task.Run or a ThreadPool.Queue and so on, you will enventually had this exception:
                // "Synchronous operations are disallowed. Call ReadAsync or set AllowSynchronousIO to true instead"
                await ms.CopyToAsync(ims);
                ims.Seek(0, SeekOrigin.Begin);

                var serializer = new DataContractSerializer(typeof(T));

                var res = (T)serializer.ReadObject(ims);
                return res;
            }


        }

     
        public Task<byte[]> SerializeAsync(T obj)
        {
            var serializer = new DataContractSerializer(typeof(T));

            using (var ms = new MemoryStream())
            {
                serializer.WriteObject(ms, obj);
                return Task.FromResult(ms.ToArray());
            }

        }
    }
}

