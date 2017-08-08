using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Dotmim.Sync.Serialization
{
    public class JsonConverter<T> : BaseConverter<T>
    {
        public override T Deserialize(Stream ms)
        {
            using (StreamReader sr = new StreamReader(ms))
            {
                var stringObject = sr.ReadToEnd();
                return JsonConvert.DeserializeObject<T>(stringObject);

            }
        }

        public override void Serialize(T obj, Stream ms)
        {
            var serializedObjectString = JsonConvert.SerializeObject(obj);
            StreamWriter writer = new StreamWriter(ms);
            writer.Write(serializedObjectString);
            
        }

        public override byte[] Serialize(T obj)
        {
            MemoryStream ms = new MemoryStream();
            var serializedObjectString = JsonConvert.SerializeObject(obj);
            using (StreamWriter writer = new StreamWriter(ms))
            {
                writer.Write(serializedObjectString);
            }
            return ms.ToArray();
        }

    }
}
