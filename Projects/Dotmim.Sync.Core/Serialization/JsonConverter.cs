using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Dotmim.Sync.Core.Serialization
{
    public class JsonConverter<T> :  BaseConverter<T>
    {
        public override T Deserialize(Stream ms)
        {
            using (StreamReader sr = new StreamReader(ms))
            {
                return JsonConvert.DeserializeObject<T>(sr.ReadToEnd());

            }
        }

        public override void Serialize(T obj, Stream ms)
        {
            var serializedObjectString = JsonConvert.SerializeObject(obj);
            using (StreamWriter writer = new StreamWriter(ms))
            {
                writer.Write(serializedObjectString);
                writer.Flush();
            }
        }

    }
}
