using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Dotmim.Sync.Core.Serialization
{
    public abstract class BaseConverter<T>
    {
        public abstract void Serialize(T obj, Stream ms);
        public abstract T Deserialize(Stream ms);


    }
}
