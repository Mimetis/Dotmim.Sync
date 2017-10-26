using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Dotmim.Sync.Serialization
{
    public interface ISerializer
    {
        void Serialize<T>(T obj, Stream ms);
        T Deserialize<T>(Stream ms);
        byte[] Serialize<T>(T obj);
    }
}
