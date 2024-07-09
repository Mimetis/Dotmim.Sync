using Dotmim.Sync.Extensions;
using Dotmim.Sync.Serialization;
using Microsoft.AspNetCore.Http;

namespace Dotmim.Sync.Web.Server
{
    public static class SessionExtensions
    {
        private static ISerializer serializer = SerializersFactory.JsonSerializerFactory.GetSerializer();

        public static T Get<T>(this ISession session, string key)
        {
            var data = session.GetString(key);
            
            if (data == null)
                return default;
            
            return serializer.Deserialize<T>(data);
        }

        public static void Set<T>(this ISession session, string key, T value)
        {
            var jsonBytes = serializer.Serialize(value);

            session.SetString(key, jsonBytes.ToUtf8String());
        }
    }
}
