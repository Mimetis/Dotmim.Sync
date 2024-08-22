using Dotmim.Sync.Extensions;
using Dotmim.Sync.Serialization;
using Microsoft.AspNetCore.Http;

namespace Dotmim.Sync.Web.Server
{
    /// <summary>
    /// Session extensions.
    /// </summary>
    public static class SessionExtensions
    {
        private static ISerializer serializer = SerializersFactory.JsonSerializerFactory.GetSerializer();

        /// <summary>
        /// Get a value from the session.
        /// </summary>
        public static T Get<T>(this ISession session, string key)
        {
            var data = session.GetString(key);

            return data == null ? default : serializer.Deserialize<T>(data);
        }

        /// <summary>
        /// Set a value in the session.
        /// </summary>
        public static void Set<T>(this ISession session, string key, T value)
        {
            var jsonBytes = serializer.Serialize(value);

            session.SetString(key, jsonBytes.ToUtf8String());
        }
    }
}