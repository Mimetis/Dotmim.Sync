using System;

namespace Dotmim.Sync.Cache
{
    /// <summary>
    /// TODO : ICache should be only used on Web providers.
    /// Maybe delete this memory system and rely on web session / cache from ASP.NET 
    /// </summary>
    public interface ICache
    {
        void Clear();
        T GetValue<T>(string key);
        void Remove(string key);
        void Set<T>(string cacheKey, T value);
    }
}