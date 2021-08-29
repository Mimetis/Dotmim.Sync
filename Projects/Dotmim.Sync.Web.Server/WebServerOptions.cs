using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Text;

namespace Dotmim.Sync.Web.Server
{
    public class WebServerOptions
    {
        //public MemoryCacheEntryOptions GetServerCacheOptions()
        //{
        //    var sessionCacheEntryOptions = new MemoryCacheEntryOptions();
        //    sessionCacheEntryOptions.SetSlidingExpiration(this.ServerCacheSlidingExpiration);
        //    return sessionCacheEntryOptions;
        //}

        //public MemoryCacheEntryOptions GetClientCacheOptions()
        //{
        //    var sessionCacheEntryOptions = new MemoryCacheEntryOptions();
        //    sessionCacheEntryOptions.SetSlidingExpiration(this.ClientCacheSlidingExpiration);
        //    return sessionCacheEntryOptions;
        //}

        /// <summary>
        /// Gets or Sets Converters used by different clients
        /// </summary>
        public Collection<IConverter> Converters { get; set; }

        ///// <summary>
        ///// Gets or Sets how long the server cache entry can be inactive(e.g.not accessed) before it will be removed. Default is 1h
        ///// </summary>
        //public TimeSpan ServerCacheSlidingExpiration { get; set; }

        ///// <summary>
        ///// Gets or Sets how long the client session cache entry can be inactive(e.g.not accessed) before it will be removed. Default is 10 min
        ///// </summary>
        //public TimeSpan ClientCacheSlidingExpiration { get; set; }


        /// <summary>
        /// Create a new instance of options with default values
        /// </summary>
        public WebServerOptions() : base()
        {
            this.Converters = new Collection<IConverter>();
            //this.ServerCacheSlidingExpiration = TimeSpan.FromHours(12);
            //this.ClientCacheSlidingExpiration = TimeSpan.FromMinutes(10);

        }

    }
}
