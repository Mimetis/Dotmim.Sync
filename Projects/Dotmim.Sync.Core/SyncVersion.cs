using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{

    //public enum SyncVersionEnum
    //{
    //    V030,
    //    V057,
    //    V060,
    //    V061
    //}

    public static class SyncVersion
    {
        public static Version Current { get; } = new Version(0, 6, 2);

        public static Version EnsureVersion(string v) => v == "1" ? new Version(0, 5, 7) : new Version(v);
    }


    //public class SyncVersion
    //{
    //    /// <summary>
    //    /// Get the version enum
    //    /// </summary>
    //    public SyncVersionEnum Version { get; }

    //    /// <summary>
    //    /// Gets the last version
    //    /// </summary>
    //    public static SyncVersion Current { get; } = new SyncVersion(SyncVersionEnum.V061);

    //    /// <summary>
    //    /// Create a new version
    //    /// </summary>
    //    public SyncVersion(SyncVersionEnum v) => this.Version = v;

    //    public SyncVersion(string v) => this.Version = v switch
    //    {
    //        "0.3.0" => SyncVersionEnum.V030,
    //        // special case for version 1: Consider it's a version at least 0.5
    //        "1" => SyncVersionEnum.V057,
    //        "0.5.7" => SyncVersionEnum.V057,
    //        "0.6.0" => SyncVersionEnum.V060,
    //        "0.6.1" => SyncVersionEnum.V061,
    //        _ => Current.Version
    //    };


    //    public override string ToString() => this.Version switch
    //    {
    //        SyncVersionEnum.V030 => "0.3.0",
    //        SyncVersionEnum.V057 => "0.5.7",
    //        SyncVersionEnum.V060 => "0.6.0",
    //        SyncVersionEnum.V061 => "0.6.1",
    //        _ => "0.6.1"
    //    };
    //}
}
