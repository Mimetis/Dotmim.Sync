using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tools
{
    public enum ArgumentType
    {
        None,
        // Root
        RootVersion,
        RootHelp,
        RootSync,
        RootVerbose,
        // Project
        ProjectNew,
        ProjectInfo,
        ProjectDelete,
        ProjectList,
        // Provider
        ProviderProviderType,
        ProviderConnectionString,
        ProviderSyncType,
        // Table
        TableAdd,
        TableSchema,
        TableRemove,
        TableDirection,
        // Configuration
        ConfigurationConflict,
        ConfigurationBatchSize,
        ConfigurationBatchDirectory,
        ConfigurationFormat,
        ConfigurationBulkOperations,
        // Yaml
        YamlFileName,
    }
}
