using Dotmim.Sync.Enumerations;
using System;
using System.Runtime.Serialization;

namespace Dotmim.Sync.Web
{
    /// <summary>
    /// Serialize exception to be sent over the wire.
    /// </summary>
    [Serializable, DataContract(Name = "ex")]
#pragma warning disable CA1711 // Identifiers should not have incorrect suffix
    public class WebSyncException
#pragma warning restore CA1711 // Identifiers should not have incorrect suffix
    {
        /// <summary>
        /// Gets or Sets type name of inner exception.
        /// </summary>
        [DataMember(Name = "tn", IsRequired = false, EmitDefaultValue = false, Order = 1)]
        public string TypeName { get; set; }

        /// <summary>
        /// Gets or Sets the MEssage associated to the exception.
        /// </summary>
        [DataMember(Name = "m", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public string Message { get; set; }

        /// <summary>
        /// Gets or sets sync stage when exception occured.
        /// </summary>
        [DataMember(Name = "ss", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public SyncStage SyncStage { get; set; }

        /// <summary>
        /// Gets or sets data source error number if available.
        /// </summary>
        [DataMember(Name = "n", IsRequired = false, Order = 5)]
        public int Number { get; set; }

        /// <summary>
        /// Gets or Sets data source if available.
        /// </summary>
        [DataMember(Name = "d", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public string DataSource { get; set; }

        /// <summary>
        /// Gets or Sets initial catalog if available.
        /// </summary>
        [DataMember(Name = "ic", IsRequired = false, EmitDefaultValue = false, Order = 7)]
        public string InitialCatalog { get; set; }
    }
}