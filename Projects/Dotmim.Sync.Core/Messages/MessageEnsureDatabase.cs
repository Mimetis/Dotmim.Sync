using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Filter;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Messages
{
    /// <summary>
    /// Message exchanged during the Ensure Database sync stage
    /// </summary>
    public class MessageEnsureDatabase
    {
        private DmSet _schema;

        public MessageEnsureDatabase(ScopeInfo scopeInfo, DmSet schema, ICollection<FilterClause> filters, SerializationFormat serializationFormat)
        {
            this.ScopeInfo = scopeInfo ?? throw new ArgumentNullException(nameof(scopeInfo));
            this.Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            this.Filters = filters;
            this.SerializationFormat = serializationFormat;
        }

        /// <summary>
        /// Gets or Sets he scope info used during the ensure database sync stage
        /// </summary>
        public ScopeInfo ScopeInfo { get; set; }

        /// <summary>
        /// Gets or Sets the database schema
        /// </summary>
        public DmSet Schema { get => _schema; set => _schema = value; }

        /// <summary>
        /// Gets or Sets the filters used during the sync, to be applied on the database
        /// </summary>
        public ICollection<FilterClause> Filters { get; set; }

        /// <summary>
        /// Gets or Sets the Serialization format used during the sync
        /// </summary>
        public SerializationFormat SerializationFormat { get; set; }

    }
}
