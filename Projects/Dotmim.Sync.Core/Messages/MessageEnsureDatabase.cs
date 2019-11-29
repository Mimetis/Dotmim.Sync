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

        public MessageEnsureDatabase(bool checkSchema, DmSet schema, ICollection<FilterClause> filters)
        {
            this.CheckSchema = checkSchema;
            this.Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            this.Filters = filters;
        }

        /// <summary>
        /// Gets or Sets if we need to check all tables
        /// </summary>
        public bool CheckSchema { get; set; }

        /// <summary>
        /// Gets or Sets the database schema
        /// </summary>
        public DmSet Schema { get => _schema; set => _schema = value; }

        /// <summary>
        /// Gets or Sets the filters used during the sync, to be applied on the database
        /// </summary>
        public ICollection<FilterClause> Filters { get; set; }

    }
}
