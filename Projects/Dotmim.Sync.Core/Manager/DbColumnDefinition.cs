using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Manager
{
    /// <summary>
    /// Column definition from the datastore.
    /// This class is used only when retrieving the columns definition from the datastore
    /// </summary>
    public class DbColumnDefinition
    {
        /// <summary>
        /// Gets or sets the column name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the column ordinal
        /// </summary>
        public int Ordinal { get; set; }

        /// <summary>
        /// Gets or sets the column datastore type name
        /// </summary>
        public string TypeName { get; set; }

        /// <summary>
        /// Gets or sets the column max length
        /// </summary>
        public long MaxLength { get; set; }

        /// <summary>
        /// Gets or sets the column precision
        /// </summary>
        public byte Precision { get; set; }


        /// <summary>
        /// Gets or sets the column scale
        /// </summary>
        public byte Scale { get; set; }

        /// <summary>
        /// Gets or sets if the column is nullable 
        /// </summary>
        public bool IsNullable { get; set; }

        /// <summary>
        /// Gets or sets if the column is auto increment
        /// </summary>
        public bool IsIdentity { get; set; }

        /// <summary>
        /// Gets or sets if the column is unsigned
        /// </summary>
        public bool IsUnsigned { get; set; }

        /// <summary>
        /// Gets or sets if the column is unicode 
        /// </summary>
        public bool IsUnicode { get; set; }

        /// <summary>
        /// Gets or sets if the column is a computed column
        /// </summary>
        public bool IsCompute { get; set; }

    }
}
