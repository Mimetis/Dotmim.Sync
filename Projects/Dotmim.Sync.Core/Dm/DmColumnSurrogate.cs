using Dotmim.Sync.Data;
using System;
using System.Data;

namespace Dotmim.Sync.Data.Surrogate
{
    /// <summary>
    /// Represents a surrogate of a DmColumn object, which DotMim sync uses during custom binary serialization.
    /// </summary>
    [Serializable]
    public class DmColumnSurrogate
    {
        /// <summary>Gets or sets the name of the column that the DmColumnSurrogate object represents.</summary>
        public string ColumnName { get; set; }

        /// <summary>
        /// Gets or sets the dm type of the column that the DmColumnSurrogate object represents.
        /// </summary>
        public Type DataType { get; set; }
        internal bool dbTypeAllowed;
        public DbType DbType { get; set; }
        public bool AllowDBNull { get; set; } = true;
        public bool Unique { get; set; } = false;
        public bool ReadOnly { get; set; } = false;
        public int MaxLength { get; internal set; }
        public int Ordinal { get; internal set; }
        public bool PrecisionSpecified { get; set; }
        public bool ScaleSpecified { get; set; }
        public Boolean AutoIncrement { get; set; }
        public Byte Precision { get; internal set; }
        public Byte Scale { get; set; }

        /// <summary>
        /// Only used for Serialization
        /// </summary>
        public DmColumnSurrogate()
        {

        }

        /// <summary>
        /// Initializes a new instance of the DmColumnSurrogate class.
        /// </summary>
        public DmColumnSurrogate(DmColumn dc)
        {
            if (dc == null)
                throw new ArgumentNullException("dc", "DmColumn");


            this.dbTypeAllowed = dc.dbTypeAllowed;
            if (dc.dbTypeAllowed)
                this.DbType = dc.DbType;

            this.AllowDBNull = dc.AllowDBNull;
            this.ColumnName = dc.ColumnName;
            this.ReadOnly = dc.ReadOnly;
            this.MaxLength = dc.MaxLength;
            this.AutoIncrement = dc.AutoIncrement;
            this.Precision = dc.Precision;
            this.PrecisionSpecified = dc.PrecisionSpecified;
            this.Scale = dc.Scale;
            this.ScaleSpecified = dc.ScaleSpecified;
            this.Unique = dc.Unique;
            this.DataType = dc.DataType;
            this.Ordinal = dc.Ordinal;
        }

        /// <summary>
        /// Constructs a DmColumn object based on a DmColumnSurrogate object.
        /// </summary>
        public DmColumn ConvertToDmColumn()
        {
            DmColumn dmColumn = DmColumn.CreateColumn(this.ColumnName, this.DataType);

            dmColumn.dbTypeAllowed = this.dbTypeAllowed;
            if (dmColumn.dbTypeAllowed)
                dmColumn.DbType = this.DbType;

            dmColumn.AllowDBNull = this.AllowDBNull;
            dmColumn.ReadOnly = this.ReadOnly;
            dmColumn.MaxLength = this.MaxLength;
            dmColumn.AutoIncrement = this.AutoIncrement;
            dmColumn.Precision = this.Precision;
            dmColumn.PrecisionSpecified = this.PrecisionSpecified;
            dmColumn.Scale = this.Scale;
            dmColumn.ScaleSpecified = this.ScaleSpecified;
            dmColumn.Unique = this.Unique;
            dmColumn.SetOrdinal(this.Ordinal);

            return dmColumn;
        }
    }
}