using DmBinaryFormatter;
using Dotmim.Sync.Data;
using System;
using System.Data;
using System.Text;

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
        public int DbType { get; set; }
        public bool AllowDBNull { get; set; } = true;
        public bool Unique { get; set; } = false;
        public bool ReadOnly { get; set; } = false;
        public int MaxLength { get;  set; }
        public int Ordinal { get;  set; }
        public bool PrecisionSpecified { get; set; }
        public bool ScaleSpecified { get; set; }
        public Boolean AutoIncrement { get; set; }
        public Byte Precision { get;  set; }
        public Byte Scale { get; set; }
        public String OrginalDbType { get; set; }

        /// <summary>
        /// Gets or sets the dm type of the column that the DmColumnSurrogate object represents.
        /// </summary>
        public String DataType { get; set; }
        internal bool dbTypeAllowed;

        /// <summary>
        /// Only used for Serialization
        /// </summary>
        public DmColumnSurrogate()
        {



        }

        public long GetBytesLength()
        {
            long bytesLength = String.IsNullOrEmpty(ColumnName) ? 1L : Encoding.UTF8.GetBytes(ColumnName).Length;
            bytesLength += 4L; // DbType
            bytesLength += 1L; // AllowDBNull
            bytesLength += 1L; // Unique
            bytesLength += 1L; // Readonly
            bytesLength += 4L; // Maxlength
            bytesLength += 4L; // Ordinal
            bytesLength += 1L; // PrecisionsSpecified
            bytesLength += 1L; // ScaleScpecified
            bytesLength += 1L; // Autoinc
            bytesLength += 1L; // Precision
            bytesLength += 1L; // Scale
            bytesLength += 1L; // dbTypeAllowed
            bytesLength += String.IsNullOrEmpty(OrginalDbType) ? 1L : Encoding.UTF8.GetBytes(OrginalDbType).Length;
            bytesLength += Encoding.UTF8.GetBytes(DataType).Length; //Type
            bytesLength += Encoding.UTF8.GetBytes(this.GetType().GetAssemblyQualifiedName()).Length; // Type

            return bytesLength;
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
                this.DbType = (int)dc.DbType;

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
            this.DataType = dc.DataType.GetAssemblyQualifiedName();
            this.Ordinal = dc.Ordinal;
            this.OrginalDbType = dc.OrginalDbType;
        }

        /// <summary>
        /// Constructs a DmColumn object based on a DmColumnSurrogate object.
        /// </summary>
        public DmColumn ConvertToDmColumn()
        {
            DmColumn dmColumn = DmColumn.CreateColumn(this.ColumnName, DmUtils.GetTypeFromAssemblyQualifiedName(this.DataType));

            dmColumn.dbTypeAllowed = this.dbTypeAllowed;
            if (dmColumn.dbTypeAllowed)
                dmColumn.DbType = (DbType)this.DbType;

            dmColumn.AllowDBNull = this.AllowDBNull;
            dmColumn.ReadOnly = this.ReadOnly;
            dmColumn.MaxLength = this.MaxLength;
            dmColumn.AutoIncrement = this.AutoIncrement;
            dmColumn.Precision = this.Precision;
            dmColumn.PrecisionSpecified = this.PrecisionSpecified;
            dmColumn.Scale = this.Scale;
            dmColumn.ScaleSpecified = this.ScaleSpecified;
            dmColumn.Unique = this.Unique;
            dmColumn.OrginalDbType = this.OrginalDbType;
            dmColumn.SetOrdinal(this.Ordinal);

            return dmColumn;
        }
    }
}