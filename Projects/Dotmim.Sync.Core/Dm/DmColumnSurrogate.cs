using Dotmim.Sync.Serialization;
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
        public string TableName { get; set; }
        public int DbType { get; set; }
        public bool AllowDBNull { get; set; } = true;
        public bool IsUnique { get; set; } = false;
        public bool IsReadOnly { get; set; } = false;
        public Int32 MaxLength { get;  set; }
        public int Ordinal { get;  set; }
        public bool PrecisionSpecified { get; set; }
        public bool ScaleSpecified { get; set; }
        public Boolean IsAutoIncrement { get; set; }
        public Byte Precision { get;  set; }
        public Byte Scale { get; set; }
        public String OriginalDbType { get; set; }
        public String OriginalTypeName { get; set; }
        public bool IsUnsigned { get; set; }
        public bool IsUnicode { get; set; }
        public bool IsCompute { get; set; }

        /// <summary>
        /// Gets or sets the dm type of the column that the DmColumnSurrogate object represents.
        /// </summary>
        public String DataType { get; set; }

        public bool DbTypeAllowed { get; set; }

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
            bytesLength += 1L; // IsUnique
            bytesLength += 1L; // Readonly
            bytesLength += 4L; // Maxlength
            bytesLength += 4L; // Ordinal
            bytesLength += 1L; // PrecisionsSpecified
            bytesLength += 1L; // ScaleScpecified
            bytesLength += 1L; // Autoinc
            bytesLength += 1L; // Precision
            bytesLength += 1L; // Scale
            bytesLength += 1L; // dbTypeAllowed
            bytesLength += 1L; // IsUnsigned
            bytesLength += 1L; // IsCompute
            bytesLength += 1L; // IsUnicode
            bytesLength += String.IsNullOrEmpty(TableName) ? 1L : Encoding.UTF8.GetBytes(TableName).Length;
            bytesLength += String.IsNullOrEmpty(OriginalDbType) ? 1L : Encoding.UTF8.GetBytes(OriginalDbType).Length;
            bytesLength += String.IsNullOrEmpty(OriginalTypeName) ? 1L : Encoding.UTF8.GetBytes(OriginalTypeName).Length;
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


            this.DbTypeAllowed = dc.dbTypeAllowed;
            if (dc.dbTypeAllowed)
                this.DbType = (int)dc.dbType;

            this.AllowDBNull = dc.AllowDBNull;
            this.ColumnName = dc.ColumnName;
            this.TableName = dc.Table?.TableName;
            this.IsReadOnly = dc.IsReadOnly;
            this.MaxLength = dc.MaxLength;
            this.IsAutoIncrement = dc.IsAutoIncrement;
            this.Precision = dc.Precision;
            this.PrecisionSpecified = dc.PrecisionSpecified;
            this.Scale = dc.Scale;
            this.ScaleSpecified = dc.ScaleSpecified;
            this.IsUnique = dc.IsUnique;
            this.IsUnsigned = dc.IsUnsigned;
            this.IsCompute = dc.IsCompute;
            this.IsUnicode = dc.IsUnicode;
            this.DataType = dc.DataType.GetAssemblyQualifiedName();
            this.Ordinal = dc.Ordinal;
            this.OriginalDbType = dc.OriginalDbType;
            this.OriginalTypeName = dc.OriginalTypeName;
        }

        /// <summary>
        /// Constructs a DmColumn object based on a DmColumnSurrogate object.
        /// </summary>
        public DmColumn ConvertToDmColumn()
        {
            DmColumn dmColumn = DmColumn.CreateColumn(this.ColumnName, DmUtils.GetTypeFromAssemblyQualifiedName(this.DataType));

            dmColumn.dbTypeAllowed = this.DbTypeAllowed;
            if (dmColumn.dbTypeAllowed)
                dmColumn.DbType = (DbType)this.DbType;

            dmColumn.AllowDBNull = this.AllowDBNull;
            dmColumn.IsReadOnly = this.IsReadOnly;
            dmColumn.MaxLength = this.MaxLength;
            dmColumn.IsAutoIncrement = this.IsAutoIncrement;
            dmColumn.Precision = this.Precision;
            dmColumn.PrecisionSpecified = this.PrecisionSpecified;
            dmColumn.Scale = this.Scale;
            dmColumn.ScaleSpecified = this.ScaleSpecified;
            dmColumn.IsUnique = this.IsUnique;
            dmColumn.IsUnicode = this.IsUnicode;
            dmColumn.IsCompute = this.IsCompute;
            dmColumn.IsUnsigned = this.IsUnsigned;
            dmColumn.OriginalDbType = this.OriginalDbType;
            dmColumn.OriginalTypeName = this.OriginalTypeName;
            dmColumn.SetOrdinal(this.Ordinal);

            return dmColumn;
        }
    }
}