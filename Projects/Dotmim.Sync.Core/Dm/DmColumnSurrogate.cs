using Dotmim.Sync.Data;
using System;

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
        public Type DmType { get; set; }

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

            this.ColumnName = dc.ColumnName;
            this.DmType = dc.DataType;
        }

        /// <summary>
        /// Constructs a DmColumn object based on a DmColumnSurrogate object.
        /// </summary>
        public DmColumn ConvertToDmColumn()
        {

            DmColumn dmColumn = DmColumn.CreateColumn(this.ColumnName, this.DmType);
            return dmColumn;
        }
    }
}