
using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Dotmim.Sync.Scope
{
    /// <summary>
    /// Represents a config from a table, stored in the config data xml field
    /// </summary>
    public class ScopeConfigDataAdapter
    {
        ObjectNameParser _trackingTableName;
        Collection<string> _filterColumns;
        Collection<ScopeConfigDataColumn> _columns;
        Collection<ScopeConfigDataParameter> _filterParameters;

        /// <summary>
        /// Gets or sets the name of the stored procedure that Sync Framework uses to delete data in bulk in a base table during synchronization.
        /// </summary>
        [XmlAttribute("BulkDelProc")]
        public string BulkDeleteProcName { get; set; }


        /// <summary>
        /// Gets or sets the name of the stored procedure that Sync Framework uses to add data in bulk in a base table during synchronization.
        /// </summary>
        [XmlAttribute("BulkInsProc")]
        public string BulkInsertProcName { get; set; }


        /// <summary>
        /// Gets or sets the table type name that Sync Framework uses for bulk procedures.
        /// </summary>
        [XmlAttribute("BulkTableType")]
        public string BulkTableTypeName { get; set; }


        /// <summary>
        /// Gets or sets the name of the stored procedure that Sync Framework uses to update data in bulk in a base table during synchronization.
        /// </summary>
        [XmlAttribute("BulkUpdProc")]
        public string BulkUpdateProcName { get; set; }


        /// <summary>
        /// Gets a collection of ScopeConfigDataColumn objects that represent all columns in a table.
        /// </summary>
        [XmlElement("Col")]
        public Collection<ScopeConfigDataColumn> Columns
        {
            get
            {
                return this._columns;
            }
        }

        /// <summary>
        /// Gets or sets the name of the stored procedure that Sync Framework uses to delete metadata from a tracking table.
        /// </summary>
        [XmlAttribute("DelMetaProc")]
        public string DeleteMetadataProcName { get; set; }

        /// <summary>
        /// Gets or sets the name of the stored procedure that Sync Framework uses to delete data from a base table during synchronization.
        /// </summary>
        [XmlAttribute("DelProc")]
        public string DeleteProcName { get; set; }

        /// <summary>
        /// Gets or sets the name of the delete trigger that Sync Framework creates on a base table to update metadata in a tracking table.
        /// </summary>
        [XmlAttribute("DelTrig")]
        public string DeleteTriggerName { get; set; }

        /// <summary>
        /// Specifies the SQL WHERE clause (without the WHERE keyword) that is used to filter the result set from the base table.
        /// </summary>
        [XmlElement("FilterClause")]
        public string FilterClause { get; set; }

        /// <summary>
        /// Gets the list of columns that are used for filtering.
        /// </summary>
        [XmlElement("FilterCol")]
        public Collection<string> FilterColumns => this._filterColumns;

        /// <summary>Gets a collection of parameters that are used for filtering and that are specified in <see cref="P:Microsoft.Synchronization.Data.SqlServer.SqlSyncProviderAdapterConfiguration.FilterClause" />.</summary>
        [XmlElement("FilterParam")]
        public Collection<ScopeConfigDataParameter> FilterParameters => this._filterParameters;

        /// <summary>Gets or sets the name, including database-specific delimiters that other nodes in a synchronization topology use to identify a table.</summary>
        [XmlAttribute("GlobalName")]
        public string GlobalTableName { get; set; }

        /// <summary>Gets or sets the name of the stored procedure that Sync Framework uses to insert metadata into a tracking table.</summary>
        [XmlAttribute("InsMetaProc")]
        public string InsertMetadataProcName { get; set; }

        /// <summary>Gets or sets the name of the stored procedure that Sync Framework uses to insert data into a base table during synchronization.</summary>
        [XmlAttribute("InsProc")]
        public string InsertProcName { get; set; }

        /// <summary>Gets or sets the name of the insert trigger that Sync Framework creates on a base table to update metadata in a tracking table.</summary>
        [XmlAttribute("InsTrig")]
        public string InsertTriggerName { get; set; }

        internal IEnumerable<ScopeConfigDataColumn> MutableColumns
        {
            get
            {
                foreach (ScopeConfigDataColumn column in this.Columns)
                {

                    if (string.Equals(column.Type, "timestamp", StringComparison.OrdinalIgnoreCase) || string.Equals(column.Type, "rowversion", StringComparison.OrdinalIgnoreCase))
                        continue;

                    yield return column;
                }
            }
        }

        /// <summary>
        /// Returns if Bulk Operations are allowed
        /// </summary>
        internal bool HasBulkOperationsEnabled =>   !string.IsNullOrEmpty(this.BulkInsertProcName)
                                                 && !string.IsNullOrEmpty(this.BulkUpdateProcName)
                                                 && !string.IsNullOrEmpty(this.BulkDeleteProcName);


        /// <summary>Gets or sets the name of the stored procedure that Sync Framework uses to select changes from a base table during synchronization.</summary>
        [XmlAttribute("SelChngProc")]
        public string SelectChangesProcName { get; set; }


        /// <summary>Gets or sets the name of the stored procedure that Sync Framework uses to select conflicting rows from a base table during synchronization.</summary>
        [XmlAttribute("SelRowProc")]
        public string SelectRowProcName { get; set; }


        /// <summary>Gets or sets the name, not including database-specific delimiters, that the local node in a synchronization topology uses to identify a table.</summary>
        [XmlAttribute("Name")]
        public string TableName { get; set; }


        /// <summary>Gets the name, not including database-specific delimiters, of the tracking table for a specific base table.</summary>
        [XmlAttribute("TrackingTable")]
        public string TrackingTableName
        {
            get
            {
                return this._trackingTableName.QuotedString;
            }
            set
            {
                this._trackingTableName.ParseString(value);
            }
        }


        /// <summary>Gets the name, not including database-specific delimiters, of the tracking table for a specific base table.</summary>
        public string UnquotedTrackingTableName
        {
            get
            {
                return this._trackingTableName.UnquotedString;
            }
        }

        /// <summary>Gets or sets the name of the stored procedure that Sync Framework uses to update metadata in a tracking table.</summary>
        [XmlAttribute("UpdMetaProc")]
        public string UpdateMetadataProcName { get; set; }


        /// <summary>Gets or sets the name of the stored procedure that Sync Framework uses to update data in a base table during synchronization.</summary>
        [XmlAttribute("UpdProc")]
        public string UpdateProcName { get; set; }


        /// <summary>Gets or sets the name of the update trigger that Sync Framework creates on a base table to update metadata in a tracking table.</summary>
        [XmlAttribute("UpdTrig")]
        public string UpdateTriggerName { get; set; }


        /// <summary>Initializes a new instance of the <see cref="T:Microsoft.Synchronization.Data.SqlServer.SqlSyncProviderAdapterConfiguration" /> class.</summary>
        public ScopeConfigDataAdapter()
        {
            this._columns = new Collection<ScopeConfigDataColumn>();
            this._filterColumns = new Collection<string>();
            this._trackingTableName = new ObjectNameParser();
            this._filterParameters = new Collection<ScopeConfigDataParameter>();
        }


    }
}
