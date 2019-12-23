//using Dotmim.Sync.Serialization;
//using System;
//using System.Collections.Generic;
//using System.Globalization;
//using System.Linq;
//using System.Text;
//using Dotmim.Sync.Enumerations;
//using System.Collections;
//using System.Runtime.Serialization;
//using System.Data;
//using System.Collections.ObjectModel;

//namespace Dotmim.Sync.Data.Surrogate
//{


//    [DataContract(Name = "dms")]
//    public class DmSetLightSchema : IDisposable
//    {

//        /// <summary>Gets or sets the name of the DmSet that the DmSetSurrogate object represents.</summary>
//        [DataMember(Name = "n")]
//        public string DmSetName { get; set; }

//        /// <summary>Gets or sets the locale information used to compare strings within the table.</summary>
//        [DataMember(Name = "ci")]
//        public string CultureInfoName { get; set; }

//        /// <summary>Gets or sets the Case sensitive rul of the DmSet that the DmSetSurrogate object represents.</summary>
//        [DataMember(Name = "cs")]
//        public bool CaseSensitive { get; set; }

//        /// <summary>
//        /// Gets or Sets an array of DmTableSurrogate objects that comprise 
//        /// the dm set that is represented by the DmSetSurrogate object.
//        /// </summary>
//        [DataMember(Name = "t")]
//        public Collection<DmTableLightSchema> Tables { get; set; }

//        /// <summary>
//        /// Gets or Sets an array of every DmRelationLightSchema belong to this DmSet
//        /// </summary>
//        [DataMember(Name = "r")]
//        public Collection<DmRelationLightSchema> Relations { get; set; }

//        /// <summary>
//        /// Initializes a new instance of the DmSetSurrogate class from an existing DmSet
//        /// </summary>
//        public DmSetLightSchema(DmSet ds)
//        {
//            if (ds == null)
//                throw new ArgumentNullException("ds");


//            this.DmSetName = ds.DmSetName;
//            this.CultureInfoName = ds.Culture.Name;
//            this.CaseSensitive = ds.CaseSensitive;

//            this.Tables = new Collection<DmTableLightSchema>();

//            for (int i = 0; i < ds.Tables.Count; i++)
//                this.Tables.Add(new DmTableLightSchema(ds.Tables[i]));

//            if (ds.Relations != null && ds.Relations.Count > 0)
//            {
//                this.Relations = new Collection<DmRelationLightSchema>();

//                for (int i = 0; i < ds.Relations.Count; i++)
//                {
//                    var dr = ds.Relations[i];
                    
//                    var drs = new DmRelationLightSchema
//                    {
//                        ChildKeySurrogates = new DmColumnLightSchema[dr.ChildKey.Columns.Length]
//                    };

//                    for (int keyIndex = 0; keyIndex < dr.ChildKey.Columns.Length; keyIndex++)
//                        drs.ChildKeySurrogates[keyIndex] = new DmColumnLightSchema(dr.ChildKey.Columns[keyIndex]);

//                    drs.ParentKeySurrogates = new DmColumnLightSchema[dr.ParentKey.Columns.Length];
//                    for (int keyIndex = 0; keyIndex < dr.ParentKey.Columns.Length; keyIndex++)
//                        drs.ParentKeySurrogates[keyIndex] = new DmColumnLightSchema(dr.ParentKey.Columns[keyIndex]);

//                    drs.RelationName = dr.RelationName;

//                    this.Relations.Add(drs);
//                }
//            }
//        }

//        /// <summary>
//        /// Only used for Serialization
//        /// </summary>
//        public DmSetLightSchema()
//        {
//        }

//        /// <summary>
//        /// Constructs a DmSet object based on a DmSetSurrogate object.
//        /// </summary>
//        public DmSet ConvertToDmSet()
//        {
//            var dmSet = new DmSet()
//            {
//                Culture = new CultureInfo(this.CultureInfoName),
//                CaseSensitive = this.CaseSensitive,
//                DmSetName = this.DmSetName
//            };
//            this.ReadSchemaIntoDmSet(dmSet);
//            return dmSet;
//        }

//        /// <summary>
//        /// Read schema in an existing DmSet
//        /// </summary>
//        /// <param name="ds"></param>
//        public void ReadSchemaIntoDmSet(DmSet ds)
//        {
//            var dmTableSurrogateArray = this.Tables;
//            for (int i = 0; i < dmTableSurrogateArray.Count; i++)
//            {
//                var dmTableSurrogate = dmTableSurrogateArray[i];
//                var dmTable = new DmTable();
//                dmTableSurrogate.ReadSchemaIntoDmTable(dmTable);

//                dmTable.Culture = new CultureInfo(dmTableSurrogate.CultureInfoName);
//                dmTable.CaseSensitive = dmTableSurrogate.CaseSensitive;
//                dmTable.TableName = dmTableSurrogate.TableName;

//                ds.Tables.Add(dmTable);
//            }

//            if (this.Relations != null && this.Relations.Count > 0)
//            {
//                foreach (var dmRelationSurrogate in this.Relations)
//                {
//                    DmColumn[] parentColumns = new DmColumn[dmRelationSurrogate.ParentKeySurrogates.Length];
//                    DmColumn[] childColumns = new DmColumn[dmRelationSurrogate.ChildKeySurrogates.Length];

//                    for (int i = 0; i < parentColumns.Length; i++)
//                    {
//                        var columnName = dmRelationSurrogate.ParentKeySurrogates[i].ColumnName;
//                        var tableName = dmRelationSurrogate.ParentKeySurrogates[i].TableName;
//                        var schemaName = dmRelationSurrogate.ParentKeySurrogates[i].SchemaName;

//                        parentColumns[i] = ds.Tables[tableName, schemaName].Columns[columnName];

//                        columnName = dmRelationSurrogate.ChildKeySurrogates[i].ColumnName;
//                        tableName = dmRelationSurrogate.ChildKeySurrogates[i].TableName;
//                        schemaName = dmRelationSurrogate.ChildKeySurrogates[i].SchemaName;

//                        childColumns[i] = ds.Tables[tableName, schemaName].Columns[columnName];

//                    }

//                    var relation = new DmRelation(dmRelationSurrogate.RelationName, parentColumns, childColumns);
//                    ds.Relations.Add(relation);
//                }
//            }
//        }

//        public void Clear()
//        {
//            if (this.Relations != null)
//            {
//                this.Relations.Clear();
//                this.Relations = null;
//            }

//            if (this.Tables != null)
//            {
//                foreach (var tbl in this.Tables)
//                    tbl.Clear();

//                this.Tables.Clear();
//                this.Tables = null;
//            }
//        }

//        public void Dispose()
//        {
//            this.Dispose(true);
//            GC.SuppressFinalize(this);
//        }

//        protected virtual void Dispose(bool cleanup)
//        {
//            this.Clear();
//        }
//    }

//    /// <summary>
//    /// Represents a surrogate of a DmTable object, which DotMim Sync uses during custom binary serialization.
//    /// </summary>
//    [DataContract(Name = "dmt")]
//    public class DmTableLightSchema : IDisposable
//    {
//        /// <summary>
//        /// Gets or sets the locale information used to compare strings within the table.
//        /// </summary>
//        [DataMember(Name = "ci")]
//        public string CultureInfoName { get; set; }

//        /// <summary>Gets or sets the Case sensitive rul of the DmTable that the DmTableSurrogate object represents.</summary>
//        [DataMember(Name = "cs")]
//        public bool CaseSensitive { get; set; }

//        /// <summary>
//        /// Get or Set the schema used for the DmTableSurrogate
//        /// </summary>
//        [DataMember(Name = "s")]
//        public string Schema { get; set; }

//        /// <summary>
//        /// Gets or sets the name of the table that the DmTableSurrogate object represents.
//        /// </summary>
//        [DataMember(Name = "n")]
//        public string TableName { get; set; }

//        /// <summary>
//        /// Gets an array of DmColumnSurrogate objects that comprise the table that is represented by the DmTableSurrogate object.
//        /// </summary>
//        [DataMember(Name = "c")]
//        public List<DmColumnLightSchema> Columns { get; set; } = new List<DmColumnLightSchema>();

//        /// <summary>
//        /// Gets an array of DmColumnSurrogate objects that represent the PrimaryKeys.
//        /// </summary>
//        [DataMember(Name = "pk")]
//        public List<string> PrimaryKeys { get; set; } = new List<string>();

//        /// <summary>
//        /// Gets or Sets the original provider (SqlServer, MySql, Sqlite, Oracle, PostgreSQL)
//        /// </summary>
//        [DataMember(Name = "op")]
//        public string OriginalProvider { get; set; }

//        /// <summary>
//        /// Gets or Sets the Sync direction (may be Bidirectional, DownloadOnly, UploadOnly) 
//        /// Default is Bidirectional
//        /// </summary>
//        [DataMember(Name = "sd")]
//        public SyncDirection SyncDirection { get; set; }

  

//        /// <summary>
//        /// Only used for Serialization
//        /// </summary>
//        public DmTableLightSchema()
//        {

//        }

//        /// <summary>
//        /// Initializes a new instance of the DmTableSurrogate class.
//        /// </summary>
//        public DmTableLightSchema(DmTable dt)
//        {
//            if (dt == null)
//                throw new ArgumentNullException("dt", "DmTable");

//            this.TableName = dt.TableName;
//            this.CultureInfoName = dt.Culture.Name;
//            this.CaseSensitive = dt.CaseSensitive;
//            this.Schema = dt.Schema;
//            this.OriginalProvider = dt.OriginalProvider;
//            this.SyncDirection = dt.SyncDirection;

//            for (int i = 0; i < dt.Columns.Count; i++)
//                this.Columns.Add(new DmColumnLightSchema(dt.Columns[i]));

//            // Primary Keys
//            if (dt.PrimaryKey != null && dt.PrimaryKey.Columns != null && dt.PrimaryKey.Columns.Length > 0)
//            {
//                for (int i = 0; i < dt.PrimaryKey.Columns.Length; i++)
//                    this.PrimaryKeys.Add(dt.PrimaryKey.Columns[i].ColumnName);
//            }

//        }

//        /// <summary>
//        /// Copies the table schema from a DmTableSurrogate object into a DmTable object.
//        /// </summary>
//        public void ReadSchemaIntoDmTable(DmTable dt)
//        {
//            if (dt == null)
//                throw new ArgumentNullException("dt", "DmTable");

//            dt.TableName = this.TableName;
//            dt.Culture = new CultureInfo(this.CultureInfoName);
//            dt.Schema = this.Schema;
//            dt.CaseSensitive = this.CaseSensitive;
//            dt.OriginalProvider = this.OriginalProvider;
//            dt.SyncDirection = this.SyncDirection;

//            for (int i = 0; i < this.Columns.Count; i++)
//            {
//                var dmColumn = this.Columns[i].ConvertToDmColumn();
//                dt.Columns.Add(dmColumn);
//            }

//            if (this.PrimaryKeys != null && this.PrimaryKeys.Count > 0)
//            {
//                DmColumn[] keyColumns = new DmColumn[this.PrimaryKeys.Count];

//                for (int i = 0; i < this.PrimaryKeys.Count; i++)
//                {
//                    string columnName = this.PrimaryKeys[i];
//                    keyColumns[i] = dt.Columns.First(c => dt.IsEqual(c.ColumnName, columnName));
//                }
//                var key = new DmKey(keyColumns);

//                dt.PrimaryKey = key;
//            }
//        }

//        /// <summary>
//        /// Convert this surrogate to a DmTable
//        /// </summary>
//        /// <returns></returns>
//        public DmTable ConvertToDmTable()
//        {
//            var dmTable = new DmTable
//            {
//                Culture = new CultureInfo(this.CultureInfoName),
//                CaseSensitive = this.CaseSensitive
//            };
//            this.TableName = this.TableName;

//            this.ReadSchemaIntoDmTable(dmTable);

//            return dmTable;
//        }
//        public void Dispose()
//        {
//            this.Dispose(true);
//            GC.SuppressFinalize(this);
//        }

//        protected virtual void Dispose(bool cleanup) => this.Clear();

//        public void Clear()
//        {
//            if (this.PrimaryKeys != null)
//            {
//                this.PrimaryKeys.Clear();
//                this.PrimaryKeys = null;
//            }

//            if (this.Columns != null)
//            {
//                this.Columns.Clear();
//                this.Columns = null;
//            }
//        }




//    }


//    [DataContract(Name = "dmc")]
//    public class DmColumnLightSchema
//    {
//        /// <summary>Gets or sets the name of the column that the DmColumnSurrogate object represents.</summary>
//        [DataMember(Name = "n")]
//        public string ColumnName { get; set; }

//        [DataMember(Name = "t")]
//        public string TableName { get; set; }

//        [DataMember(Name = "s")]
//        public string SchemaName { get; set; }

//        [DataMember(Name = "an")]
//        public bool AllowDBNull { get; set; } = true;

//        [DataMember(Name = "iu")]
//        public bool IsUnique { get; set; } = false;

//        [DataMember(Name = "ir")]
//        public bool IsReadOnly { get; set; } = false;

//        [DataMember(Name = "ia")]
//        public Boolean IsAutoIncrement { get; set; }

//        [DataMember(Name = "ius")]
//        public bool IsUnsigned { get; set; }

//        [DataMember(Name = "iuc")]
//        public bool IsUnicode { get; set; }

//        [DataMember(Name = "ico")]
//        public bool IsCompute { get; set; }

//        [DataMember(Name = "ml")]
//        public Int32 MaxLength { get; set; }

//        [DataMember(Name = "o")]
//        public int Ordinal { get; set; }

//        [DataMember(Name = "ps")]
//        public bool PrecisionSpecified { get; set; }

//        [DataMember(Name = "p")]
//        public Byte Precision { get; set; }

//        [DataMember(Name = "ss")]
//        public bool ScaleSpecified { get; set; }

//        [DataMember(Name = "sc")]
//        public byte Scale { get; set; }

//        [DataMember(Name = "db")]
//        public int DbType { get; set; }

//        [DataMember(Name = "odb")]
//        public string OriginalDbType { get; set; }

//        [DataMember(Name = "oty")]
//        public string OriginalTypeName { get; set; }

//        [DataMember(Name = "dt")]
//        public string DataType { get; set; }

//        [DataMember(Name = "dta")]
//        public bool DbTypeAllowed { get; set; }

//        /// <summary>
//        /// Only used for Serialization
//        /// </summary>
//        public DmColumnLightSchema()
//        {



//        }

//        /// <summary>
//        /// Initializes a new instance of the DmColumnSurrogate class.
//        /// </summary>
//        public DmColumnLightSchema(DmColumn dc)
//        {
//            if (dc == null)
//                throw new ArgumentNullException("dc", "DmColumn");


//            this.DbTypeAllowed = dc.dbTypeAllowed;
//            if (dc.dbTypeAllowed)
//                this.DbType = (int)dc.dbType;

//            this.AllowDBNull = dc.AllowDBNull;
//            this.ColumnName = dc.ColumnName;
//            this.TableName = dc.Table?.TableName;
//            this.SchemaName = dc.Table?.Schema;
//            this.IsReadOnly = dc.IsReadOnly;
//            this.MaxLength = dc.MaxLength;
//            this.IsAutoIncrement = dc.IsAutoIncrement;
//            this.Precision = dc.Precision;
//            this.PrecisionSpecified = dc.PrecisionSpecified;
//            this.Scale = dc.Scale;
//            this.ScaleSpecified = dc.ScaleSpecified;
//            this.IsUnique = dc.IsUnique;
//            this.IsUnsigned = dc.IsUnsigned;
//            this.IsCompute = dc.IsCompute;
//            this.IsUnicode = dc.IsUnicode;
//            this.DataType = dc.DataType.GetAssemblyQualifiedName();
//            this.Ordinal = dc.Ordinal;
//            this.OriginalDbType = dc.OriginalDbType;
//            this.OriginalTypeName = dc.OriginalTypeName;
//        }

//        /// <summary>
//        /// Constructs a DmColumn object based on a DmColumnSurrogate object.
//        /// </summary>
//        public DmColumn ConvertToDmColumn()
//        {
//            var dmColumn = DmColumn.CreateColumn(this.ColumnName, DmUtils.GetTypeFromAssemblyQualifiedName(this.DataType));

//            dmColumn.dbTypeAllowed = this.DbTypeAllowed;
//            if (dmColumn.dbTypeAllowed)
//                dmColumn.DbType = (DbType)this.DbType;

//            dmColumn.AllowDBNull = this.AllowDBNull;
//            dmColumn.IsReadOnly = this.IsReadOnly;
//            dmColumn.MaxLength = this.MaxLength;
//            dmColumn.IsAutoIncrement = this.IsAutoIncrement;
//            dmColumn.Precision = this.Precision;
//            dmColumn.PrecisionSpecified = this.PrecisionSpecified;
//            dmColumn.Scale = this.Scale;
//            dmColumn.ScaleSpecified = this.ScaleSpecified;
//            dmColumn.IsUnique = this.IsUnique;
//            dmColumn.IsUnicode = this.IsUnicode;
//            dmColumn.IsCompute = this.IsCompute;
//            dmColumn.IsUnsigned = this.IsUnsigned;
//            dmColumn.OriginalDbType = this.OriginalDbType;
//            dmColumn.OriginalTypeName = this.OriginalTypeName;
//            dmColumn.SetOrdinal(this.Ordinal);

//            return dmColumn;
//        }
//    }


//    [DataContract(Name = "dmr")]
//    public class DmRelationLightSchema
//    {
//        /// <summary>
//        /// Gets or Sets an array of DmColumnSurrogate objects that represent the parent key.
//        /// </summary>
//        [DataMember(Name = "pks")]
//        public DmColumnLightSchema[] ParentKeySurrogates { get; set; }

//        /// <summary>
//        /// Gets ro Sets an array of DmColumnSurrogate objects that represent the parent key.
//        /// </summary>
//        [DataMember(Name = "cks")]
//        public DmColumnLightSchema[] ChildKeySurrogates { get; set; }

//        /// <summary>
//        /// Gets or Sets the relation name 
//        /// </summary>
//        [DataMember(Name = "n")]
//        public string RelationName { get; set; }

//    }
//}