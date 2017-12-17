using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Data;
using System;
using System.IO;
using System.Linq;
using Dotmim.Sync.Filter;
using Dotmim.Sync.Serialization;
using System.Collections;
using System.Collections.Generic;
using Dotmim.Sync.Builders;

namespace Dotmim.Sync
{
    [Serializable]
    public class SyncConfiguration : ICollection<DmTable>
    {

        /// <summary>
        /// Gets or Sets the default conflict resolution policy.
        /// </summary>
        public ConflictResolutionPolicy ConflictResolutionPolicy { get; set; } = ConflictResolutionPolicy.ServerWins;

        /// <summary>
        /// Tables involved. Once we have completed the ScopeSet property, this property become obsolete
        /// </summary>
        //public string[] Tables { get; set; }

        /// <summary>
        /// Gets or Sets the DmSet Schema used for synchronization
        /// </summary>


        [NonSerialized]
        private DmSet scopeSet;

        public DmSet ScopeSet
        {
            get
            {
                return scopeSet;
            }
            set
            {
                scopeSet = value;
            }
        }

        /// <summary>
        /// Gets or Sets the directory used for batch mode.
        /// Default value is [Windows Temp Path]/[DotmimSync]
        /// </summary>
        public String BatchDirectory { get; set; }

        /// <summary>
        /// Gets or Sets the size used for downloading in batch mode. 
        /// Default is 0 (no batch mode)
        /// </summary>
        public int DownloadBatchSizeInKB { get; set; }

        /// <summary>
        /// Gets/Sets the serialization converter object. Default is Json
        /// </summary>
        public SerializationFormat SerializationFormat { get; set; }

        /// <summary>
        /// Gets/Sets the log level for sync operations. Default value is false.
        /// </summary>
        public bool UseVerboseErrors { get; set; }

        /// <summary>
        /// Gets or Sets if we should use the bulk operations. Default is true.
        /// If provider doe not support bulk operations, this option is overrided to false.
        /// </summary>
        public bool UseBulkOperations { get; set; } = true;


        /// <summary>
        /// Filters applied on tables
        /// </summary>
        public FilterClauseCollection Filters { get; set; }


        /// <summary>
        /// Get the default apply action on conflict resolution.
        /// Default is ServerWins
        /// </summary>
        public ApplyAction GetApplyAction() => this.ConflictResolutionPolicy == ConflictResolutionPolicy.ServerWins ?
                    ApplyAction.Continue :
                    ApplyAction.RetryWithForceWrite;

        /// <summary>
        /// Gets if the config object has tables defined
        /// </summary>
        public bool HasTables => this.ScopeSet?.Tables.Count > 0;

        /// <summary>
        /// Gets if the config tables has columns defined
        /// </summary>
        public bool HasColumns =>
                // using SelectMany to get DmColumns and not DmColumnCollection
                this.ScopeSet?.Tables?.SelectMany(t => t.Columns).Count() > 0;


        public SyncConfiguration()
        {
            this.ScopeSet = new DmSet("DotmimSync");
            this.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;
            this.DownloadBatchSizeInKB = 0;
            this.UseVerboseErrors = false;
            this.BatchDirectory = Path.Combine(Path.GetTempPath(), "DotmimSync");
            this.SerializationFormat = SerializationFormat.Json;
            this.Filters = new FilterClauseCollection(this);
        }

        public SyncConfiguration(string[] tables) : this()
        {
            if (tables.Length <= 0)
                return;

            foreach (var table in tables)
                this.Add(table);
        }

        public SyncConfiguration Clone()
        {
            SyncConfiguration syncConfiguration = new SyncConfiguration();
            syncConfiguration.BatchDirectory = this.BatchDirectory;
            syncConfiguration.ConflictResolutionPolicy = this.ConflictResolutionPolicy;
            syncConfiguration.DownloadBatchSizeInKB = this.DownloadBatchSizeInKB;
            syncConfiguration.ScopeSet = this.ScopeSet.Clone();
            syncConfiguration.UseBulkOperations = this.UseBulkOperations;
            syncConfiguration.UseVerboseErrors = this.UseVerboseErrors;
            syncConfiguration.SerializationFormat = this.SerializationFormat;
            //syncConfiguration.OverwriteConfiguration = this.OverwriteConfiguration;

            if (this.Filters != null)
                foreach (var p in this.Filters)
                    syncConfiguration.Filters.Add(p.TableName, p.ColumnName);

            return syncConfiguration;
        }

        internal static SyncConfiguration DeserializeFromDmSet(DmSet set)
        {
            if (set == null)
                return null;

            if (!set.Tables.Contains("DotmimSync__ServiceConfiguration"))
                return null;

            SyncConfiguration configuration = new SyncConfiguration();
            var dmRowConfiguration = set.Tables["DotmimSync__ServiceConfiguration"].Rows[0];

            configuration.BatchDirectory = dmRowConfiguration["BatchDirectory"] as String; ;
            configuration.ConflictResolutionPolicy = (ConflictResolutionPolicy)dmRowConfiguration["ConflictResolutionPolicy"];
            configuration.DownloadBatchSizeInKB = (int)dmRowConfiguration["DownloadBatchSizeInKB"];
            configuration.UseBulkOperations = (bool)dmRowConfiguration["UseBulkOperations"];
            configuration.UseVerboseErrors = (bool)dmRowConfiguration["UseVerboseErrors"];
            //configuration.OverwriteConfiguration = (bool)dmRowConfiguration["OverwriteConfiguration"];
            configuration.SerializationFormat = (SerializationFormat)dmRowConfiguration["SerializationConverter"];

            if (set.Tables.Contains("DotmimSync__Filters"))
            {
                var dmTableFilterParameters = set.Tables["DotmimSync__Filters"];

                foreach (var dmRowFilter in dmTableFilterParameters.Rows)
                {
                    FilterClause filterClause = new FilterClause();

                    var tableName = dmRowFilter["TableName"] as String;
                    var columnName = dmRowFilter["ColumnName"] as String;
                    configuration.Filters.Add(tableName, columnName);
                }
            }

            //if (set.Tables.Contains("DotmimSync__Table"))
            //{
            //    var dmTableTables = set.Tables["DotmimSync__Table"];
            //    configuration.Tables = new string[dmTableTables.Rows.Count];

            //    for (int i = 0; i < dmTableTables.Rows.Count; i++)
            //    {
            //        var dmRowTable = dmTableTables.Rows[i];
            //        var tableName = dmRowTable["Name"] as String;
            //        configuration.Tables[i] = tableName;
            //    }
            //}

            var configTables = set.Tables.Where(tbl => !tbl.TableName.StartsWith("DotmimSync__"));

            if (configTables != null)
                foreach (var configTable in configTables)
                    configuration.ScopeSet.Tables.Add(configTable.Clone());

            if (set.Relations != null && set.Relations.Count > 0)
            {
                foreach (var r in set.Relations)
                {
                    var relation = r.Clone(configuration.ScopeSet);
                    configuration.ScopeSet.Relations.Add(relation);
                }
            }


            return configuration;
        }

        internal static void SerializeInDmSet(DmSet set, SyncConfiguration configuration)
        {
            if (set == null)
                return;

            DmTable dmTableConfiguration = null;
            DmTable dmTableFilterParameters = null;

            if (!set.Tables.Contains("DotmimSync__ServiceConfiguration"))
            {
                dmTableConfiguration = new DmTable("DotmimSync__ServiceConfiguration");
                set.Tables.Add(dmTableConfiguration);
            }

            dmTableConfiguration = set.Tables["DotmimSync__ServiceConfiguration"];

            dmTableConfiguration.Clear();
            dmTableConfiguration.Columns.Clear();

            dmTableConfiguration.Columns.Add<String>("BatchDirectory");
            dmTableConfiguration.Columns.Add<Int32>("ConflictResolutionPolicy");
            dmTableConfiguration.Columns.Add<Int32>("DownloadBatchSizeInKB");
            dmTableConfiguration.Columns.Add<Boolean>("EnableDiagnosticPage");
            dmTableConfiguration.Columns.Add<Boolean>("UseBulkOperations");
            dmTableConfiguration.Columns.Add<Boolean>("UseVerboseErrors");
            //dmTableConfiguration.Columns.Add<Boolean>("OverwriteConfiguration");
            dmTableConfiguration.Columns.Add<Int32>("SerializationConverter");

            var dmRowConfiguration = dmTableConfiguration.NewRow();
            dmRowConfiguration["BatchDirectory"] = configuration.BatchDirectory;
            dmRowConfiguration["ConflictResolutionPolicy"] = configuration.ConflictResolutionPolicy;
            dmRowConfiguration["DownloadBatchSizeInKB"] = configuration.DownloadBatchSizeInKB;
            dmRowConfiguration["UseBulkOperations"] = configuration.UseBulkOperations;
            dmRowConfiguration["UseVerboseErrors"] = configuration.UseVerboseErrors;
            //dmRowConfiguration["OverwriteConfiguration"] = configuration.OverwriteConfiguration;
            dmRowConfiguration["SerializationConverter"] = configuration.SerializationFormat;
            dmTableConfiguration.Rows.Add(dmRowConfiguration);

            if (configuration.ScopeSet != null && configuration.ScopeSet.Tables.Count > 0)
            {
                foreach (var dmTable in configuration.ScopeSet.Tables)
                {
                    var dmTableConf = dmTable.Clone();
                    set.Tables.Add(dmTableConf);
                }

                foreach (var dmRelation in configuration.ScopeSet.Relations)
                {
                    var dmRelationConf = dmRelation.Clone(set);
                    set.Relations.Add(dmRelationConf);
                }
            }

            if (configuration.Filters.Count > 0)
            {

                if (!set.Tables.Contains("DotmimSync__Filters"))
                {
                    dmTableFilterParameters = new DmTable("DotmimSync__Filters");
                    set.Tables.Add(dmTableFilterParameters);
                }

                dmTableFilterParameters = set.Tables["DotmimSync__Filters"];
                dmTableFilterParameters.Columns.Add<String>("TableName");
                dmTableFilterParameters.Columns.Add<String>("ColumnName");

                foreach (var p in configuration.Filters)
                {
                    var dmRowFilter = dmTableFilterParameters.NewRow();
                    dmRowFilter["TableName"] = p.TableName;
                    dmRowFilter["ColumnName"] = p.ColumnName;

                    dmTableFilterParameters.Rows.Add(dmRowFilter);
                }
            }

            //if (configuration.Tables != null && configuration.Tables.Length > 0)
            //{
            //    if (!set.Tables.Contains("DotmimSync__Table"))
            //    {
            //        dmTableTables = new DmTable("DotmimSync__Table");
            //        set.Tables.Add(dmTableTables);
            //    }

            //    dmTableTables = set.Tables["DotmimSync__Table"];
            //    dmTableTables.Columns.Add<String>("Name");

            //    foreach (var p in configuration.Tables)
            //    {
            //        var dmRowTable = dmTableTables.NewRow();
            //        dmRowTable["Name"] = p;
            //        dmTableTables.Rows.Add(dmRowTable);
            //    }
            //}


        }

        public int Count
        {
            get
            {
                if (this.ScopeSet == null)
                    return 0;
                if (this.ScopeSet.Tables == null)
                    return 0;
                return this.ScopeSet.Tables.Count;
            }
        }

        public bool IsReadOnly => false;

        /// <summary>
        /// Adding tables to configuration
        /// </summary>
        public void Add(string[] tables)
        {
            foreach (var table in tables)
                Add(table);
        }

        /// <summary>
        /// Adding tables to configuration
        /// </summary>
        public void Add(string table)
        {
            if (this.ScopeSet == null || this.ScopeSet.Tables == null)
                throw new SyncException($"Can't add new table {table} in Configuration, ScopeSet is null", SyncStage.ConfigurationApplied, SyncExceptionType.Argument);

            // Potentially user can pass something like [SalesLT].[Product]
            // or SalesLT.Product or Product. ObjectNameParser will handle it
            ObjectNameParser parser = new ObjectNameParser(table);

            var tableName = parser.ObjectName;
            var schema = parser.SchemaName;

            if (!this.ScopeSet.Tables.Contains(tableName))
            {
                var dmTable = new DmTable(tableName);
                if (!String.IsNullOrEmpty(schema))
                    dmTable.Schema = schema;

                this.ScopeSet.Tables.Add(dmTable);
            }
        }

        /// <summary>
        /// Adding table to configuration
        /// </summary>
        public void Add(DmTable item)
        {
            if (this.ScopeSet == null || this.ScopeSet.Tables == null)
                throw new SyncException("Can't add a dmTable in Configuration, ScopeSet is null", SyncStage.ConfigurationApplied, SyncExceptionType.Argument);

            this.ScopeSet.Tables.Add(item);
        }

        public void Clear()
        {
            if (this.ScopeSet == null || this.ScopeSet.Tables == null)
                return;

            this.ScopeSet.Tables.Clear();
        }

        public DmTable this[int index]
        {
            get
            {
                if (this.ScopeSet == null || this.ScopeSet.Tables == null)
                    return null;

                return this.ScopeSet.Tables[index];
            }
        }
        public DmTable this[string name]
        {
            get
            {
                if (string.IsNullOrEmpty(name))
                    throw new ArgumentNullException("name");

                if (this.ScopeSet == null || this.ScopeSet.Tables == null)
                    return null;

                return this.ScopeSet.Tables[name];
            }
        }
        public bool Contains(DmTable item)
        {
            if (this.ScopeSet == null || this.ScopeSet.Tables == null)
                return false;

            return this.ScopeSet.Tables.Contains(item);
        }
        public void CopyTo(DmTable[] array, int arrayIndex)
        {
            if (this.ScopeSet == null || this.ScopeSet.Tables == null)
                return;

            for (int i = 0; i < this.ScopeSet.Tables.Count; ++i)
                array[arrayIndex + i] = this.ScopeSet.Tables[i];
        }
        public bool Remove(DmTable item)
        {
            if (this.ScopeSet == null || this.ScopeSet.Tables == null)
                return false;

            return this.ScopeSet.Tables.Remove(item);
        }
        public IEnumerator<DmTable> GetEnumerator()
        {
            if (this.ScopeSet == null || this.ScopeSet.Tables == null)
                return null;

            return this.ScopeSet.Tables.GetEnumerator();
        }
        IEnumerator IEnumerable.GetEnumerator()
        {
            if (this.ScopeSet == null || this.ScopeSet.Tables == null)
                yield break;

            yield return this.ScopeSet.Tables.GetEnumerator();
        }
    }
}
