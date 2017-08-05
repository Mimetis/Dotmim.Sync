using Dotmim.Sync.Core.Enumerations;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DmBinaryFormatter;
using Dotmim.Sync.Core.Filter;

namespace Dotmim.Sync.Core
{
    public sealed class ServiceConfiguration
    {

        /// <summary>
        /// Gets or Sets the default conflict resolution policy.
        /// </summary>
        public ConflictResolutionPolicy ConflictResolutionPolicy { get; set; } = ConflictResolutionPolicy.ServerWins;

        /// <summary>
        /// Tables involved. Once we have completed the ScopeSet property, this property become obsolete
        /// </summary>
        public string[] Tables { get; set; }

        /// <summary>
        /// Gets or Sets the DmSet Schema used for synchronization
        /// </summary>
        public DmSet ScopeSet { get; set; }

        /// <summary>
        /// Gets or Sets the directory used for batch mode
        /// </summary>
        public String BatchDirectory { get; set; }

        /// <summary>
        /// Gets or Sets the size used for downloading in batch mode
        /// </summary>
        public int DownloadBatchSizeInKB { get; set; }


        /// <summary>
        /// Gets/Sets the serialization converter object
        /// </summary>
        public SerializationFormat SerializationConverter { get; set; }

        /// <summary>
        /// Gets/Sets the log level for sync operations. Default value is None.
        /// </summary>
        public bool UseVerboseErrors { get; set; }

        /// <summary>
        /// Enable or disable the diagnostic page served by the $diag URL.
        /// </summary>
        public bool EnableDiagnosticPage { get; set; }

        /// <summary>
        /// Gets or Sets if we should use the bulk operations. Default is true.
        /// </summary>
        public bool UseBulkOperations { get; set; } = true;

        /// <summary>
        /// Get or Sets if the coded configuration should override the database configuration. Default false
        /// </summary>
        public bool OverwriteConfiguration { get; set; } = false;

        /// <summary>
        /// Filters applied on tables
        /// </summary>
        public FilterClauseCollection Filters { get; set; }

        /// <summary>
        /// Get the default apply action on conflict resolution
        /// </summary>
        public ApplyAction GetApplyAction() => this.ConflictResolutionPolicy == ConflictResolutionPolicy.ServerWins ?
                    ApplyAction.Continue :
                    ApplyAction.RetryWithForceWrite;

        public ServiceConfiguration()
        {
            this.ScopeSet = new DmSet("DotmimSync");
            this.EnableDiagnosticPage = false;
            this.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;
            this.DownloadBatchSizeInKB = 0;
            this.UseVerboseErrors = false;
            this.BatchDirectory = Path.Combine(Path.GetTempPath(), "DotmimSync");
            this.SerializationConverter = SerializationFormat.Json;
            this.Filters = new FilterClauseCollection(this);
        }

        public ServiceConfiguration(string[] tables) : this()
        {
            this.Tables = tables;
        }


        internal ServiceConfiguration Clone()
        {
            ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
            serviceConfiguration.BatchDirectory = this.BatchDirectory;
            serviceConfiguration.ConflictResolutionPolicy = this.ConflictResolutionPolicy;
            serviceConfiguration.DownloadBatchSizeInKB = this.DownloadBatchSizeInKB;
            serviceConfiguration.EnableDiagnosticPage = this.EnableDiagnosticPage;
            serviceConfiguration.ScopeSet = this.ScopeSet.Clone();
            serviceConfiguration.Tables = this.Tables;
            serviceConfiguration.UseBulkOperations = this.UseBulkOperations;
            serviceConfiguration.UseVerboseErrors = this.UseVerboseErrors;
            serviceConfiguration.SerializationConverter = this.SerializationConverter;

            if (this.Filters != null)
                foreach (var p in this.Filters)
                    serviceConfiguration.Filters.Add(p.TableName, p.ColumnName);

            return serviceConfiguration;

        }


        public static ServiceConfiguration DeserializeFromDmSet(DmSet set)
        {
            if (set == null)
                return null;

            if (!set.Tables.Contains("DotmimSync__ServiceConfiguration"))
                return null;

            ServiceConfiguration configuration = new ServiceConfiguration();
            var dmRowConfiguration = set.Tables["DotmimSync__ServiceConfiguration"].Rows[0];

            configuration.BatchDirectory = dmRowConfiguration["BatchDirectory"] as String; ;
            configuration.ConflictResolutionPolicy = (ConflictResolutionPolicy)dmRowConfiguration["ConflictResolutionPolicy"];
            configuration.DownloadBatchSizeInKB = (int)dmRowConfiguration["DownloadBatchSizeInKB"];
            configuration.EnableDiagnosticPage = (bool)dmRowConfiguration["EnableDiagnosticPage"];
            configuration.UseBulkOperations = (bool)dmRowConfiguration["UseBulkOperations"];
            configuration.UseVerboseErrors = (bool)dmRowConfiguration["UseVerboseErrors"];
            configuration.OverwriteConfiguration = (bool)dmRowConfiguration["OverwriteConfiguration"];
            configuration.SerializationConverter = (SerializationFormat)dmRowConfiguration["SerializationConverter"];

            if (set.Tables.Contains("DotmimSync__Filters"))
            {
                var dmTableFilterParameters = set.Tables["DotmimSync__Filters"];

                foreach (var dmRowFilter in dmTableFilterParameters.Rows)
                {
                    FilterClause filterClause = new FilterClause();

                    var tableName = dmRowFilter["TableName"] as String;
                    var columnName = dmRowFilter["ColumnName"] as String;

                    //var objType = DmUtils.GetTypeFromAssemblyQualifiedName(valueType);
                    //var converter = objType.GetConverter();
                    //var objValue = converter.ConvertFromInvariantString(valueType);
                    // syncParameter.Value = objValue;

                    configuration.Filters.Add(tableName, columnName);
                }
            }

            if (set.Tables.Contains("DotmimSync__Table"))
            {
                var dmTableTables = set.Tables["DotmimSync__Table"];
                configuration.Tables = new string[dmTableTables.Rows.Count];

                for (int i = 0; i < dmTableTables.Rows.Count; i++)
                {
                    var dmRowTable = dmTableTables.Rows[i];
                    var tableName = dmRowTable["Name"] as String;
                    configuration.Tables[i] = tableName;
                }
            }

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


        public static void SerializeInDmSet(DmSet set, ServiceConfiguration configuration)
        {
            if (set == null)
                return;

            DmTable dmTableConfiguration = null;
            DmTable dmTableFilterParameters = null;
            DmTable dmTableTables = null;

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
            dmTableConfiguration.Columns.Add<Boolean>("OverwriteConfiguration");
            dmTableConfiguration.Columns.Add<Int32>("SerializationConverter");

            var dmRowConfiguration = dmTableConfiguration.NewRow();
            dmRowConfiguration["BatchDirectory"] = configuration.BatchDirectory;
            dmRowConfiguration["ConflictResolutionPolicy"] = configuration.ConflictResolutionPolicy;
            dmRowConfiguration["DownloadBatchSizeInKB"] = configuration.DownloadBatchSizeInKB;
            dmRowConfiguration["EnableDiagnosticPage"] = configuration.EnableDiagnosticPage;
            dmRowConfiguration["UseBulkOperations"] = configuration.UseBulkOperations;
            dmRowConfiguration["UseVerboseErrors"] = configuration.UseVerboseErrors;
            dmRowConfiguration["OverwriteConfiguration"] = configuration.OverwriteConfiguration;
            dmRowConfiguration["SerializationConverter"] = configuration.SerializationConverter;
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

                DmSerializer serializer = new DmSerializer();

                foreach (var p in configuration.Filters)
                {
                    var dmRowFilter = dmTableFilterParameters.NewRow();
                    dmRowFilter["TableName"] = p.TableName;
                    dmRowFilter["ColumnName"] = p.ColumnName;

                    //var objType = p.Value.GetType();
                    //var converter = objType.GetConverter();
                    //dmRowFilter["Value"] = converter.ConvertToInvariantString(p.Value);
                    //dmRowFilter["ValueType"] = objType.GetAssemblyQualifiedName();

                    dmTableFilterParameters.Rows.Add(dmRowFilter);
                }
            }

            if (configuration.Tables != null && configuration.Tables.Length > 0)
            {
                if (!set.Tables.Contains("DotmimSync__Table"))
                {
                    dmTableTables = new DmTable("DotmimSync__Table");
                    set.Tables.Add(dmTableTables);
                }

                dmTableTables = set.Tables["DotmimSync__Table"];
                dmTableTables.Columns.Add<String>("Name");

                foreach (var p in configuration.Tables)
                {
                    var dmRowTable = dmTableTables.NewRow();
                    dmRowTable["Name"] = p;
                    dmTableTables.Rows.Add(dmRowTable);
                }
            }


        }
    }
}
