using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [DataContract(Name = "st"), Serializable]
    public class SetupTable : IEquatable<SetupTable>
    {
        /// <summary>
        /// public ctor for serialization purpose
        /// </summary>
        public SetupTable()
        {

        }

        /// <summary>
        /// Gets or Sets the table name
        /// </summary>
        [DataMember(Name = "tn", IsRequired = true, Order = 1)]
        public string TableName { get; set; }

        /// <summary>
        /// Gets or Sets the schema name
        /// </summary>
        [DataMember(Name = "sn", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string SchemaName { get; set; }

        /// <summary>
        /// Gets or Sets the table columns collection
        /// </summary>
        [DataMember(Name = "cols", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public SetupColumns Columns { get; set; }

        /// <summary>
        /// Gets or Sets the Sync direction (may be Bidirectional, DownloadOnly, UploadOnly) 
        /// Default is Bidirectional
        /// </summary>
        [DataMember(Name = "sd", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public SyncDirection SyncDirection { get; set; }



        /// <summary>
        /// Specify a table to add to the sync process
        /// If you don't specify any columns, all columns in the data source will be imported
        /// </summary>
        public SetupTable(string tableName, string schemaName = null)
        {
            this.TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));

            // Potentially user can pass something like [SalesLT].[Product]
            // or SalesLT.Product or Product. ParserName will handle it
            var parserTableName = ParserName.Parse(this.TableName);
            this.TableName = parserTableName.ObjectName;

            // Check Schema
            if (string.IsNullOrEmpty(schemaName))
            {
                schemaName = string.IsNullOrEmpty(parserTableName.SchemaName) ? null : parserTableName.SchemaName;
            }
            else
            {
                var parserSchemaName = ParserName.Parse(schemaName);
                schemaName = parserSchemaName.ObjectName;
            }

            this.SchemaName = schemaName;
            this.Columns = new SetupColumns();
        }

        /// <summary>
        /// Specify a table and its columns, to add to the sync process
        /// If you're specifying some columns, all others columns in the data source will be ignored
        /// </summary>
        public SetupTable(string tableName, IEnumerable<string> columnsName, string schemaName = null)
            : this(tableName, schemaName)
        {
            this.Columns.AddRange(columnsName);
        }

        public override string ToString()
        {
            if (string.IsNullOrEmpty(SchemaName))
                return TableName;
            else
                return $"{SchemaName}.{TableName}";
        }

        public override bool Equals(object obj) => this.Equals(obj as SetupTable);

        /// <summary>
        /// Compare 2 SetupTable instances. Assuming table name and schema name are equals, we have two conflicting setup tables
        /// </summary>
        public bool Equals(SetupTable other)
        {
            if (other == null)
                return false;

            var sc = SyncGlobalization.DataSourceStringComparison;

            var sn = this.SchemaName == null ? string.Empty : this.SchemaName;
            var otherSn = other.SchemaName == null ? string.Empty : other.SchemaName;

            // checking properties
            if (!string.Equals(this.TableName, other.TableName, sc) || !sn.Equals(otherSn, sc) || !(this.SyncDirection == other.SyncDirection))
                return false;

            // checking columns
            if ((this.Columns == null && other.Columns != null) || (this.Columns != null && other.Columns == null))
                return false;

            if (this.Columns != null && other.Columns != null)
            {
                if (this.Columns.Count != other.Columns.Count || !this.Columns.All(item1 => other.Columns.Any(item2 => string.Equals(item1, item2, sc))))
                    return false;
            }

            return true;
        }

        public override int GetHashCode()
        {
            var hashCode = -1006746859;
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(this.TableName);
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(this.SchemaName);
            hashCode = hashCode * -1521134295 + EqualityComparer<SetupColumns>.Default.GetHashCode(this.Columns);
            return hashCode;
        }

        public static bool operator ==(SetupTable left, SetupTable right)
        {
            return EqualityComparer<SetupTable>.Default.Equals(left, right);
        }

        public static bool operator !=(SetupTable left, SetupTable right)
        {
            return !(left == right);
        }
    }
}
