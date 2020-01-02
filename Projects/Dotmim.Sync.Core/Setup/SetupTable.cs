using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    public class SetupTable : IEquatable<SetupTable>
    {

        /// <summary>
        /// Gets or Sets the table name
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// Gets or Sets the schema name
        /// </summary>
        public string SchemaName { get; set; }

        /// <summary>
        /// Gets or Sets the table columns collection
        /// </summary>
        public SetupColumns Columns { get; set; }

        /// <summary>
        /// Specify a table to add to the sync process
        /// If you don't specify any columns, all columns in the data source will be imported
        /// </summary>
        public SetupTable(string tableName, string schemaName = null)
        {
            // Potentially user can pass something like [SalesLT].[Product]
            // or SalesLT.Product or Product. ParserName will handle it
            var parserTableName = ParserName.Parse(tableName);
            tableName = parserTableName.ObjectName;

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

            this.TableName = tableName;
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

        public SetupTable()
        {

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

            return other != null &&
                   this.TableName.Equals(other.TableName, sc) &&
                   sn.Equals(otherSn, sc);
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
