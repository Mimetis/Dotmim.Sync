using Dotmim.Sync.Core.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Dotmim.Sync.Core.Scope
{
    /// <summary>
    /// Represents the schema of a column from the config data xml file, stored in DB 
    /// </summary>
    public class ScopeConfigDataColumn
    {
        string _quotedName;
        string _unquotedName;
        string _parameterName;
        bool _isPrimaryKey;
        string _type;
        bool _sizeSpecified;
        string _size;
        bool _precisionSpecified;
        int _precision;
        bool _scaleSpecified;
        int _scale;
        bool _defaultValueSpecified;
        string _defaultValue;
        bool _autoIncrementSeedSpecified;
        long _autoIncrementSeed;
        bool _autoIncrementStepSpecified;
        long _autoIncrementStep;
        bool _isNullable;

        /// <summary>Gets or sets the starting value for a column that automatically increments its value when new rows are inserted into the table.</summary>
        /// <returns>The starting value for an autoincrement column.</returns>
        [XmlAttribute("idSeed")]
        public long AutoIncrementSeed
        {
            get
            {
                return this._autoIncrementSeed;
            }
            set
            {
                this._autoIncrementSeedSpecified = true;
                this._autoIncrementSeed = value;
            }
        }

        /// <summary>Gets or sets whether a value is specified for the <see cref="P:Microsoft.Synchronization.Data.DbSyncColumnDescription.AutoIncrementSeed" /> property.</summary>
        /// <returns>true if a value is specified for <see cref="P:Microsoft.Synchronization.Data.DbSyncColumnDescription.AutoIncrementSeed" />; otherwise false.</returns>
        [XmlIgnore]
        public bool AutoIncrementSeedSpecified
        {
            get
            {
                return this._autoIncrementSeedSpecified;
            }
            set
            {
                this._autoIncrementSeedSpecified = value;
            }
        }

        /// <summary>Gets or sets the increment step value for a column that automatically increments its value when new rows are inserted into the table.</summary>
        /// <returns>The increment step value for an autoincrement column.</returns>
        [XmlAttribute("idStep")]
        public long AutoIncrementStep
        {
            get
            {
                return this._autoIncrementStep;
            }
            set
            {
                this._autoIncrementStepSpecified = true;
                this._autoIncrementStep = value;
            }
        }

        /// <summary>Gets or sets whether a value is specified for the <see cref="P:Microsoft.Synchronization.Data.DbSyncColumnDescription.AutoIncrementStep" /> property.</summary>
        /// <returns>true if a value is specified for <see cref="P:Microsoft.Synchronization.Data.DbSyncColumnDescription.AutoIncrementStep" />; otherwise false.</returns>
        [XmlIgnore]
        public bool AutoIncrementStepSpecified
        {
            get
            {
                return this._autoIncrementStepSpecified;
            }
            set
            {
                this._autoIncrementStepSpecified = value;
            }
        }

        /// <summary>Gets or sets the default value for the column when new rows are created.</summary>
        /// <returns>The default value for the column if it is set; otherwise, an empty string.</returns>
        [XmlAttribute("default")]
        public string DefaultValue
        {
            get
            {
                return this._defaultValue;
            }
            set
            {
                this._defaultValueSpecified = true;
                this._defaultValue = value;
            }
        }

        /// <summary>Gets or sets whether a value is specified for the <see cref="P:Microsoft.Synchronization.Data.DbSyncColumnDescription.DefaultValue" /> property.</summary>
        /// <returns>true if a value is specified for <see cref="P:Microsoft.Synchronization.Data.DbSyncColumnDescription.DefaultValue" />; otherwise false.</returns>
        [XmlIgnore]
        public bool DefaultValueSpecified
        {
            get
            {
                return this._defaultValueSpecified;
            }
            set
            {
                this._defaultValueSpecified = value;
            }
        }

        string DefaultValueString
        {
            get
            {
                string empty = string.Empty;
                if (this.DefaultValueSpecified)
                {
                    empty = string.Concat(" DEFAULT ", this.DefaultValue);
                }
                return empty;
            }
        }

        internal string DefinitionString
        {
            get
            {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.Append(string.Concat(this.QuotedName, " ", this.Type));
                stringBuilder.Append(this.ExtendedTypeInfoString);
                stringBuilder.Append(this.DefaultValueString);
                stringBuilder.Append(this.IdentityString);
                stringBuilder.Append(this.NullableString);
                return stringBuilder.ToString();
            }
        }

        internal string DefinitionStringForTrackingTable
        {
            get
            {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.Append(string.Concat(this.QuotedName, " ", this.Type));
                stringBuilder.Append(this.ExtendedTypeInfoString);
                stringBuilder.Append(this.NullableString);
                return stringBuilder.ToString();
            }
        }

        internal string DefinitionStringForType
        {
            get
            {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.Append(string.Concat(this.QuotedName, " ", this.Type));
                stringBuilder.Append(this.ExtendedTypeInfoString);
                return stringBuilder.ToString();
            }
        }

        string ExtendedTypeInfoString
        {
            get
            {
                string empty = string.Empty;
                string str = this._type;
                string str1 = str;
                if (str != null)
                {
                    switch (str1)
                    {
                        case "decimal":
                        case "numeric":
                            {
                                if (!this.PrecisionSpecified || !this.ScaleSpecified)
                                {
                                    break;
                                }
                                object[] precision = { "(", this.Precision, ",", this.Scale, ")" };
                                empty = string.Concat(precision);
                                break;
                            }
                        case "binary":
                        case "varbinary":
                        case "varchar":
                        case "char":
                        case "nvarchar":
                        case "nchar":
                            {
                                if (!this.SizeSpecified)
                                {
                                    break;
                                }
                                empty = string.Concat("(", this.Size, ")");
                                break;
                            }
                    }
                }
                return empty;
            }
        }

        internal string FilterDefinitionStringForTrackingTable
        {
            get
            {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.Append(string.Concat(this.QuotedName, " ", this.Type));
                stringBuilder.Append(this.ExtendedTypeInfoString);
                stringBuilder.Append(" NULL");
                return stringBuilder.ToString();
            }
        }

        string IdentityString
        {
            get
            {
                string empty = string.Empty;
                if (this.AutoIncrementStepSpecified && this.AutoIncrementSeedSpecified)
                {
                    object[] autoIncrementSeed = { " IDENTITY(", this.AutoIncrementSeed, ",", this.AutoIncrementStep, ")" };
                    empty = string.Concat(autoIncrementSeed);
                }
                return empty;
            }
        }

        /// <summary>Gets or sets a value that indicates whether null values are allowed in this column.</summary>
        /// <returns>true if null values are allowed; otherwise, false.</returns>
        [XmlAttribute("null")]
        public bool IsNullable
        {
            get
            {
                return this._isNullable;
            }
            set
            {
                this._isNullable = value;
            }
        }

        /// <summary>Gets or sets whether a value is specified for the <see cref="P:Microsoft.Synchronization.Data.DbSyncColumnDescription.IsNullable" /> property.</summary>
        /// <returns>true if a value is specified for <see cref="P:Microsoft.Synchronization.Data.DbSyncColumnDescription.IsNullable" />; otherwise false.</returns>
        [XmlIgnore]
        public bool IsNullableSpecified
        {
            get
            {
                return this._isNullable;
            }
        }

        /// <summary>Gets or sets whether this column is part of the primary key for the table.</summary>
        /// <returns>true if this column is part of the primary key for the table; otherwise false.</returns>
        [XmlAttribute("pk")]
        public bool IsPrimaryKey
        {
            get
            {
                return this._isPrimaryKey;
            }
            set
            {
                this._isPrimaryKey = value;
                if (this._isPrimaryKey)
                {
                    this._isNullable = false;
                }
            }
        }

        /// <summary>Gets or sets whether a value is specified for the <see cref="P:Microsoft.Synchronization.Data.DbSyncColumnDescription.IsPrimaryKey" /> property.</summary>
        /// <returns>true if a value is specified for <see cref="P:Microsoft.Synchronization.Data.DbSyncColumnDescription.IsPrimaryKey" />; otherwise false.</returns>
        [XmlIgnore]
        public bool IsPrimaryKeySpecified => this._isPrimaryKey;

        private string NullableString
        {
            get
            {
                if (!this.IsNullable)
                {
                    return " NOT NULL";
                }
                return " NULL";
            }
        }

        /// <summary>Gets or sets the name of the parameter that is used to represent this column in synchronization queries.</summary>
        /// <returns>The name of the parameter that is used to represent this column in synchronization queries.</returns>
        /// <exception cref="T:System.ArgumentNullException">
        ///   <paramref name="value" /> is a null.</exception>
        /// <exception cref="T:System.ArgumentException">
        ///   <paramref name="value" /> is empty.</exception>
        [XmlAttribute("param")]
        public string ParameterName
        {
            get
            {
                return this._parameterName;
            }
            set
            {
                this._parameterName = value;
            }
        }

        /// <summary>Gets or sets the precision for the column if the data type is numeric.</summary>
        /// <returns>The precision for the column if it is specified; otherwise -1.</returns>
        [XmlAttribute("prec")]
        public int Precision
        {
            get
            {
                return this._precision;
            }
            set
            {
                this._precisionSpecified = true;
                this._precision = value;
            }
        }

        /// <summary>Gets or sets whether a value is specified for the <see cref="P:Microsoft.Synchronization.Data.DbSyncColumnDescription.Precision" /> property.</summary>
        /// <returns>true if a value is specified for <see cref="P:Microsoft.Synchronization.Data.DbSyncColumnDescription.Precision" />; otherwise false.</returns>
        [XmlIgnore]
        public bool PrecisionSpecified
        {
            get
            {
                return this._precisionSpecified;
            }
            set
            {
                this._precisionSpecified = value;
            }
        }

        /// <summary>Gets the name of the column with database-specific delimiters.</summary>
        /// <returns>The name of the column with database-specific delimiters.</returns>
        public string QuotedName => this._quotedName;

        /// <summary>Gets or sets the scale for the column if the data type is numeric and has a decimal component.</summary>
        /// <returns>The scale for the column if it is specified; otherwise -1.</returns>
        [XmlAttribute("scale")]
        public int Scale
        {
            get
            {
                return this._scale;
            }
            set
            {
                this._scaleSpecified = true;
                this._scale = value;
            }
        }

        /// <summary>Gets or sets whether a value is specified for the <see cref="P:Microsoft.Synchronization.Data.DbSyncColumnDescription.Scale" /> property.</summary>
        /// <returns>true if a value is specified for <see cref="P:Microsoft.Synchronization.Data.DbSyncColumnDescription.Scale" />; otherwise false.</returns>
        [XmlIgnore]
        public bool ScaleSpecified
        {
            get
            {
                return this._scaleSpecified;
            }
            set
            {
                this._scaleSpecified = value;
            }
        }

        /// <summary>Gets or sets the size of the column.</summary>
        /// <returns>The size of the column if it is specified; otherwise null.</returns>
        [XmlAttribute("size")]
        public string Size
        {
            get
            {
                return this._size;
            }
            set
            {
                this._sizeSpecified = true;
                this._size = value;
            }
        }

        /// <summary>Gets or sets whether a value is specified for the <see cref="P:Microsoft.Synchronization.Data.DbSyncColumnDescription.Size" /> property.</summary>
        /// <returns>true if a value is specified for <see cref="P:Microsoft.Synchronization.Data.DbSyncColumnDescription.Size" />; otherwise false.</returns>
        [XmlIgnore]
        public bool SizeSpecified
        {
            get
            {
                return this._sizeSpecified;
            }
            set
            {
                this._sizeSpecified = value;
            }
        }

        /// <summary>Gets or sets the data type of the column.</summary>
        [XmlAttribute("type")]
        public string Type
        {
            get
            {
                return this._type;
            }
            set
            {
                this._type = value.ToLowerInvariant();
                this.SetDefaultPrecisionAndScale();
            }
        }

        /// <summary>Gets or sets the name of the column without database-specific delimiters.</summary>
        /// <returns>The name of the column without database-specific delimiters.</returns>
        /// <exception cref="T:System.ArgumentNullException">
        ///   <paramref name="value" /> is a null.</exception>
        /// <exception cref="T:System.ArgumentException">
        ///   <paramref name="value" /> is empty.</exception>
        [XmlAttribute("name")]
        public string UnquotedName
        {
            get
            {
                return this._unquotedName;
            }
            set
            {
                this._unquotedName = value;
                this._quotedName = string.Concat("[", this._unquotedName, "]");
            }
        }

        /// <summary>Initializes a new instance of the <see cref="T:Microsoft.Synchronization.Data.DbSyncColumnDescription" /> class by using default values.</summary>
        public ScopeConfigDataColumn()
        {
        }

        //internal ScopeConfigDataColumnDescription(SyncSchemaColumn dataColumn)
        //{
        //    this._unquotedName = dataColumn.ColumnName;
        //    this._quotedName = string.Concat("[", this._unquotedName, "]");
        //    this.SetPropertiesFromDataColumn(dataColumn);
        //}

        /// <summary>Initializes a new instance of the <see cref="T:Microsoft.Synchronization.Data.DbSyncColumnDescription" /> class for a column that has the specified name and data type.</summary>
        public ScopeConfigDataColumn(string columnName, string type)
        {
            ObjectNameParser objectNameParser = new ObjectNameParser(columnName);
            this._unquotedName = objectNameParser.ObjectName;
            this._quotedName = objectNameParser.QuotedObjectName;
            this.Type = type;
        }

        void SetDefaultPrecisionAndScale()
        {
            if (this._type.Equals("money"))
            {
                this.Precision = 19;
                this.Scale = 4;
                return;
            }
            if (this._type.Equals("smallmoney"))
            {
                this.Precision = 10;
                this.Scale = 4;
            }
        }

       
        /// <summary>Returns a string that represents the <see cref="T:Microsoft.Synchronization.Data.DbSyncColumnDescription" /> object.</summary>
        /// <returns>A string that represents the <see cref="T:Microsoft.Synchronization.Data.DbSyncColumnDescription" /> object.</returns>
        public override string ToString()
        {
            string empty = string.Empty;
            if (this.IsPrimaryKey)
            {
                empty = "PK ";
            }
            string[] unquotedName = { empty, "Column Name: \"", this.UnquotedName, "\" Type: \"", this.Type, "\"" };
            return string.Concat(unquotedName);
        }
    }
}
