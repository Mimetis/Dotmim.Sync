using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Dotmim.Sync.Scope
{
    /// <summary>
    /// Represents a scope config xml or json data
    /// </summary>
    [XmlRoot("SqlSyncProviderScopeConfiguration")]
    public class ScopeConfigData
    {
        Collection<ScopeConfigDataAdapter> _adapterConfigurations;

        /// <summary>Gets or sets a list of objects that are each associated with a table in a SQL Server database.</summary>
        [XmlElement("Adapter")]
        public Collection<ScopeConfigDataAdapter> AdapterConfigurations
        {
            get
            {
                return this._adapterConfigurations;
            }
        }

        /// <summary>Gets or sets a value that indicates whether the associated scope is a template.</summary>
        [XmlAttribute("IsTemplate")]
        public bool IsTemplate { get; set; }

        [XmlIgnore]
        public Guid ScopeConfigId { get; set; }

        /// <summary>Initializes a new instance of the <see cref="T:Microsoft.Synchronization.Data.SqlServer.SqlSyncProviderScopeConfiguration" /> class. </summary>
        public ScopeConfigData()
        {
            this._adapterConfigurations = new Collection<ScopeConfigDataAdapter>();
        }

      
    }
}
