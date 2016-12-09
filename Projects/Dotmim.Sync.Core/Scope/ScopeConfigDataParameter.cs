using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Dotmim.Sync.Core.Scope
{
  
    /// <summary>
    /// Contains a name and value pair for a filter parameter that controls what items are enumerated during synchronization.
    /// </summary>
    [XmlInclude(typeof(DBNull))]
    public class ScopeConfigDataParameter
    {
        string _parameterName;

        object _value;

        /// <summary>Gets or sets the name of the filter parameter.</summary>
        [XmlAttribute("name")]
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

        /// <summary>Gets or set the value of the filter parameter.</summary>
        [XmlElement("Value")]
        public object Value
        {
            get
            {
                return this._value;
            }
            set
            {
                this._value = value;
            }
        }

        public ScopeConfigDataParameter()
        {
        }

        public ScopeConfigDataParameter(string parameterName, object value)
        {
            this._parameterName = parameterName ;
            this._value = value;
        }
    }
}
