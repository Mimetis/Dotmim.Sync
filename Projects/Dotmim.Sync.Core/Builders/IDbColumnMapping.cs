using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Dotmim.Sync.Builders
{
    public  interface IDbColumnMapping
    {

        /// <summary>
        /// Get the string DB type issued from a DmColumn OrginalDbType || DbType
        /// </summary>
        string GetDbTypeString(DmColumn column);


        /// <summary>
        /// Get the precision string for a DmColumn OrinalDbType || DbType
        /// </summary>
        /// <param name="column"></param>
        /// <returns></returns>
        string GetDbTypePrecisionStrinf(DmColumn column);

        /// <summary>
        /// Get the string representation of a DbType enumeration value.
        /// Useful when generating Table, Stored procedure or Trigger creation scripts 
        /// </summary>
        string GetDbTypeString(DbType dbType);


        /// <summary>
        /// Validate a type during a table cration
        /// </summary>
        bool ValidateType(string stype);

    }
}
