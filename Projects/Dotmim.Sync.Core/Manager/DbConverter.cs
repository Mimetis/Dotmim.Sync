using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Manager
{
    public abstract class DbConverter
    {

        public static DbConverter GetConverter(DbConverterType type)
        {
            switch (type)
            {
                case DbConverterType.SqlServer:
                    break;
                case DbConverterType.MySql:
                    break;
                case DbConverterType.Sqlite:
                    return new DbConverterSqlite();
            }

            return null;
            
        }

        public abstract bool CanConvertFrom(DbConverterType type);

        public abstract DmColumn ConvertFrom(DbConverterType type, DmColumn dmColumn);

        public abstract bool IsValid(string type);
    }
}
