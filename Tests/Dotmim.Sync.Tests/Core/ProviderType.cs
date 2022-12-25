using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tests.Core
{
    [Flags]
    public enum ProviderType
    {
        Sql = 1,
        MySql = 2,
        Sqlite = 4,
        MariaDB = 8,
        Postgres = 16,

    }

    public static class EnumExtensions
    {
        /// <summary>
        /// Funny extension method to be able to retrieve all the enum flags I set
        /// </summary>
        public static IEnumerable<Enum> GetFlags(this Enum input)
        {
            foreach (Enum value in Enum.GetValues(input.GetType()))
                if (input.HasFlag(value))
                    yield return value;
        }
    }
    
}
