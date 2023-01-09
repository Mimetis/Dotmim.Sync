using Dotmim.Sync.Tests.Core;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tests.Fixtures
{

    public abstract class RelationalFixture {
    
     
    }

    public class SqlServerFixtureType : RelationalFixture  {   }
    public class MySqlFixtureType : RelationalFixture  {   }
    public class MariaDBFixtureType : RelationalFixture  {   }
    public class SqliteFixtureType : RelationalFixture  {   }
    public class PostgresFixtureType : RelationalFixture  {   }


    }
