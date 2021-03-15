using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace IdentityColumnsServer
{
    public interface ISeedingServices
    {
        Task<List<Seeding>> GetSeedingsAsync(Guid scopeId, DbConnection connection);
    }
}