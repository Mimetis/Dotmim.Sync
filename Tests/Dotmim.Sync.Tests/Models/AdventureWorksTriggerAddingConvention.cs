using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using System.Linq;

namespace Dotmim.Sync.Tests.Models
{
#if NET8_0
    public class AdventureWorksTriggerAddingConvention : IModelFinalizingConvention
    {
        public virtual void ProcessModelFinalizing(
            IConventionModelBuilder modelBuilder,
            IConventionContext<IConventionModelBuilder> context)
        {
            foreach (var entityType in modelBuilder.Metadata.GetEntityTypes())
            {
                var table = StoreObjectIdentifier.Create(entityType, StoreObjectType.Table);
                if (table != null
                    && entityType.GetDeclaredTriggers().All(t => t.GetDatabaseName(table.Value) == null))
                {
                    entityType.Builder.HasTrigger(table.Value.Name + "_Trigger");
                }

                foreach (var fragment in entityType.GetMappingFragments(StoreObjectType.Table))
                {
                    if (entityType.GetDeclaredTriggers().All(t => t.GetDatabaseName(fragment.StoreObject) == null))
                    {
                        entityType.Builder.HasTrigger(fragment.StoreObject.Name + "_Trigger");
                    }
                }
            }
        }
    }
#endif
}
