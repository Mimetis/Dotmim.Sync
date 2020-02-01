## Change Tracking

New **Sql** provider that uses the **Change Tracking** feature, from SQL Server.
This provider is called `SqlSyncChangeTrackingProvider`.

`SqlSyncChangeTrackingProvider` is compatible with all others sync providers.

What does it mean ?
- No more tracking tables in your database
- No more triggers on your tables in your database
- Metadatas retention managed by SQL Server itself 
- Some restrictions on filters (especially on Delete statement with a filter)

To be able to use `SqlSyncChangeTrackingProvider` on your database, do not forget to active the **Change Tracking** on your database :

``` SQL
ALTER DATABASE AdventureWorks  
SET CHANGE_TRACKING = ON  
(CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON)  
```

You don't have to activate **Change Tracking** on each table. It will be enabled by the `Dotmim.Sync` framework.

