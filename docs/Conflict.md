# Conflicts

## Default behavior

By default, conflicts are resolved automaticaly using the configuration policy property `ConflictResolutionPolicy` set in the `SyncConfiguration` object :  
You can choose: 
* `ConflictResolutionPolicy.ServerWins` : The server is allways the winner of any conflict. this behavior is the default behavior.
* `ConflictResolutionPolicy.ClientWins` : The client is allways the winner of any conflict.

> Default value is `ServerWins`.

``` cs
agent.SetConfiguration(c => {
    c.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;
});
``` 

## Resolution side

**A conflict is always resolved on the server side.**

Depending on your policy resolution, the workflow could be:
* A conflict is generated on the client and the server side (for example a row, with same Id, is updated on both the server and the client database)
* The client is launching a sync processus.
* The server tries to apply the row and a conflict is generated.
* The server resolve the conflict on the server side.
* If the server wins, the resolved server row is sent to the client and is *force-applied* on the client database.
* If the client wins, the server will *force-apply* the client row on the server. Nothing happen on the client, since the row is correct.


## Handling conflicts manually

If you decide to manually resolve a conflict, the `ConflictResolutionPolicy` option will be ignored.  
To be able to resolve a conflict, you just have to *Intercept*  the `ApplyChangedFailed` method and choose the correct version.  

``` cs
agent.LocalProvider.InterceptApplyChangesFailed(args =>
{
 // do stuff
}
```

The `ApplyChangeFailedEventArgs` argument contains all the required properties to be able to resolve your conflict:

You will determinate the correct version through the `Action` property of type `ConflictAction`:
* `ConflictAction.ClientWins` : The client row will be applied on server, even if there is a conflict, so the client row wins.
* `ConflictAction.ServerWins` : The client row won't be applied on the server, so the server row wins.
* `ConflictAction.MergeRow`   : It's up to you to choose the correct row to send on both server and client. the `FinalRow` object will be used instead of Server or Client row.

You are able to compare the row in conflict through the `Conflict` property of type `SyncConflict`:
* `Conflict.LocalRow`   : Contains the conflict row from the client side. This row is readonly.
* `Conflict.RemoteRow`  : Contains the conflict row from the server side. This row is readonly.
* `Conflict.Type`       : Gets the `ConflictType` enumeration. For example `ConflictType.RemoteUpdateLocalUpdate` represents a conflict row beetween an updated row on the server and the same row updated on the client as well.

You can use the current connection during this event to be able to perform actions on the server side through the `DbConnection` and `DbTransaction` properties.  
If you decide to rollback the transaction, all the sync process will be rollback. 

Eventually, the `FinalRow` property is used when you specify an Action to `ConflictAction.MergeRow`. In this way, you decide what will contains the row applied on both server and client side. Be careful, the `FinalRow` property is null until you specify the `Action` property to `ConflictAction.MergeRow` !

## TCP mode

Manually resolving a conflict based on a column value:

``` cs
agent.LocalProvider.InterceptApplyChangesFailed(e =>
{
    if (e.Conflict.RemoteRow.Table.TableName == "Region")
    {
        e.Action = (int)e.Conflict.RemoteRow["Id"] == 1 ? ConflictResolution.ClientWins : ConflictResolution.ServerWins;
    }
}
```

Manually resolving a conflict based on the conflict type :

``` cs
agent.LocalProvider.InterceptApplyChangesFailed(e =>
{
    switch (e.Conflict.Type)
    {
        case ConflictType.RemoteUpdateLocalInsert:
        case ConflictType.RemoteUpdateLocalUpdate:
        case ConflictType.RemoteUpdateLocalDelete:
        case ConflictType.RemoteInsertLocalInsert:
        case ConflictType.RemoteInsertLocalUpdate:
        case ConflictType.RemoteInsertLocalDelete:
            e.Action = ConflictResolution.ServerWins;
            break;
        case ConflictType.RemoteUpdateLocalNoRow:
        case ConflictType.RemoteInsertLocalNoRow:
        case ConflictType.RemoteDeleteLocalNoRow:
            e.Action = ConflictResolution.ClientWins;
            break;
    }
}
```

Resolving a conflict by specifying a merged row :

``` cs
agent.LocalProvider.InterceptApplyChangesFailed(e =>
{
    if (e.Conflict.RemoteRow.Table.TableName == "Region")
    {
        e.Action = ConflictResolution.MergeRow;
        e.FinalRow["RegionDescription"] = "Eastern alone !";
    }
}
```
Be careful, the `e.FinalRow` is null until you specify the `Action` property to `ConflictAction.MergeRow` !

### HTTP Mode

Since we see that conflicts are resolved on the server side, if you are in a proxy mode, involving a server web side, it is there that you need to intercept failed applied changes:

``` csharp
    public class SyncController : ControllerBase
    {
        private WebProxyServerProvider webProxyServer;

        // Injected thanks to Dependency Injection
        public SyncController(WebProxyServerProvider proxy)
        {
            webProxyServer = proxy;
        }

        [HttpPost]
        public async Task Post()
        {
            // Get the underline local provider
            var provider = webProxyServer.GetLocalProvider(this.HttpContext);

            provider.InterceptApplyChangesFailed(e =>
            {
                if (e.Conflict.RemoteRow.Table.TableName == "Region")
                {
                    e.Resolution = ConflictResolution.MergeRow;
                    e.FinalRow["RegionDescription"] = "Eastern alone !";
                }
                else
                {
                    e.Resolution = ConflictResolution.ServerWins;
                }
            });

            await webProxyServer.HandleRequestAsync(this.HttpContext);
        }
    }
```