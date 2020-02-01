# Direction : Bidirectional, DownloadOnly, UploadOnly

You can set a synchronization direction to each table.  
Use the `SyncDirection` enumeration for each table in the `SyncSetup` object.

> `Bidirectional` is the default value for all tables added.

``` cs
public enum SyncDirection
{
    Bidirectional = 1,
    DownloadOnly = 2,
    UploadOnly = 3
}
```

In this example, `Customer` `CustomerAddress` and `Address` are defined as `DownloadOnly` :

``` cs
var tables = new string[] { "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product",
                                "SalesLT.Address", "SalesLT.Customer", "SalesLT.CustomerAddress"};

var syncSetup = new SyncSetup(tables);
syncSetup.Tables["Customer"].SyncDirection = SyncDirection.DownloadOnly;
syncSetup.Tables["CustomerAddress"].SyncDirection = SyncDirection.DownloadOnly;
syncSetup.Tables["Address"].SyncDirection = SyncDirection.DownloadOnly;

var agent = new SyncAgent(clientProvider, serverProvider, syncSetup);

```
 