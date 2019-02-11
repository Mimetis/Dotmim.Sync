# Schemas

One great feature in **SQL Server** is the [schema](https://technet.microsoft.com/en-us/library/dd283095%28v=sql.100%29.aspx?f=255&MSPPError=-2147217396) option.     
You can configure your sync tables with schema if you target the `SqlSyncProvider`.

You have two way to configure schemas :  

Directly during the tables declaration, as string:

``` cs
var tables = new string[] { "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product",
                                "Address", "Customer", "CustomerAddress"};

SyncAgent agent = new SyncAgent(clientProvider, serverProvider, tables);
```

On each table, from the `SyncConfiguration` action:

``` cs
var tables = new string[] { "ProductCategory", "ProductModel", "Product",
                                "Address", "Customer", "CustomerAddress"};

SyncAgent agent = new SyncAgent(clientProvider, serverProvider, tables);
agent.SetConfiguration(c =>{

    c["Address"].Schema = "SalesLT";
    c["Customer"].Schema = "SalesLT";
    c["CustomerAddress"].Schema = "SalesLT";
});

```
Be careful, **schemas are not replicated if you target `SqliteSyncProvider` or `MySqlSyncProvider` as client providers**
