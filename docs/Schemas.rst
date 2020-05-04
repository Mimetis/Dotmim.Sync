Schemas
===============

.. note:: The schema feature is only avaialable for SQL Server


One great feature in **SQL Server** is the `schema <https://technet.microsoft.com/en-us/library/dd283095%28v=sql.100%29.aspx?f=255>`_  option.     

You can configure your sync tables with schema if you target the ``SqlSyncProvider``.

You have two way to configure schemas :  

Directly during the tables declaration, as string:

.. code-block:: csharp

    var tables = new string[] { "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product",
                                    "Address", "Customer", "CustomerAddress"};

    SyncAgent agent = new SyncAgent(clientProvider, serverProvider, tables);


Or on each table, from the `SyncSetup` setup instance:

.. code-block:: csharp

    var tables = new string[] { "ProductCategory", "ProductModel", "Product",
                                "Address", "Customer", "CustomerAddress"};

    SyncAgent agent = new SyncAgent(clientProvider, serverProvider, tables);

    agent.Setup.Tables["ProductCategory"].SchemaName = "SalesLt";
    agent.Setup.Tables["ProductModel"].SchemaName = "SalesLt";
    agent.Setup.Tables["Product"].SchemaName = "SalesLt";

Be careful, **schemas are not replicated if you target `SqliteSyncProvider` or `MySqlSyncProvider` as client providers**
