Filters
=======================

You can apply a filter on any table, even if the filtered column belongs to another table.

For instance, you can apply a filter on the **Customer** table, even if the filter is set on the **Address** table on the **City** column.

In a nutshell, adding a filter for a specific table requires:

* Creating a ``SetupFilter`` instance for this table (you can not have more than one ``SetupFilter`` per table)
* Creating a *[parameter]* with a type and optionally a default value.
* Creating a *[where]* condition to map the *[parameter]* and a column from your table.
* If your filtered table is not the base table, you will have to specify one or more *[joins]* methods to reach the base filtered table.

Simple Filter
^^^^^^^^^^^^^^^^

.. note:: You will find a complete sample here : `Simple Filter sample <https://github.com/Mimetis/Dotmim.Sync/tree/master/Samples/SimpleFilter>`_ 


You have a straightforward method to add a filter, derivated from your ``SyncSetup`` instance:

.. code-block:: csharp

    setup.Filters.Add("Customer", "CustomerID");


Basically, this method will add a filter on the ``Customer`` table, based on the ``CustomerID`` column.

Internally, this method will:

* Creates a ``SetupFilter`` instance for the table ``Customer``.
* Creates a *Parameter* called ``CustomerID`` that will have the same type as the ``CustomerID`` column from the ``Customer`` table.
* Creates a *Where* condition where the ``CustomerID`` *parameter* will be compared to the ``CustomerID`` column from the ``Customer`` table.


Since you are creating a filter based on a table and a column existing in your ``SyncSetup``, you don't have to specify type, joins and where clauses.


Here is another way to create this simple filter:

.. code-block:: csharp

    var filter = new SetupFilter("Customer");
    // Add a column as parameter. This column will be automaticaly added in the tracking table
    filter.AddParameter("CustomerID", "Customer");
    // add the side where expression, mapping the parameter to the column
    filter.AddWhere("CustomerID", "Customer", "CustomerID");
    // add this filter to setup
    setup.Filters.Add(filter);


This code is a little bit more verbose, but is a little bit more flexible in some circumstances


Complex Filter
^^^^^^^^^^^^^^^^^^

.. note:: You will find a complete sample here : `Complex Filter sample <https://github.com/Mimetis/Dotmim.Sync/tree/master/Samples/ComplexFilter>`_ 


Usually, you have more than one filter, especially if you have foreign keys in between.
So far, you will have to manage the links between all your filtered tables.

To illustrate how it works, here is a straightforward scenario:

1) We Want only **Customers** from a specific **City** and a specific **Postal code**.
2) Each customer has **Addresses** and **Sales Orders** which should be filtered as well.

.. image:: assets/DatabaseDiagram.png
    :align: center
    :alt: Tables diagram

We will have to filter on each level:

* Level zero: **Address**
* Level one: **CustomerAddress**
* Level two: **Customer**, **SalesOrderHeader**
* Level four: **SalesOrderDetail**

The main difference with the *easy way* method, is that we will details all the methods on the ``SetupFilter`` to create a fully customized filter.


The ``SetupFilter`` class
---------------------------------

The ``SetupFilter`` class will allows you to personalize your filter on a defined table (``Customer`` in this example):

.. code-block:: csharp

    var customerFilter = new SetupFilter("Customer");


.. warning:: Be careful, you can have only **one** ``SetupFilter`` instance per table. Obviously, this instance will allow you to define multiple parameters / criterias!


The ``.AddParameter()`` method
------------------------------------

Allows you to add a new parameter to the ``_changes`` stored procedure.

This method can be called with two kind of arguments:

* Your parameter is a **custom** parameter. You have to define its name and its ``DbType``. Optionally, you can define if it can be null and its default value (SQL Server only)
* Your parameter is a **mapped**  column. Easier, you just have to define its name and the mapped column. This way, ``Dotmim.Sync`` will determine the parameter properties, based on the schema


For instance, the parameters declaration for the table ``Customer`` looks like:

.. code-block:: csharp

    customerFilter.AddParameter("City", "Address", true);
    customerFilter.AddParameter("postal", DbType.String, true, null, 20);

* ``City`` parameter is defined from the ``Address.City`` column.
* ``postal`` parameter is a **custom** defined parameter.
   
  * *Indeed we have a ``PostalCode`` column in the ``Address`` table, that could be used here. But we will use a custom parameter instead, for the example*

At the end, the generation code should looks like:

.. code-block:: sql

    ALTER PROCEDURE [dbo].[sCustomerAddress_Citypostal__changes]
        @sync_min_timestamp bigint,
        @sync_scope_id uniqueidentifier,
        @City varchar(MAX) NULL,
        @postal nvarchar(20) NULL
        
Where ``@City`` is a mapped parameter and ``@postal`` is a custom parameter.

The ``.AddJoin()`` method
-------------------------------

If your filter is applied on a column in the actual table, you don't need to add any ``join`` statement.

But, in our example, the ``Customer`` table is two levels below the ``Address`` table (where we have the filtered columns ``City`` and ``PostalCode``)

So far, we can add some join statement here, going from ``Customer`` to ``CustomerAddress`` then to ``Address``:


.. code-block:: csharp

    customerFilter.AddJoin(Join.Left, "CustomerAddress")
      .On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
    
    customerFilter.AddJoin(Join.Left, "Address")
      .On("CustomerAddress", "AddressId", "Address", "AddressId");

The generated statement now looks like:

.. code-block:: sql

    FROM [Customer] [base]
    RIGHT JOIN [tCustomer] [side]ON [base].[CustomerID] = [side].[CustomerID]
    LEFT JOIN [CustomerAddress] ON [CustomerAddress].[CustomerId] = [base].[CustomerId]
    LEFT JOIN [Address] ON [CustomerAddress].[AddressId] = [Address].[AddressId]


As you can see **DMS** will take care of quoted table / column names and aliases in the stored procedure.

Just focus on the name of your table.

The ``.AddWhere()`` method
---------------------------------

Now, and for each parameter, you will have to define the where condition.

Each parameter will be compare to an existing column in an existing table.

For instance: 

* The ``City`` parameter should be compared to the ``City`` column in the ``Address`` table.
* The ``postal`` parameter should be compared to the ``PostalCode`` column in the ``Address`` table:


.. code-block:: csharp

    // Mapping City parameter to Address.City column
    addressFilter.AddWhere("City", "Address", "City");
    // Mapping the custom "postal" parameter to Address.PostalCode
    addressFilter.AddWhere("PostalCode", "Address", "postal");



The generated sql statement looks like this:

.. code-block:: sql

  WHERE (
  (
   (
     ([Address].[City] = @City OR @City IS NULL) AND ([Address].[PostalCode] = @postal OR @postal IS NULL)
    )
   OR [side].[sync_row_is_tombstone] = 1
  )

The ``.AddCustomWhere()`` method
---------------------------------------

If you need more, this method will allow you to add your own ``where`` condition.

Be careful, this method takes a ``string`` as argument, which will not be parsed, but instead, just added at the end of the stored procedure statement.

.. warning:: If you are using the AddCustomWhere method, you **NEED** to handle deleted rows.

Using the ``AddCustomWhere`` method allows you to do *whatever you want* with the ``Where`` clause in the select changes.

For instance, here is the code that is generated using a ``AddCustomWhere`` clause:

.. code-block:: csharp

    var filter = new SetupFilter("SalesOrderDetail");
    filter.AddParameter("OrderQty", System.Data.DbType.Int16);
    filter.AddCustomWhere("{{{OrderQty}}} = @OrderQty");


.. code-block:: sql

    SELECT DISTINCT ..............
    WHERE (
    (
        [OrderQty] = @OrderQty
    )
    AND 
        [side].[timestamp] > @sync_min_timestamp
        AND ([side].[update_scope_id] <> @sync_scope_id OR [side].[update_scope_id] IS NULL)
    )

.. note:: The **{{{** and **}}}** characters are used to escape the column ``OrderQty``, and will be replaced by the escaper character of the database engine.
    
    * For **SQL Server** and **SQLite** it will be ``[`` and ``]``
    * For **MySQL** and **MariaDB** it will be `````
    * For **Postgres**, it will be ``"`` 


The problem here is pretty simple. 

1) When you are deleting a row, the tracking table marks the row as deleted (``sync_row_is_tombstone = 1``)
2) Your row is not existing anymore in the ``SalesOrderDetail`` table.
3) If you are not handling this situation, this deleted row will never been selected for sync, because of your ``where`` custom clause ...

Fortunately for us, we have a pretty simple workaround: Add a **custom condition** to also **retrieve deleted rows** in your custom where clause.

How to get deleted rows in your Where clause ?
---------------------------------------------------

Basically, all the deleted rows are stored in the tracking table.

* This tracking table is *aliased* and should be called in your clause with the alias ``side``.  
* Each row marked as deleted has a **bit** flag called ``sync_row_is_tombstone`` set to **1**.

You don't have to care about any timeline, since it's done automatically in the rest of the generated **SQL** statement.

That being said, you have eventually to add ``OR side.sync_row_is_tombstone = 1`` to your ``AddCustomWhere`` clause.

Here is the good ``AddCustomWhere`` method where deleted rows are handled correctly:

.. code-block:: csharp

    var filter = new SetupFilter("SalesOrderDetail");
    filter.AddParameter("OrderQty", System.Data.DbType.Int16);
    filter.AddCustomWhere("{{{OrderQty}}} = @OrderQty OR {{{side}}}.{{{sync_row_is_tombstone}}} = 1");
    setup.Filters.Add(filter);


Complete Sample
^^^^^^^^^^^^^^^^^

Here is the full sample, where we have defined the filters (``City`` and ``postal`` code) on each filtered tables: ``Customer``, ``CustomerAddress``, ``Address``, ``SalesOrderHeader`` and ``SalesOrderDetail``

You will find the source code in the last commit, project ``Dotmim.Sync.SampleConsole.csproj``, file ``program.cs``, method ``SynchronizeAsync()``:

.. code-block:: csharp

    var setup = new SyncSetup(new string[] {"ProductCategory",
        "ProductModel", "Product",
        "Address", "Customer", "CustomerAddress",
        "SalesOrderHeader", "SalesOrderDetail" });

    // ----------------------------------------------------
    // Horizontal Filter: On rows. Removing rows from source
    // ----------------------------------------------------
    // Over all filter : "we Want only customer from specific city and specific postal code"
    // First level table : Address
    // Second level tables : CustomerAddress
    // Third level tables : Customer, SalesOrderHeader
    // Fourth level tables : SalesOrderDetail

    // Create a filter on table Address on City Washington
    // Optional : Sub filter on PostalCode, for testing purpose
    var addressFilter = new SetupFilter("Address");

    // For each filter, you have to provider all the input parameters
    // A parameter could be a parameter mapped to an existing colum : 
    // That way you don't have to specify any type, length and so on ...
    // We can specify if a null value can be passed as parameter value : 
    // That way ALL addresses will be fetched
    // A default value can be passed as well, but works only on SQL Server (MySql is a damn ... thing)
    addressFilter.AddParameter("City", "Address", true);

    // Or a parameter could be a random parameter bound to anything. 
    // In that case, you have to specify everything
    // (This parameter COULD BE bound to a column, like City, 
    //  but for the example, we go for a custom parameter)
    addressFilter.AddParameter("postal", DbType.String, true, null, 20);

    // Then you map each parameter on wich table / column the "where" clause should be applied
    addressFilter.AddWhere("City", "Address", "City");
    addressFilter.AddWhere("PostalCode", "Address", "postal");
    setup.Filters.Add(addressFilter);

    var addressCustomerFilter = new SetupFilter("CustomerAddress");
    addressCustomerFilter.AddParameter("City", "Address", true);
    addressCustomerFilter.AddParameter("postal", DbType.String, true, null, 20);

    // You can join table to go from your table up (or down) to your filter table
    addressCustomerFilter.AddJoin(Join.Left, "Address")
        .On("CustomerAddress", "AddressId", "Address", "AddressId");

    // And then add your where clauses
    addressCustomerFilter.AddWhere("City", "Address", "City");
    addressCustomerFilter.AddWhere("PostalCode", "Address", "postal");
    setup.Filters.Add(addressCustomerFilter);

    var customerFilter = new SetupFilter("Customer");
    customerFilter.AddParameter("City", "Address", true);
    customerFilter.AddParameter("postal", DbType.String, true, null, 20);
    customerFilter.AddJoin(Join.Left, "CustomerAddress")
        .On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
    customerFilter.AddJoin(Join.Left, "Address")
        .On("CustomerAddress", "AddressId", "Address", "AddressId");
    customerFilter.AddWhere("City", "Address", "City");
    customerFilter.AddWhere("PostalCode", "Address", "postal");
    setup.Filters.Add(customerFilter);

    var orderHeaderFilter = new SetupFilter("SalesOrderHeader");
    orderHeaderFilter.AddParameter("City", "Address", true);
    orderHeaderFilter.AddParameter("postal", DbType.String, true, null, 20);
    orderHeaderFilter.AddJoin(Join.Left, "CustomerAddress")
        .On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
    orderHeaderFilter.AddJoin(Join.Left, "Address")
        .On("CustomerAddress", "AddressId", "Address", "AddressId");
    orderHeaderFilter.AddWhere("City", "Address", "City");
    orderHeaderFilter.AddWhere("PostalCode", "Address", "postal");
    setup.Filters.Add(orderHeaderFilter);

    var orderDetailsFilter = new SetupFilter("SalesOrderDetail");
    orderDetailsFilter.AddParameter("City", "Address", true);
    orderDetailsFilter.AddParameter("postal", DbType.String, true, null, 20);
    orderDetailsFilter.AddJoin(Join.Left, "SalesOrderHeader")
        .On("SalesOrderHeader", "SalesOrderID", "SalesOrderDetail", "SalesOrderID");
    orderDetailsFilter.AddJoin(Join.Left, "CustomerAddress")
        .On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
    orderDetailsFilter.AddJoin(Join.Left, "Address")
        .On("CustomerAddress", "AddressId", "Address", "AddressId");
    orderDetailsFilter.AddWhere("City", "Address", "City");
    orderDetailsFilter.AddWhere("PostalCode", "Address", "postal");
    setup.Filters.Add(orderDetailsFilter);

    // ----------------------------------------------------

And you ``SyncAgent`` now looks like:

.. code-block:: csharp

    // Creating an agent that will handle all the process
    var agent = new SyncAgent(clientProvider, serverProvider);

    // Adding 2 parameters
    // Because I've specified that "postal" could be null, 
    // I can set the value to DBNull.Value (and the get all postal code in Toronto city)
    var parameters = new SyncParameters
    {
        { "City", new Guid("Toronto") },
        { "postal", DBNull.Value }
    };    

    // [Optional]: Get some progress event during the sync process
    var progress = new SynchronousProgress<ProgressArgs>(
        pa => Console.WriteLine('$'"{pa.PogressPercentageString}\t {pa.Message}"));

    var s1 = await agent.SynchronizeAsync(setup, parameters, progress);


Http mode
^^^^^^^^^^^^^^

.. note:: You will find a complete sample here : `Complex Web Filter sample <https://github.com/Mimetis/Dotmim.Sync/tree/master/Samples/FilterWebSync>`_ 


If you're using the http mode, you will notice some differences between the **client side** and the **server side**:

* The **server side** will declare the filters.
* The **client side** will declare the paramaters.

Server side
--------------------

You have to declare your ``SetupFilters`` from within your ``ConfigureServices()`` method.

Pretty similar from the last example, excepting you do not add any ``SyncParameter`` value at the end:

.. code-block:: csharp

    public void ConfigureServices(IServiceCollection services)
    {
        services.AddControllers();

        services.AddDistributedMemoryCache();
        services.AddSession(options => options.IdleTimeout = TimeSpan.FromMinutes(30));

        // Get a connection string for your server data source
        var connectionString = Configuration.GetSection("ConnectionStrings")["DefaultConnection"];

        // Set the web server Options
        var options = new SyncOptions
        {
            BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "server")       
        };


        // Create the setup used for your sync process

        var setup = new SyncSetup("ProductCategory", "ProductModel", "Product",
                                   "Address", "Customer", "CustomerAddress",
                                   "SalesOrderHeader", "SalesOrderDetail")
        {
            StoredProceduresPrefix = "s",
            StoredProceduresSuffix = "",
            TrackingTablesPrefix = "s",
            TrackingTablesSuffix = ""
        };

        // Create a filter on table Address on City Washington
        // Optional : Sub filter on PostalCode, for testing purpose
        var addressFilter = new SetupFilter("Address");
        addressFilter.AddParameter("City", "Address", true);
        addressFilter.AddParameter("postal", DbType.String, true, null, 20);
        addressFilter.AddWhere("City", "Address", "City");
        addressFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(addressFilter);

        var addressCustomerFilter = new SetupFilter("CustomerAddress");
        addressCustomerFilter.AddParameter("City", "Address", true);
        addressCustomerFilter.AddParameter("postal", DbType.String, true, null, 20);
        addressCustomerFilter.AddJoin(Join.Left, "Address")
            .On("CustomerAddress", "AddressId", "Address", "AddressId");
        addressCustomerFilter.AddWhere("City", "Address", "City");
        addressCustomerFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(addressCustomerFilter);

        var customerFilter = new SetupFilter("Customer");
        customerFilter.AddParameter("City", "Address", true);
        customerFilter.AddParameter("postal", DbType.String, true, null, 20);
        customerFilter.AddJoin(Join.Left, "CustomerAddress")
            .On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
        customerFilter.AddJoin(Join.Left, "Address")
            .On("CustomerAddress", "AddressId", "Address", "AddressId");
        customerFilter.AddWhere("City", "Address", "City");
        customerFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(customerFilter);

        var orderHeaderFilter = new SetupFilter("SalesOrderHeader");
        orderHeaderFilter.AddParameter("City", "Address", true);
        orderHeaderFilter.AddParameter("postal", DbType.String, true, null, 20);
        orderHeaderFilter.AddJoin(Join.Left, "CustomerAddress")
            .On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
        orderHeaderFilter.AddJoin(Join.Left, "Address")
            .On("CustomerAddress", "AddressId", "Address", "AddressId");
        orderHeaderFilter.AddWhere("City", "Address", "City");
        orderHeaderFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(orderHeaderFilter);

        var orderDetailsFilter = new SetupFilter("SalesOrderDetail");
        orderDetailsFilter.AddParameter("City", "Address", true);
        orderDetailsFilter.AddParameter("postal", DbType.String, true, null, 20);
        orderDetailsFilter.AddJoin(Join.Left, "SalesOrderHeader")
            .On("SalesOrderHeader", "SalesOrderID", "SalesOrderDetail", "SalesOrderID");
        orderDetailsFilter.AddJoin(Join.Left, "CustomerAddress")
            .On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
        orderDetailsFilter.AddJoin(Join.Left, "Address")
            .On("CustomerAddress", "AddressId", "Address", "AddressId");
        orderDetailsFilter.AddWhere("City", "Address", "City");
        orderDetailsFilter.AddWhere("PostalCode", "Address", "postal");
        setup.Filters.Add(orderDetailsFilter);

        // add a SqlSyncProvider acting as the server hub
        services.AddSyncServer<SqlSyncProvider>(connectionString, setup, options);
    }

    public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
    {
        if (env.IsDevelopment())
            app.UseDeveloperExceptionPage();

        app.UseHttpsRedirection();
        app.UseRouting();
        app.UseSession();
        app.UseEndpoints(endpoints => endpoints.MapControllers());
    }

Client side
---------------

The client side shoud be familiar to you:

.. code-block:: csharp

    // Defining the local provider
    var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));

    // Replacing a classic remote orchestrator 
    // with a web proxy orchestrator that point on the web api
    var proxyClientProvider = new WebRemoteOrchestrator("http://localhost:52288/api/Sync");

    // Set the web server Options
    var options = new SyncOptions
    {
        BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "client")
    };

    // Creating an agent that will handle all the process
    var agent = new SyncAgent(clientProvider, proxyClientProvider, options);

    // [Optional]: Get some progress event during the sync process
    var progress = new SynchronousProgress<ProgressArgs>(
       pa => Console.WriteLine($"{pa.ProgressPercentage:p}\t {pa.Message}"));

    // Adding 2 parameters
    // Because I've specified that "postal" could be null, 
    // I can set the value to DBNull.Value (and the get all postal code in Toronto city)
    var parameters = new SyncParameters
    {
        { "City", new Guid("Toronto") },
        { "postal", DBNull.Value }
    }; 

    var s1 = await agent.SynchronizeAsync(parameters, progress);
