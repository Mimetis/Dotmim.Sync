ScopeInfoClients
================================

What is a scope client ?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We saw that a **scope** is a set of tables and is stored in the :guilabel:`scope_info` table.

A **scope client** is a the association of one scope with a filter, and is stored in the :guilabel:`scope_info_client`  table.

A scope client record contains:

- A scope (think "**FROM**" in a database) : Set of tables defined in the scope_info table
- A list of filter parameters (think "**WHERE**" in a database) : The filter definition is stored in the scope. We are talking here about the values of theses filter.

Let's imagine you are synchronizing some **Products** and **ProductCategories**, where you want only the products of the category **"Books"**. You will have to define a scope client with the following parameters:

- **Scope** : :guilabel:`Product`, :guilabel:`ProductCategory` tables.
- **Filter parameters values** : ``ProductCategoryID = "Books"``

**DMS** will automatically create:

- The scope in **scope_info** with the 2 tables :guilabel:`Product`, :guilabel:`ProductCategory`.
- The filter parameter value ``ProductCategoryID = 'Books'`` in the **scope_info_client** table.
