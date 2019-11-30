using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Dotmim.Sync.Tests.StandAlone
{
    public class DmRelationTests
    {
        private DmSet _set = null;
        private DmSet _compositeFkSet = null;
        private DmTable clientTable = null;
        private DmTable clientTypeTable = null;

        public DmRelationTests()
        {
            _set = new DmSet();
            clientTable = new DmTable("Client");
            clientTypeTable = new DmTable("TypeClient");
            _set.Tables.Add(clientTable);
            _set.Tables.Add(clientTypeTable);

            DmColumn Col0 = new DmColumn<int>("ClientId");
            DmColumn Col1 = new DmColumn<int>("ClientType");
            DmColumn Col2 = new DmColumn<String>("ClientName");
            clientTable.Columns.Add(Col0);
            clientTable.Columns.Add(Col1);
            clientTable.Columns.Add(Col2);
            clientTable.PrimaryKey = new DmKey(Col0);

            DmColumn Col3 = new DmColumn<int>("TypeId");
            DmColumn Col4 = new DmColumn<string>("TypeName");
            clientTypeTable.Columns.Add(Col3);
            clientTypeTable.Columns.Add(Col4);
            clientTypeTable.PrimaryKey = new DmKey(Col3);

            _compositeFkSet = BuildCompositeFKDatabaseModel();
            BuilCompositeFKdSampleData(_compositeFkSet);
        }

        [Fact]
        public void Foreign()
        {

            DmRelation Relation = new DmRelation("FK_ClientType",
                clientTypeTable.Columns["TypeId"],
                clientTable.Columns["ClientType"]);

            _set.Relations.Add(Relation);

            DmRow Row = clientTable.NewRow();
            Row[0] = 1;
            Row[1] = 1;
            Row[2] = "Sébastien";
            clientTable.Rows.Add(Row);

            Row = clientTable.NewRow();
            Row[0] = 2;
            Row[1] = 1;
            Row[2] = "Pierre";
            clientTable.Rows.Add(Row);

            Row = clientTable.NewRow();
            Row[0] = 3;
            Row[1] = 2;
            Row[2] = "Paul";

            Row = clientTypeTable.NewRow();
            Row[0] = 1;
            Row[1] = "Grand Compte";
            clientTypeTable.Rows.Add(Row);

            Row = clientTypeTable.NewRow();
            Row[0] = 2;
            Row[1] = "PME";
            clientTypeTable.Rows.Add(Row);

            // Get all rows wher ClientType = "Grand Compte"
            var rowTypeGrandCompte = clientTypeTable.FindByKey(1);
            // Get all childs
            var rowsClients = rowTypeGrandCompte.GetChildRows("FK_ClientType");
            Assert.Equal(2, rowsClients.Length);

            // Get all parents
            var rowClient = clientTable.Rows[0];
            var rowClientType = rowClient.GetParentRow("FK_ClientType");


            Assert.Equal("Grand Compte", rowClientType[1]);
        }

        [Fact]
        public void Composite_ForeignKey_FindRow()
        {
            //TODO: Fix schema in DmRElation
            var spaghettiDish = _compositeFkSet.Tables["MenusCategoriesRows", ""].FindByKey(new object[] { 6, 2, 4 });
            Assert.True((spaghettiDish?.ItemArray[3])?.ToString() == "Spaghetti with tomato", "Row not be found.");
        }

        [Fact]
        public void Composite_ForeignKey_Navigation()
        {
            //TODO: Fix schema in DmRElation
            var hollydayMenu = _compositeFkSet.Tables["Menus", ""].FindByKey(2);

            Assert.True(hollydayMenu != null, "The child row could not be found.");

            var hollydayMenuCategory = hollydayMenu.GetChildRows("FK_MenusCategories_Menu");

            Assert.True(hollydayMenuCategory != null, "The child rows could not be found.");

            Assert.True(hollydayMenuCategory.Length == 2, "Invalid number of rows.");

            var hollydayMenuDishes = hollydayMenuCategory[0].GetChildRows("FK_MenusCategoriesRows_MenusCategories");

            Assert.True(hollydayMenuDishes?.Length == 2, "Invalid number of rows.");
        }

        private static DmSet BuildCompositeFKDatabaseModel()
        {
            var set = new DmSet();

            var menuTable = new DmTable("Menus");
            menuTable.Columns.Add(new DmColumn<int>("Id"));
            menuTable.Columns.Add(new DmColumn<string>("Description"));
            menuTable.PrimaryKey = new DmKey(menuTable.Columns[0]);

            set.Tables.Add(menuTable);

            var menuCategoryTable = new DmTable("MenusCategories");
            menuCategoryTable.Columns.Add(new DmColumn<int>("Id"));
            menuCategoryTable.Columns.Add(new DmColumn<int>("MenuId"));
            menuCategoryTable.Columns.Add(new DmColumn<string>("Description"));
            menuCategoryTable.PrimaryKey = new DmKey(new[]
            {
                menuCategoryTable.Columns[0],
                menuCategoryTable.Columns[1],
            });

            set.Tables.Add(menuCategoryTable);

            var menuCategoryRowTable = new DmTable("MenusCategoriesRows");
            menuCategoryRowTable.Columns.Add(new DmColumn<int>("Id"));
            menuCategoryRowTable.Columns.Add(new DmColumn<int>("MenuId"));
            menuCategoryRowTable.Columns.Add(new DmColumn<int>("MenuCatogoryId"));
            menuCategoryRowTable.Columns.Add(new DmColumn<string>("Description"));
            menuCategoryRowTable.Columns.Add(new DmColumn<decimal>("Price"));
            menuCategoryRowTable.PrimaryKey = new DmKey(new[]
                {
                 menuCategoryRowTable.Columns[0],
                 menuCategoryRowTable.Columns[1],
                 menuCategoryRowTable.Columns[2],
                });

            set.Tables.Add(menuCategoryRowTable);

            set.Relations
                .Add(new DmRelation("FK_MenusCategories_Menu"
                    , menuTable.Columns[0]                  // Id
                    , menuCategoryTable.Columns[1]));       // MenuId

            set.Relations
                .Add(new DmRelation("FK_MenusCategoriesRows_MenusCategories"
                    , new DmColumn[]
                        {
                            menuCategoryTable.Columns[0],	// Id
							menuCategoryTable.Columns[1],	// MenuId
						}
                    , new DmColumn[]
                        {
                            menuCategoryRowTable.Columns[2],// MenuCatogoryId
							menuCategoryRowTable.Columns[1],// MenuId
						}));
            return set;
        }

        private static void BuilCompositeFKdSampleData(DmSet set)
        {
            //TODO: Fix schema in DmRElation
            var menuTable = set.Tables["Menus",""];
            var menuCategoryTable = set.Tables["MenusCategories", ""];
            var menuCategoryRowTable = set.Tables["MenusCategoriesRows", ""];
            // Build sample data

            var menu1 = menuTable.NewRow();
            menu1["Id"] = 1;
            menu1["Description"] = "Standard Menu";

            menuTable.Rows.Add(menu1);

            var menu1Category1 = menuCategoryTable.NewRow();
            menu1Category1["Id"] = 1;
            menu1Category1["MenuId"] = 1;       // Standard Menu
            menu1Category1["Description"] = "First dishes";

            menuCategoryTable.Rows.Add(menu1Category1);

            var menu1Category1Row1 = menuCategoryRowTable.NewRow();

            menu1Category1Row1["Id"] = 1;
            menu1Category1Row1["MenuId"] = 1;       // Standard Menu
            menu1Category1Row1["MenuCatogoryId"] = 1;// First dishes
            menu1Category1Row1["Description"] = "Spaghetti with carbonara";
            menu1Category1Row1["Price"] = 5m;

            menuCategoryRowTable.Rows.Add(menu1Category1Row1);

            var menu1Category1Row2 = menuCategoryRowTable.NewRow();

            menu1Category1Row2["Id"] = 2;
            menu1Category1Row2["MenuId"] = 1;       // Standard Menu
            menu1Category1Row2["MenuCatogoryId"] = 1;// First dishes
            menu1Category1Row2["Description"] = "Spaghetti with tomato";
            menu1Category1Row2["Price"] = 3.5m;

            menuCategoryRowTable.Rows.Add(menu1Category1Row2);

            var menu1Category2 = menuCategoryTable.NewRow();
            menu1Category2["Id"] = 2;
            menu1Category2["MenuId"] = 1;       // Standard Menu
            menu1Category2["Description"] = "Seconds dishes";

            menuCategoryTable.Rows.Add(menu1Category2);

            var menu1Category2Row1 = menuCategoryRowTable.NewRow();
            menu1Category2Row1["Id"] = 3;
            menu1Category2Row1["MenuId"] = 1;       // Standard Menu
            menu1Category2Row1["MenuCatogoryId"] = 2;// Seconds dishes
            menu1Category2Row1["Description"] = "Spaghetti with tomato";
            menu1Category2Row1["Price"] = 9m;

            menuCategoryRowTable.Rows.Add(menu1Category2Row1);

            var menu2 = menuTable.NewRow();
            menu2["Id"] = 2;
            menu2["Description"] = "Hollyday Menu";

            menuTable.Rows.Add(menu2);

            var menu2Category1 = menuCategoryTable.NewRow();
            menu2Category1["Id"] = 3;
            menu2Category1["MenuId"] = 2;       // Hollyday Menu
            menu2Category1["Description"] = "First dishes";

            menuCategoryTable.Rows.Add(menu2Category1);

            var menu2Category1Row1 = menuCategoryRowTable.NewRow();

            menu2Category1Row1["Id"] = 4;
            menu2Category1Row1["MenuId"] = 2;       // Hollyday Menu
            menu2Category1Row1["MenuCatogoryId"] = 3;// First dishes
            menu2Category1Row1["Description"] = "Spaghetti with carbonara";
            menu2Category1Row1["Price"] = 8m;

            menuCategoryRowTable.Rows.Add(menu2Category1Row1);

            var menu2Category1Row2 = menuCategoryRowTable.NewRow();

            menu2Category1Row2["Id"] = 5;
            menu2Category1Row2["MenuId"] = 2;       // Hollyday Menu
            menu2Category1Row2["MenuCatogoryId"] = 3;// First dishes
            menu2Category1Row2["Description"] = "Spaghetti with tomato";
            menu2Category1Row2["Price"] = 5m;

            menuCategoryRowTable.Rows.Add(menu2Category1Row2);

            var menu2Category2 = menuCategoryTable.NewRow();
            menu2Category2["Id"] = 4;
            menu2Category2["MenuId"] = 2;       //  Hollyday Menu
            menu2Category2["Description"] = "Seconds dishes";

            menuCategoryTable.Rows.Add(menu2Category2);

            var menu2Category2Row1 = menuCategoryRowTable.NewRow();
            menu2Category2Row1["Id"] = 6;
            menu2Category2Row1["MenuId"] = 2;       // Hollyday Menu
            menu2Category2Row1["MenuCatogoryId"] = 4;// Seconds dishes
            menu2Category2Row1["Description"] = "Spaghetti with tomato";
            menu2Category2Row1["Price"] = 12m;

            menuCategoryRowTable.Rows.Add(menu2Category2Row1);
        }

    }
}
