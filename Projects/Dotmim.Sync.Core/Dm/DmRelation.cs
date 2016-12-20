using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Linq;

namespace Dotmim.Sync.Data
{
    public class DmRelation
    {
        private DmKey childKey;
        private DmKey parentKey;


        public DmSet DmSet { get; internal set; }

        public DmRelation(string relationName, DmColumn parentColumn, DmColumn childColumn)
        {
          
            DmColumn[] parentColumns = new DmColumn[1];
            parentColumns[0] = parentColumn;

            DmColumn[] childColumns = new DmColumn[1];
            childColumns[0] = childColumn;

            Create(relationName, parentColumns, childColumns);

        }

        /// <summary>
        /// Initializes a new instance of the DmRelation class using the specified name, matched arrays of parent
        /// and child columns, and value to create constraints.
        /// </summary>
        public DmRelation(string relationName, DmColumn[] parentColumns, DmColumn[] childColumns)
        {
            Create(relationName, parentColumns, childColumns);
        }

        private void Create(string relationName, DmColumn[] parentColumns, DmColumn[] childColumns)
        {
            try
            {
                parentKey = new DmKey(parentColumns);
                childKey = new DmKey(childColumns);

                if (parentColumns.Length != childColumns.Length)
                    throw new Exception("KeyLengthMismatch");

                for (int i = 0; i < parentColumns.Length; i++)
                {
                    if ((parentColumns[i].Table.DmSet == null) || (childColumns[i].Table.DmSet == null))
                        throw new Exception("Parent Or Child Columns Do Not Have DmSet");
                }

                CheckState();

                RelationName = relationName ?? "";
            }
            finally
            {
            }
        }


        /// <summary>
        /// The internal constraint object for the parent table.
        /// </summary>
        internal DmKey ParentKey
        {
            get
            {
                CheckStateForProperty();
                return parentKey;
            }
        }

        /// <summary>
        /// Gets the parent table of this relation.
        /// </summary>
        public virtual DmTable ParentTable
        {
            get
            {
                CheckStateForProperty();
                return parentKey.Table;
            }
        }


        public DmColumn[] ParentColumns => parentKey.Columns;

        /// <summary>
        /// Gets the child columns of this relation.
        /// </summary>
        public DmColumn[] ChildColumns => childKey.Columns;

        /// <summary>
        /// Gets the internal Key object for the child table.
        /// </summary>
        internal DmKey ChildKey
        {
            get
            {
                CheckStateForProperty();
                return childKey;
            }
        }

        /// <summary>
        /// Gets the child table of this relation.
        /// </summary>
        public virtual DmTable ChildTable
        {
            get
            {
                CheckStateForProperty();
                return childKey.Table;
            }
        }


        private static bool IsKeyNull(object[] values)
        {
            for (int i = 0; i < values.Length; i++)
                if (values[i] != null)
                    return false;

            return true;
        }

        /// <summary>
        /// Gets the child rows for the parent row across the relation using the version given
        /// </summary>
        internal static DmRow[] GetChildRows(DmKey parentKey, DmKey childKey, DmRow parentRow, DmRowVersion version)
        {
            object[] values = parentRow.GetKeyValues(parentKey, version);
            if (IsKeyNull(values))
                return new DmRow[] { childKey.Table.NewRow() };

            return childKey.Table.Rows.Where(r => childKey.ValuesAreEqual(r, values)).ToArray();

        }

        /// <summary>
        /// Gets the parent rows for the given child row across the relation using the version given
        /// </summary>
        internal static DmRow[] GetParentRows(DmKey parentKey, DmKey childKey, DmRow childRow, DmRowVersion version)
        {
            object[] values = childRow.GetKeyValues(childKey, version);
            if (IsKeyNull(values))
                return new DmRow[] { parentKey.Table.NewRow() };

            return parentKey.Table.Rows.Where(r => parentKey.ValuesAreEqual(r, values)).ToArray();
        }

        /// <summary>
        /// For a foreignkey, actually we have only one row
        /// </summary>
        internal static DmRow GetParentRow(DmKey parentKey, DmKey childKey, DmRow childRow, DmRowVersion version)
        {
            if (!childRow.HasVersion((version == DmRowVersion.Original) ? DmRowVersion.Original : DmRowVersion.Current))
            {
                if (childRow.tempRecord == -1)
                    return null;
            }

            object[] values = childRow.GetKeyValues(childKey, version);
            if (IsKeyNull(values))
                return null;

            return parentKey.Table.Rows.FirstOrDefault(r => parentKey.ValuesAreEqual(r, values));
        }

        /// <summary>
        /// Gets or sets the name used to look up this relation in the parent data
        /// </summary>
        public string RelationName { get; set; }

        // If we're not in a dataSet relations collection, we need to verify on every property get that we're
        // still a good relation object.
        internal void CheckState()
        {
            if (parentKey.Table.DmSet != childKey.Table.DmSet)
                throw new Exception("RelationDmSetMismatch");

            if (childKey.ColumnsEqual(parentKey))
                throw new Exception("KeyColumnsIdentical");

            for (int i = 0; i < parentKey.Columns.Length; i++)
            {
                if (parentKey.Columns[i].DataType != childKey.Columns[i].DataType)
                    throw new Exception("ColumnsTypeMismatch");
            }

            this.DmSet = parentKey.Table.DmSet;
        }

        /// <summary>
        /// Checks to ensure the DataRelation is a valid object, even if it doesn't
        /// </summary>
        protected void CheckStateForProperty()
        {
            try
            {
                CheckState();
            }
            catch (Exception e)
            {
                throw new Exception("BadObjectPropertyAccess " + e.Message);
            }
        }

        internal DmRelation Clone(DmSet destination)
        {

            DmTable parent = destination.Tables[ParentTable.TableName];
            DmTable child = destination.Tables[ChildTable.TableName];
            int keyLength = parentKey.Columns.Length;

            DmColumn[] parentColumns = new DmColumn[keyLength];
            DmColumn[] childColumns = new DmColumn[keyLength];

            for (int i = 0; i < keyLength; i++)
            {
                parentColumns[i] = parent.Columns[ParentKey.Columns[i].ColumnName];
                childColumns[i] = child.Columns[ChildKey.Columns[i].ColumnName];
            }

            DmRelation clone = new DmRelation(RelationName, parentColumns, childColumns);

            return clone;
        }

        /// <summary>
        /// </summary>
        public override string ToString() => RelationName;


    }

}
