//using System;
//using System.Collections.Generic;
//using System.Data;
//using System.Dynamic;
//using System.Reflection;
//using System.Reflection.Emit;
//using System.Text;

//namespace Dotmim.Sync
//{
//    /// <summary>
//    /// Not Yet Used, but soon ...
//    /// </summary>
//    public static class ContainerTypeBuilder
//    {
//        private static Dictionary<string, TypeInfo> cache = new Dictionary<string, TypeInfo>();

//        public static Type CreateNewType(SyncTable table)
//        {
//            if (cache.ContainsKey(table.TableName))
//            {
//                var t = cache[table.TableName].AsType();
//                return t;
//            }

//            var myTypeInfo = CompileResultTypeInfo(table);
//            var myType = myTypeInfo.AsType();
//            cache.Add(table.TableName, myTypeInfo);
//            return myType;
//        }

//        public static object CreateNewObject(SyncTable table)
//        {
//            var myType = CreateNewType(table);
//            return Activator.CreateInstance(myType);
//        }
//        private static TypeInfo CompileResultTypeInfo(SyncTable table)
//        {
//            var tb = GetTypeBuilder(table.TableName);
//            var constructor = tb.DefineDefaultConstructor(MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.RTSpecialName);

//            // Create row state property before
//            CreateProperty(tb, "RowState", typeof(DataRowState));

//            // Add all properties from Table
//            foreach (var column in table.Columns)
//                CreateProperty(tb, column.ColumnName, column.GetDataType());

//            var objectType = tb.CreateTypeInfo();
//            return objectType;
//        }

//        private static TypeBuilder GetTypeBuilder(string tableName)
//        {
//            var typeSignature = tableName;
//            var an = new AssemblyName(typeSignature);

//            var assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(an, AssemblyBuilderAccess.Run);

//            var moduleBuilder = assemblyBuilder.DefineDynamicModule("MainModule");

//            var tb = moduleBuilder.DefineType(typeSignature,
//                    TypeAttributes.Public |
//                    TypeAttributes.Class |
//                    TypeAttributes.AutoClass |
//                    TypeAttributes.AnsiClass |
//                    TypeAttributes.BeforeFieldInit |
//                    TypeAttributes.AutoLayout,
//                    null);

//            return tb;
//        }

//        private static void CreateProperty(TypeBuilder tb, string propertyName, Type propertyType)
//        {
//            var fieldBuilder = tb.DefineField("_" + propertyName, propertyType, FieldAttributes.Private);

//            var propertyBuilder = tb.DefineProperty(propertyName, PropertyAttributes.HasDefault, propertyType, null);
//            var getPropMthdBldr = tb.DefineMethod("get_" + propertyName, MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig, propertyType, Type.EmptyTypes);
//            var getIl = getPropMthdBldr.GetILGenerator();

//            getIl.Emit(OpCodes.Ldarg_0);
//            getIl.Emit(OpCodes.Ldfld, fieldBuilder);
//            getIl.Emit(OpCodes.Ret);

//            MethodBuilder setPropMthdBldr =
//                tb.DefineMethod("set_" + propertyName,
//                  MethodAttributes.Public |
//                  MethodAttributes.SpecialName |
//                  MethodAttributes.HideBySig,
//                  null, new[] { propertyType });

//            var setIl = setPropMthdBldr.GetILGenerator();
//            var modifyProperty = setIl.DefineLabel();
//            var exitSet = setIl.DefineLabel();

//            setIl.MarkLabel(modifyProperty);
//            setIl.Emit(OpCodes.Ldarg_0);
//            setIl.Emit(OpCodes.Ldarg_1);
//            setIl.Emit(OpCodes.Stfld, fieldBuilder);

//            setIl.Emit(OpCodes.Nop);
//            setIl.MarkLabel(exitSet);
//            setIl.Emit(OpCodes.Ret);

//            propertyBuilder.SetGetMethod(getPropMthdBldr);
//            propertyBuilder.SetSetMethod(setPropMthdBldr);
//        }
//    }
//    public class ContainerField
//    {
//        public ContainerField(string name, Type type)
//        {
//            this.FieldName = name;
//            this.FieldType = type;
//        }

//        public string FieldName { get; set; }
//        public Type FieldType { get; set; }
//    }
//}
