/*
MIT License

Copyright (c) 2022 Zoltan Csizmadia

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

using Dotmim.Sync;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;

namespace System.Text.Json.Serialization.Metadata
{
    /// <summary>
    /// Data contract resolver used to create JsonTypeInfo for types with DataContractAttribute.
    /// </summary>
    public class DataContractResolver : DefaultJsonTypeInfoResolver
    {

        /// <summary>
        /// Gets the JsonTypeInfo for the specified type.
        /// </summary>
        public static JsonTypeInfo GetTypeInfo(JsonTypeInfo jsonTypeInfo, JsonSerializerOptions options)
        {
            Guard.ThrowIfNull(jsonTypeInfo);

            options ??= JsonSerializerOptions.Default;

            if (jsonTypeInfo.Kind == JsonTypeInfoKind.Object)
            {
                foreach (var jsonPropertyInfo in CreateDataMembers(jsonTypeInfo, options).OrderBy((x) => x.Order))
                    jsonTypeInfo.Properties.Add(jsonPropertyInfo);
            }

            return jsonTypeInfo;
        }

        /// <summary>
        /// Gets the JsonTypeInfo for the specified type.
        /// </summary>
        public override JsonTypeInfo GetTypeInfo(Type type, JsonSerializerOptions options)
        {
            var jsonTypeInfo = base.GetTypeInfo(type, options);

            if (jsonTypeInfo.Kind != JsonTypeInfoKind.Object)
                return jsonTypeInfo;

            jsonTypeInfo.Properties.Clear();

            var ti = GetTypeInfo(jsonTypeInfo, options);

            return ti;
        }

        private static bool IsNullOrDefault(object obj)
        {
            if (obj is null)
                return true;

            var type = obj.GetType();

            if (!type.IsValueType)
                return false;

            return Activator.CreateInstance(type).Equals(obj);
        }

        private static IEnumerable<MemberInfo> EnumerateFieldsAndProperties(Type type, BindingFlags bindingFlags)
        {
            foreach (var fieldInfo in type.GetFields(bindingFlags))
                yield return fieldInfo;

            foreach (var propertyInfo in type.GetProperties(bindingFlags))
                yield return propertyInfo;
        }

        private static IEnumerable<JsonPropertyInfo> CreateDataMembers(JsonTypeInfo jsonTypeInfo, JsonSerializerOptions options)
        {
            var isDataContract = jsonTypeInfo.Type.GetCustomAttribute<DataContractAttribute>() != null;
            var bindingFlags = BindingFlags.Instance | BindingFlags.Public;

            if (isDataContract)
                bindingFlags |= BindingFlags.NonPublic;

            foreach (var memberInfo in EnumerateFieldsAndProperties(jsonTypeInfo.Type, bindingFlags))
            {
                if (memberInfo == null)
                    continue;

                DataMemberAttribute attr = null;
                if (isDataContract)
                {
                    attr = memberInfo.GetCustomAttribute<DataMemberAttribute>();
                    if (attr == null)
                        continue;
                }
                else
                {
                    if (memberInfo.GetCustomAttribute<IgnoreDataMemberAttribute>() != null)
                        continue;
                }

                Func<object, object> getValue = null;
                Action<object, object> setValue = null;
                Type propertyType = null;
                string propertyName = null;

                if (memberInfo.MemberType == MemberTypes.Field && memberInfo is FieldInfo fieldInfo)
                {
                    propertyName = attr?.Name ?? fieldInfo.Name;
                    propertyName = options.PropertyNamingPolicy?.ConvertName(propertyName) ?? propertyName;
                    propertyType = fieldInfo.FieldType;
                    getValue = fieldInfo.GetValue;
                    setValue = fieldInfo.SetValue;
                }
                else
                if (memberInfo.MemberType == MemberTypes.Property && memberInfo is PropertyInfo propertyInfo)
                {
                    propertyName = attr?.Name ?? propertyInfo.Name;
                    propertyName = options.PropertyNamingPolicy?.ConvertName(propertyName) ?? propertyName;
                    propertyType = propertyInfo.PropertyType;
                    if (propertyInfo.CanRead)
                    {
                        getValue = propertyInfo.GetValue;
                    }

                    if (propertyInfo.CanWrite)
                    {
                        setValue = propertyInfo.SetValue;
                    }
                }
                else
                {
                    continue;
                }

                var jsonPropertyInfo = jsonTypeInfo.CreateJsonPropertyInfo(propertyType, propertyName);
                if (jsonPropertyInfo == null)
                {
                    continue;
                }

                jsonPropertyInfo.Get = getValue;
                jsonPropertyInfo.Set = setValue;

                if (attr != null)
                {
                    jsonPropertyInfo.IsRequired = attr.IsRequired;
                    jsonPropertyInfo.Order = attr.Order;
                    jsonPropertyInfo.ShouldSerialize = !attr.EmitDefaultValue ? ((_, obj) => !IsNullOrDefault(obj)) : null;
                }

                if (!jsonPropertyInfo.IsRequired)
                {
                    var requiredAttr = memberInfo.GetCustomAttribute<RequiredAttribute>();
                    if (requiredAttr != null)
                    {
                        jsonPropertyInfo.IsRequired = true;
                    }
                }

                yield return jsonPropertyInfo;
            }
        }
    }
}