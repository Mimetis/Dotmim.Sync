using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;

namespace Dotmim.Sync.Serialization.Serializers
{
    public class ObjectSerializer : TypeSerializer
    {

        public override void Serialize(DmSerializer dmSerializer, object obj, Type objType)
        {
            var currentType = objType;

            while (currentType != null && currentType != typeof(Object))
            {
                // Get the serialized members
                var members = DmUtils.GetMembers(currentType);

                //var ctorParameters = objType.GetConstructorParameters(true);
                //if (ctorParameters != null)
                //    members = DmUtils.GetMembersOrderedByParametersForConstructor(members, ctorParameters);

                foreach (MemberInfo member in members)
                {
                    object memberValue = member.GetValue(obj);
                    var memberType = member.GetMemberType();

                    dmSerializer.Serialize(memberValue, memberType);
                }

                currentType = currentType.GetTypeInfo().BaseType;
            }
        }

        public override object Deserialize(DmSerializer dmSerializer, Type objType, bool isDebugMode = false)
        {

            Object instance = objType.CreateInstance();

            var currentType = objType;

            while (currentType != null && currentType != typeof(Object))

            {
                List<MemberInfo> members = DmUtils.GetMembers(currentType);

                var membersValue = new object[members.Count];

                for (int i = 0; i < members.Count; i++)
                {
                    var member = members[i];
                    var memberValue = dmSerializer.GetObject(isDebugMode);

                    if (memberValue != null)
                        member.SetValue(instance, memberValue);
                }
                currentType = currentType.GetTypeInfo().BaseType;
            }

            return instance;
        }
    }
}
