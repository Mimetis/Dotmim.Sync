using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;

namespace DmBinaryFormatter.Serializers
{
    public class ObjectSerializer : TypeSerializer
    {

        public override void Serialize(DmSerializer dmSerializer, object obj, Type objType)
        {
            // Get the serialized members
            var members = DmUtils.GetMembers(objType);

            //var ctorParameters = objType.GetConstructorParameters(true);
            //if (ctorParameters != null)
            //    members = DmUtils.GetMembersOrderedByParametersForConstructor(members, ctorParameters);

            foreach (MemberInfo member in members)
            {
                object memberValue = member.GetValue(obj);
                var memberType = member.GetMemberType();

                dmSerializer.Serialize(memberValue, memberType);
            }

        }

        public override object Deserialize(DmSerializer dmSerializer, Type objType, bool isDebugMode = false)
        {
            Object instance = objType.CreateInstance();
            List<MemberInfo> members = DmUtils.GetMembers(objType);

            var membersValue = new object[members.Count];

            for (int i = 0; i < members.Count; i++)
            {
                var member = members[i];
                var memberValue = dmSerializer.GetObject(isDebugMode);
     
                if (memberValue != null)
                    member.SetValue(instance, memberValue);
            }


            return instance;
        }
    }
}
