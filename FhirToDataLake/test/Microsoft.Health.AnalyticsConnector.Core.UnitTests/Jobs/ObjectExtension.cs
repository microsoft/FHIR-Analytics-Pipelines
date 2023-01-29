// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Reflection;

namespace Microsoft.Health.AnalyticsConnector.Core.UnitTests.Jobs
{
    public static class ObjectExtension
    {
        public static object CloneObject(this object objSource)
        {
            // Get the type of source object and create a new instance of that type
            Type typeSource = objSource.GetType();
            object objTarget = Activator.CreateInstance(typeSource);

            // Get all the properties of source object type
            PropertyInfo[] propertyInfo = typeSource.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);

            // Assign all source property to target object 's properties
            foreach (PropertyInfo property in propertyInfo)
            {
                // Check whether property can be written to
                if (property.CanWrite)
                {
                    // check whether property type is value type, enum or string type
                    if (property.PropertyType.IsValueType || property.PropertyType.IsEnum || property.PropertyType == typeof(string))
                    {
                        property.SetValue(objTarget, property.GetValue(objSource, null), null);
                    }

                    // else property type is object/complex types, so need to recursively call this method until the end of the tree is reached
                    else
                    {
                        object objPropertyValue = property.GetValue(objSource, null);

                        property.SetValue(objTarget, objPropertyValue?.CloneObject(), null);
                    }
                }
            }

            return objTarget;
        }
    }
}
