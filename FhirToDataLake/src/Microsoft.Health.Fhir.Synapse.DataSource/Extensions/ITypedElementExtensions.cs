// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Linq;
using Hl7.Fhir.ElementModel;

namespace Microsoft.Health.Fhir.Synapse.DataSource.Extensions
{
    public static class ITypedElementExtensions
    {
        /// <summary>
        /// Get property value from ITypedElement.
        /// </summary>
        /// <param name="element">self element.</param>
        /// <param name="propertyName">property name.</param>
        /// <returns>property value.</returns>
        public static object GetPropertyValue(
            this ITypedElement element,
            string propertyName)
        {
            if (element == null ||
                string.IsNullOrEmpty(propertyName))
            {
                return null;
            }

            return element.Children(propertyName).FirstOrDefault()?.Value;
        }
    }
}
