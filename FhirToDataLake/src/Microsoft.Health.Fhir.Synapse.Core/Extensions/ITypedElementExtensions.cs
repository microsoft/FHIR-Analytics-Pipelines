// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Hl7.Fhir.ElementModel;
using Hl7.Fhir.Serialization;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;

namespace Microsoft.Health.Fhir.Synapse.Core.Extensions
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

        public static DateTime? GetLastUpdatedDay(this ITypedElement element)
        {
            var result = element?.Children(FhirBundleConstants.MetaKey).FirstOrDefault()?.GetPropertyValue(FhirBundleConstants.LastUpdatedKey);
            if (result == null)
            {
                return null;
            }

            var lastUpdateDatetime = DateTimeOffset.Parse(result.ToString());
            return new DateTime(lastUpdateDatetime.Year, lastUpdateDatetime.Month, lastUpdateDatetime.Day);
        }

        public static JsonBatchData ToJsonBatchData(this IEnumerable<ITypedElement> elementData)
        {
            if (elementData == null)
            {
                return null;
            }

            return new JsonBatchData(
                elementData.Select(element => element.ToJObject()).ToList());
        }
    }
}
