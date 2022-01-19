// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement
{
    public interface IFhirSchemaManager<T>
        where T : FhirSchemaNode
    {
        /// <summary>
        /// Get FHIR schema node for the specified resource type, return null if the resource type doesn't exist.
        /// </summary>
        /// <param name="resourceType">Resource type string.</param>
        /// <returns>Instance of FhirSchemaNode, represents the schema for given resource type.</returns>
        public T GetSchema(string resourceType);

        /// <summary>
        /// Get FHIR schema nodes for all resource types.
        /// </summary>
        /// <returns>A FHIR schema node dictionary. Keys are resource types and values are instances of FHIR schema nodes.</returns>
        public Dictionary<string, T> GetAllSchemas();
    }
}
