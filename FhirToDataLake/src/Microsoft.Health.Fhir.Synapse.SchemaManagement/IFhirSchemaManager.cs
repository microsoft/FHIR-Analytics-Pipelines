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
        /// Get schema types for the specified resource type, return empty if the schema types don't exist.
        /// </summary>
        /// <param name="resourceType">Resource type string.</param>
        /// <returns>List of schema types, represents the schema types name for given resource type.</returns>
        public List<string> GetSchemaTypes(string resourceType);

        /// <summary>
        /// Get FHIR schema node for the specified schema type, return null if the schema type doesn't exist.
        /// </summary>
        /// <param name="schemaType">Schema type string.</param>
        /// <returns>Instance of FhirSchemaNode, represents the schema for given resource type.</returns>
        public T GetSchema(string schemaType);

        /// <summary>
        /// Get FHIR schema nodes for all schema types.
        /// </summary>
        /// <returns>A FHIR schema node dictionary. Keys are schema types and values are instances of FHIR schema nodes.</returns>
        public Dictionary<string, T> GetAllSchemas();
    }
}
