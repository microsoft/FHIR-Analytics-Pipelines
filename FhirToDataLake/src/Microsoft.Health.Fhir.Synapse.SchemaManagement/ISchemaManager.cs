// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement
{
    public interface ISchemaManager<T>
        where T : BaseSchemaNode
    {
        /// <summary>
        /// Get schema types for the specified resource type, return empty if the schema types don't exist.
        /// </summary>
        /// <param name="resourceType">Resource type string.</param>
        /// <returns>List of schema types, represents the schema types name for given resource type.</returns>
        public List<string> GetSchemaTypes(string resourceType);

        /// <summary>
        /// Get schema node for the specified schema type, return null if the schema type doesn't exist.
        /// </summary>
        /// <param name="schemaType">Schema type string.</param>
        /// <returns>Instance of FhirSchemaNode, represents the schema for given resource type.</returns>
        public T GetSchema(string schemaType);

        /// <summary>
        /// Get schema content for all schema types.
        /// Currently used for initializing parquet schema.
        /// </summary>
        /// <returns>A schema dictionary.</returns>
        public Dictionary<string, string> GetAllSchemaContent();

        /// <summary>
        /// Get schema nodes for all schema types.
        /// </summary>
        /// <returns>A schema node dictionary. Keys are schema types and values are instances of FHIR schema nodes.</returns>
        public Dictionary<string, T> GetAllSchemas();
    }
}
