// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet
{
    public class FhirParquetSchemaManager : IFhirSchemaManager<FhirParquetSchemaNode>
    {
        private readonly Dictionary<string, List<string>> _schemaTypesMap;
        private readonly Dictionary<string, FhirParquetSchemaNode> _resourceSchemaNodesMap;
        private readonly ILogger<FhirParquetSchemaManager> _logger;

        // Will be moved to schema configurations
        private readonly bool _customizedSchema = false;

        public FhirParquetSchemaManager(
            IOptions<SchemaConfiguration> schemaConfiguration,
            ILogger<FhirParquetSchemaManager> logger)
        {
            _logger = logger;

            _resourceSchemaNodesMap = LoadDefaultSchemas(schemaConfiguration.Value.SchemaCollectionDirectory);
            _logger.LogInformation($"{_resourceSchemaNodesMap.Count} resource default schemas been loaded.");

            if (_customizedSchema)
            {
                var resourceCustomizedSchemaNodesMap = LoadCustomizedSchemas(schemaConfiguration.Value.CustomizedSchemaImageReference);
                _logger.LogInformation($"{resourceCustomizedSchemaNodesMap.Count} resource customized schemas been loaded.");
                _resourceSchemaNodesMap = _resourceSchemaNodesMap.Concat(resourceCustomizedSchemaNodesMap).ToDictionary(x => x.Key, x => x.Value);
            }

            _logger.LogInformation($"Initialize FHIR schemas completed.");

            _schemaTypesMap = new Dictionary<string, List<string>>();

            // Temporarily set schema type list only contains single value for each resource type
            // E.g:
            // Schema list for "Patient" resource is ["Patient"]
            // Schema list for "Observation" resource is ["Observation"]
            foreach (var schemaNodeItem in _resourceSchemaNodesMap)
            {
                if (_schemaTypesMap.ContainsKey(schemaNodeItem.Value.Type))
                {
                    _schemaTypesMap[schemaNodeItem.Value.Type].Add(schemaNodeItem.Key);
                }
                else
                {
                    _schemaTypesMap.Add(schemaNodeItem.Value.Type, new List<string>() { schemaNodeItem.Key });
                }
            }

            // After supporting customized schema, the schema type map will be set below when customized schema is enable
            // _ = SchemaTypesMap.TryAdd(resource, new List<string>() { resource, $"{resource}_customized"});
        }

        public List<string> GetSchemaTypes(string resourceType)
        {
            if (!_schemaTypesMap.ContainsKey(resourceType))
            {
                _logger.LogError($"Schema types for {resourceType} is empty.");
                return new List<string>();
            }

            return _schemaTypesMap[resourceType];
        }

        public FhirParquetSchemaNode GetSchema(string schemaType)
        {
            if (!_resourceSchemaNodesMap.ContainsKey(schemaType))
            {
                _logger.LogError($"Schema for schema type {schemaType} is not supported.");
                return null;
            }

            return _resourceSchemaNodesMap[schemaType];
        }

        public Dictionary<string, FhirParquetSchemaNode> GetAllSchemas()
        {
            return new Dictionary<string, FhirParquetSchemaNode>(_resourceSchemaNodesMap);
        }

        private Dictionary<string, FhirParquetSchemaNode> LoadCustomizedSchemas(string schemaImageReference)
        {
            var jSchemaCollections = _jsonSchemaCollectionsProvider.GetJsonSchemaCollectionAsync(schemaImageReference, CancellationToken.None).Result;
            _logger.LogInformation($"{jSchemaCollections.Count} resource customized schemas have been fetched from {schemaImageReference}.");

            var customizedSchemaNodesMap = new Dictionary<string, FhirParquetSchemaNode>();

            foreach (var jSchemaItem in jSchemaCollections)
            {
                // The customized schema keys are "{resourceType}_customized". E.g. "Patient_customized"
                var parquetSchemaNode = _jsonSchemaParser.ParseJSchema(jSchemaItem.Key, jSchemaItem.Value);
                customizedSchemaNodesMap.Add(parquetSchemaNode.Name, parquetSchemaNode);
            }

            return customizedSchemaNodesMap;
        }
    }
}
