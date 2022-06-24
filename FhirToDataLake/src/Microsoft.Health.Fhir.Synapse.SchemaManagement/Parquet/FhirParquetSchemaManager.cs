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
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.CustomizedSchema;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet
{
    public class FhirParquetSchemaManager : IFhirSchemaManager<FhirParquetSchemaNode>
    {
        private readonly Dictionary<string, List<string>> _schemaTypesMap;
        private readonly Dictionary<string, FhirParquetSchemaNode> _resourceSchemaNodesMap;
        private readonly ILogger<FhirParquetSchemaManager> _logger;
        private readonly JsonSchemaParser _jsonSchemaParser;
        private readonly JsonSchemaCollectionsProvider _jsonSchemaCollectionsProvider;

        // Will be moved to schema configurations
        private readonly bool _customizedSchema = true;

        public FhirParquetSchemaManager(
            IOptions<SchemaConfiguration> schemaConfiguration,
            JsonSchemaCollectionsProvider jSchemaCollectionsProvider,
            ILogger<FhirParquetSchemaManager> logger)
        {
            _logger = logger;
            _jsonSchemaParser = new JsonSchemaParser();
            _jsonSchemaCollectionsProvider = jSchemaCollectionsProvider;

            _resourceSchemaNodesMap = LoadDefaultSchemas(schemaConfiguration.Value.SchemaCollectionDirectory);
            _logger.LogInformation($"{_resourceSchemaNodesMap.Count} resource default schemas been loaded.");

            if (_customizedSchema)
            {
                var resourceCustomizedSchemaNodesMap = LoadCustomizedSchemas(schemaConfiguration.Value.CustomizedSchemaImageReference);
                _logger.LogInformation($"{resourceCustomizedSchemaNodesMap.Count} resource customized schemas been loaded.");
                _resourceSchemaNodesMap.Concat(resourceCustomizedSchemaNodesMap).ToDictionary(x => x.Key, x => x.Value);
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

        private Dictionary<string, FhirParquetSchemaNode> LoadDefaultSchemas(string schemaDirectoryPath)
        {
            if (!Directory.Exists(schemaDirectoryPath))
            {
                _logger.LogError($"Schema directory \"{schemaDirectoryPath}\" not exist.");
                throw new FhirSchemaException($"Schema directory \"{schemaDirectoryPath}\" not exist.");
            }

            var schemaFiles = Directory.EnumerateFiles(schemaDirectoryPath, "*.json");
            if (schemaFiles.Count() == 0)
            {
                _logger.LogError($"No schema can be found in \"{schemaDirectoryPath}\".");
                throw new FhirSchemaException($"No schema can be found in \"{schemaDirectoryPath}\".");
            }

            var defaultSchemaNodesMap = new Dictionary<string, FhirParquetSchemaNode>();

            foreach (string file in schemaFiles)
            {
                string schemaNodeString;
                FhirParquetSchemaNode schemaNode;
                try
                {
                    schemaNodeString = File.ReadAllText(file);
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Read schema file failed for \"{file}\". Reason: {ex.Message}.");
                    throw new FhirSchemaException($"Read schema file failed for \"{file}\". Reason: {ex.Message}.", ex);
                }

                try
                {
                    schemaNode = JsonSerializer.Deserialize<FhirParquetSchemaNode>(schemaNodeString);
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Parse schema file failed for \"{file}\". Reason: {ex.Message}.");
                    throw new FhirSchemaException($"Parse schema file failed for \"{file}\". Reason: {ex.Message}.", ex);
                }

                if (string.IsNullOrEmpty(schemaNode.Type))
                {
                    _logger.LogError($"Invalid resource type {schemaNode.Type} for schema file \"{file}\".");
                    throw new FhirSchemaException($"Invalid resource type {schemaNode.Type} for schema file \"{file}\".");
                }

                if (defaultSchemaNodesMap.ContainsKey(schemaNode.Type))
                {
                    _logger.LogError($"Find duplicated schema for \"{schemaNode.Type}\" when loading schema file \"{file}\".");
                    throw new FhirSchemaException($"Find duplicated schema for \"{schemaNode.Type}\" when loading schema file \"{file}\".");
                }

                defaultSchemaNodesMap.Add(schemaNode.Type, schemaNode);
            }

            return defaultSchemaNodesMap;
        }

        private Dictionary<string, FhirParquetSchemaNode> LoadCustomizedSchemas(string schemaImageReference)
        {
            var jSchemaCollections = _jsonSchemaCollectionsProvider.GetJsonSchemaCollectionAsync(schemaImageReference, CancellationToken.None).Result;
            _logger.LogInformation($"{jSchemaCollections.Count} resource customized schemas been fetched from {schemaImageReference}.");

            var customizedSchemaNodesMap = new Dictionary<string, FhirParquetSchemaNode>();

            foreach (var jSchemaItem in jSchemaCollections)
            {
                var parquetSchemaNode = _jsonSchemaParser.ParseJSchema(jSchemaItem.Key, jSchemaItem.Value);
                customizedSchemaNodesMap.Add(parquetSchemaNode.Name, parquetSchemaNode);
            }

            return customizedSchemaNodesMap;
        }
    }
}
