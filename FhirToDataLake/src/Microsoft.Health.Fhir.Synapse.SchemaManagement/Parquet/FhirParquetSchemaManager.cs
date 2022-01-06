// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet
{
    public class FhirParquetSchemaManager : IFhirSchemaManager<FhirParquetSchemaNode>
    {
        private readonly Dictionary<string, FhirParquetSchemaNode> _resourceSchemaNodesMap;
        private readonly ILogger<FhirParquetSchemaManager> _logger;

        public FhirParquetSchemaManager(
            IOptions<SchemaConfiguration> schemaConfiguration,
            ILogger<FhirParquetSchemaManager> logger)
        {
            _logger = logger;

            _resourceSchemaNodesMap = LoadSchemas(schemaConfiguration.Value.SchemaCollectionDirectory);
            _logger.LogInformation($"Initialize FHIR schemas completed, {_resourceSchemaNodesMap.Count} resource schemas been loaded.");
        }

        public FhirParquetSchemaNode GetSchema(string resourceType)
        {
            if (!_resourceSchemaNodesMap.ContainsKey(resourceType))
            {
                _logger.LogError($"Schema for resource type {resourceType} is not supported.");
                return null;
            }

            return _resourceSchemaNodesMap[resourceType];
        }

        public Dictionary<string, FhirParquetSchemaNode> GetAllSchemas()
        {
            return new Dictionary<string, FhirParquetSchemaNode>(_resourceSchemaNodesMap);
        }

        private Dictionary<string, FhirParquetSchemaNode> LoadSchemas(string schemaDirectoryPath)
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

            var schemaNodesMap = new Dictionary<string, FhirParquetSchemaNode>();

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

                if (schemaNodesMap.ContainsKey(schemaNode.Type))
                {
                    _logger.LogError($"Find duplicated schema for \"{schemaNode.Type}\" when loading schema file \"{file}\".");
                    throw new FhirSchemaException($"Find duplicated schema for \"{schemaNode.Type}\" when loading schema file \"{file}\".");
                }

                schemaNodesMap.Add(schemaNode.Type, schemaNode);
            }

            return schemaNodesMap;
        }
    }
}
