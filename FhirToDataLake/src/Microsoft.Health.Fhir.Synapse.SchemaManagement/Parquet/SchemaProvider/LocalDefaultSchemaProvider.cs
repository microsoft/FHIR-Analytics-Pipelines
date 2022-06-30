// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider
{
    public class LocalDefaultSchemaProvider : IParquetSchemaProvider
    {
        private readonly ILogger<LocalDefaultSchemaProvider> _logger;

        public LocalDefaultSchemaProvider(ILogger<LocalDefaultSchemaProvider> logger)
        {
            _logger = logger;
        }

        public Task<Dictionary<string, FhirParquetSchemaNode>> GetSchemasAsync(string schemaDirectoryPath, CancellationToken cancellationToken = default)
        {
            if (!Directory.Exists(schemaDirectoryPath))
            {
                _logger.LogError($"Schema directory \"{schemaDirectoryPath}\" not exist.");
                throw new GenerateFhirParquetSchemaNodeException($"Schema directory \"{schemaDirectoryPath}\" not exist.");
            }

            var schemaFiles = Directory.EnumerateFiles(schemaDirectoryPath, "*.json");
            if (schemaFiles.Count() == 0)
            {
                _logger.LogError($"No schema can be found in \"{schemaDirectoryPath}\".");
                throw new GenerateFhirParquetSchemaNodeException($"No schema can be found in \"{schemaDirectoryPath}\".");
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
                    throw new GenerateFhirParquetSchemaNodeException($"Read schema file failed for \"{file}\". Reason: {ex.Message}.", ex);
                }

                try
                {
                    schemaNode = JsonConvert.DeserializeObject<FhirParquetSchemaNode>(schemaNodeString);
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Parse schema file failed for \"{file}\". Reason: {ex.Message}.");
                    throw new GenerateFhirParquetSchemaNodeException($"Parse schema file failed for \"{file}\". Reason: {ex.Message}.", ex);
                }

                if (string.IsNullOrEmpty(schemaNode.Type))
                {
                    _logger.LogError($"Invalid resource type {schemaNode.Type} for schema file \"{file}\".");
                    throw new GenerateFhirParquetSchemaNodeException($"Invalid resource type {schemaNode.Type} for schema file \"{file}\".");
                }

                if (defaultSchemaNodesMap.ContainsKey(schemaNode.Type))
                {
                    _logger.LogError($"Find duplicated schema for \"{schemaNode.Type}\" when loading schema file \"{file}\".");
                    throw new GenerateFhirParquetSchemaNodeException($"Find duplicated schema for \"{schemaNode.Type}\" when loading schema file \"{file}\".");
                }

                defaultSchemaNodesMap.Add(schemaNode.Type, schemaNode);
            }

            return Task.FromResult(defaultSchemaNodesMap);
        }
    }
}
