// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider
{
    public class LocalDefaultSchemaProvider : IParquetSchemaProvider
    {
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<LocalDefaultSchemaProvider> _logger;

        public LocalDefaultSchemaProvider(IDiagnosticLogger diagnosticLogger, ILogger<LocalDefaultSchemaProvider> logger)
        {
            _diagnosticLogger = diagnosticLogger;
            _logger = logger;
        }

        public Task<Dictionary<string, FhirParquetSchemaNode>> GetSchemasAsync(CancellationToken cancellationToken = default)
        {
            Dictionary<string, string> embeddedSchemas = LoadEmbeddedSchema();
            Dictionary<string, FhirParquetSchemaNode> defaultSchemaNodesMap;
            try
            {
                defaultSchemaNodesMap = embeddedSchemas.Select(schemaItem =>
                {
                    var schemaNode = JsonConvert.DeserializeObject<FhirParquetSchemaNode>(schemaItem.Value);
                    return new KeyValuePair<string, FhirParquetSchemaNode>(schemaNode.Type, schemaNode);
                }).ToDictionary(x => x.Key, x => x.Value);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"Parse schema file failed. Reason: {ex.Message}.");
                _logger.LogInformation($"Parse schema file failed. Reason: {ex.Message}.");
                throw new GenerateFhirParquetSchemaNodeException($"Parse schema file failed. Reason: {ex.Message}.", ex);
            }

            return Task.FromResult(defaultSchemaNodesMap);
        }

        private Dictionary<string, string> LoadEmbeddedSchema()
        {
            Dictionary<string, string> embeddedSchema = new Dictionary<string, string>();
            var executingAssembly = Assembly.GetExecutingAssembly();
            string folderName = string.Format("{0}.Schemas.R4", executingAssembly.GetName().Name);
            var resourceNames = executingAssembly
                .GetManifestResourceNames()
                .Where(r => r.StartsWith(folderName) && r.EndsWith(".json"))
                .ToArray();
            foreach (var name in resourceNames)
            {
                using (Stream stream = executingAssembly.GetManifestResourceStream(name))
                using (StreamReader reader = new StreamReader(stream))
                {
                    embeddedSchema.Add(name, reader.ReadToEnd());
                }
            }

            return embeddedSchema;
        }
    }
}
