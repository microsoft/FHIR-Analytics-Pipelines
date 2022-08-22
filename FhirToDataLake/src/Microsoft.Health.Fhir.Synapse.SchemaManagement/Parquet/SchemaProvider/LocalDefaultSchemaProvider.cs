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
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider
{
    public class LocalDefaultSchemaProvider : IParquetSchemaProvider
    {
        private const string SchemaR4EmbeddedPrefix = "Schemas.R4";
        private const string SchemaR5EmbeddedPrefix = "Schemas.R5";

        private readonly ILogger<LocalDefaultSchemaProvider> _logger;

        public LocalDefaultSchemaProvider(ILogger<LocalDefaultSchemaProvider> logger)
        {
            _logger = logger;
        }

        public Task<Dictionary<string, FhirParquetSchemaNode>> GetSchemasAsync(FhirVersion fhirVersion, CancellationToken cancellationToken = default)
        {
            Dictionary<string, string> embeddedSchemas = LoadEmbeddedSchema(fhirVersion);
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
                _logger.LogError($"Parse schema file failed. Reason: {ex.Message}.");
                throw new GenerateFhirParquetSchemaNodeException($"Parse schema file failed. Reason: {ex.Message}.", ex);
            }

            return Task.FromResult(defaultSchemaNodesMap);
        }

        private Dictionary<string, string> LoadEmbeddedSchema(FhirVersion fhirVersion)
        {
            Dictionary<string, string> embeddedSchema = new Dictionary<string, string>();

            var executingAssembly = Assembly.GetExecutingAssembly();
            string folderName = GetEmbeddedSchemaFolder(executingAssembly, fhirVersion);
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

        private string GetEmbeddedSchemaFolder(Assembly assembly, FhirVersion fhirVersion)
        {
            return fhirVersion switch
            {
                FhirVersion.R4 => string.Format("{0}.{1}", assembly.GetName().Name, SchemaR4EmbeddedPrefix),
                FhirVersion.R5 => string.Format("{0}.{1}", assembly.GetName().Name, SchemaR5EmbeddedPrefix),

                // Will not happened because we have validated schema version when initialization.
                _ => throw new GenerateFhirParquetSchemaNodeException($"Fhir Schema Version {fhirVersion} is not supported.")
            };
        }
    }
}
