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
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider
{
    public class LocalDefaultSchemaProvider : IParquetSchemaProvider
    {
        private const string SchemaR4EmbeddedPrefix = "Schemas.R4";
        private const string SchemaR5EmbeddedPrefix = "Schemas.R5";
        private const string SchemaDicomEmbeddedPrefix = "Schemas.dicom";

        private readonly DataSourceConfiguration _dataSourceConfiguration;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<LocalDefaultSchemaProvider> _logger;

        public LocalDefaultSchemaProvider(
            IOptions<DataSourceConfiguration> dataSourceConfiguration,
            IDiagnosticLogger diagnosticLogger,
            ILogger<LocalDefaultSchemaProvider> logger)
        {
            _dataSourceConfiguration = EnsureArg.IsNotNull(dataSourceConfiguration, nameof(dataSourceConfiguration)).Value;
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
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
                _logger.LogInformation(ex, $"Parse schema file failed. Reason: {ex.Message}.");
                throw new GenerateFhirParquetSchemaNodeException($"Parse schema file failed. Reason: {ex.Message}.", ex);
            }

            return Task.FromResult(defaultSchemaNodesMap);
        }

        private Dictionary<string, string> LoadEmbeddedSchema()
        {
            Dictionary<string, string> embeddedSchema = new Dictionary<string, string>();

            var executingAssembly = Assembly.GetExecutingAssembly();
            string folderName = GetEmbeddedSchemaFolder(executingAssembly);
            string[] resourceNames = executingAssembly
                .GetManifestResourceNames()
                .Where(r => r.StartsWith(folderName) && r.EndsWith(".json"))
                .ToArray();

            foreach (string name in resourceNames)
            {
                using (Stream stream = executingAssembly.GetManifestResourceStream(name))
                using (var reader = new StreamReader(stream))
                {
                    embeddedSchema.Add(name, reader.ReadToEnd());
                }
            }

            return embeddedSchema;
        }

        private string GetEmbeddedSchemaFolder(Assembly assembly)
        {
            switch (_dataSourceConfiguration.Type)
            {
                case DataSourceType.FHIR:
                    var fhirVersion = _dataSourceConfiguration.FhirServer.Version;
                    return fhirVersion switch
                    {
                        FhirVersion.R4 => string.Format("{0}.{1}", assembly.GetName().Name, SchemaR4EmbeddedPrefix),
                        FhirVersion.R5 => string.Format("{0}.{1}", assembly.GetName().Name, SchemaR5EmbeddedPrefix),

                        // Will not happened because we have validated schema version when initialization.
                        _ => throw new GenerateFhirParquetSchemaNodeException($"Fhir schema version {fhirVersion} is not supported.")
                    };
                case DataSourceType.DICOM:
                    return string.Format("{0}.{1}", assembly.GetName().Name, SchemaDicomEmbeddedPrefix);
                default:
                    throw new ConfigurationErrorException($"Data source type {_dataSourceConfiguration.Type} is not supported");
            }
        }
    }
}
