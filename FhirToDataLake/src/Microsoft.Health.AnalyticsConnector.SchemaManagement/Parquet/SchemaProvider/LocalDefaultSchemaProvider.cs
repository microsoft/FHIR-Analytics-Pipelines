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
using Microsoft.Health.AnalyticsConnector.Common;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Exceptions;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Exceptions;
using Newtonsoft.Json;

namespace Microsoft.Health.AnalyticsConnector.SchemaManagement.Parquet.SchemaProvider
{
    public class LocalDefaultSchemaProvider : IParquetSchemaProvider
    {
        private const string SchemaR4EmbeddedPrefix = "Schemas.R4";
        private const string SchemaR5EmbeddedPrefix = "Schemas.R5";
        private const string SchemaDicomEmbeddedPrefix = "Schemas.Dicom";

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

        public Task<Dictionary<string, ParquetSchemaNode>> GetSchemasAsync(CancellationToken cancellationToken = default)
        {
            Dictionary<string, string> embeddedSchemas = LoadEmbeddedSchema();
            Dictionary<string, ParquetSchemaNode> defaultSchemaNodesMap;
            try
            {
                defaultSchemaNodesMap = embeddedSchemas.Select(schemaItem =>
                {
                    var schemaNode = JsonConvert.DeserializeObject<ParquetSchemaNode>(schemaItem.Value);
                    return new KeyValuePair<string, ParquetSchemaNode>(schemaNode.Type, schemaNode);
                }).ToDictionary(x => x.Key, x => x.Value);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"Parse schema file failed. Reason: {ex.Message}.");
                _logger.LogInformation(ex, $"Parse schema file failed. Reason: {ex.Message}.");
                throw new GenerateFhirParquetSchemaNodeException($"Parse schema file failed. Reason: {ex.Message}.", ex);
            }

            if (_dataSourceConfiguration.Type == DataSourceType.FhirDataLakeStore)
            {
                foreach ((var schemanNodeType, var schemaNode) in defaultSchemaNodesMap)
                {
                    schemaNode.SubNodes.Add(DataLakeConstants.BlobNameColumnKey, CreateBlobSchemaNode(schemanNodeType, DataLakeConstants.BlobNameColumnKey));
                    schemaNode.SubNodes.Add(DataLakeConstants.ETagColumnKey, CreateBlobSchemaNode(schemanNodeType, DataLakeConstants.ETagColumnKey));
                    schemaNode.SubNodes.Add(DataLakeConstants.IndexColumnKey, CreateBlobSchemaNode(schemanNodeType, DataLakeConstants.IndexColumnKey));
                }
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
                case DataSourceType.FhirDataLakeStore:
                    fhirVersion = _dataSourceConfiguration.FhirDataLakeStore.Version;
                    return fhirVersion switch
                    {
                        FhirVersion.R4 => string.Format("{0}.{1}", assembly.GetName().Name, SchemaR4EmbeddedPrefix),
                        FhirVersion.R5 => string.Format("{0}.{1}", assembly.GetName().Name, SchemaR5EmbeddedPrefix),

                        // Will not happened because we have validated schema version when initialization.
                        _ => throw new GenerateFhirParquetSchemaNodeException($"Fhir schema version {fhirVersion} is not supported.")
                    };
                default:
                    throw new ConfigurationErrorException($"Data source type {_dataSourceConfiguration.Type} is not supported");
            }
        }

        private static ParquetSchemaNode CreateBlobSchemaNode(string schemaNodeType, string schemaNodeName)
        {
            return new ParquetSchemaNode
            {
                Name = schemaNodeName,
                Type = schemaNodeName.Equals(DataLakeConstants.IndexColumnKey) ? "integer" : "string",
                IsRepeated = false,
                IsLeaf = true,
                NodePaths = new List<string> { schemaNodeType, schemaNodeName },
                SubNodes = null,
                ChoiceTypeNodes = null,
            };
        }
    }
}
