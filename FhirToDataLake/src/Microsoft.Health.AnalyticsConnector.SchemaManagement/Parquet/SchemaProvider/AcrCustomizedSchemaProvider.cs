// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DotLiquid;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Liquid.Converter.Models.Json;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.ContainerRegistry;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Exceptions;
using NJsonSchema;

namespace Microsoft.Health.AnalyticsConnector.SchemaManagement.Parquet.SchemaProvider
{
    public class AcrCustomizedSchemaProvider : IParquetSchemaProvider
    {
        private readonly IContainerRegistryTemplateProvider _containerRegistryTemplateProvider;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<AcrCustomizedSchemaProvider> _logger;
        private readonly string _schemaImageReference;

        public AcrCustomizedSchemaProvider(
            IContainerRegistryTemplateProvider containerRegistryTemplateProvider,
            IOptions<SchemaConfiguration> schemaConfiguration,
            IDiagnosticLogger diagnosticLogger,
            ILogger<AcrCustomizedSchemaProvider> logger)
        {
            EnsureArg.IsNotNull(schemaConfiguration, nameof(schemaConfiguration));

            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _containerRegistryTemplateProvider = EnsureArg.IsNotNull(containerRegistryTemplateProvider, nameof(containerRegistryTemplateProvider));
            _schemaImageReference = schemaConfiguration.Value.SchemaImageReference;
        }

        public async Task<Dictionary<string, ParquetSchemaNode>> GetSchemasAsync(CancellationToken cancellationToken = default)
        {
            Dictionary<string, JsonSchema> jsonSchemaCollection = await GetJsonSchemaCollectionAsync(cancellationToken);

            return jsonSchemaCollection
                .Select(x => JsonSchemaParser.ParseJSchema(x.Key, x.Value))
                .ToDictionary(x => GetCustomizedSchemaType(x.Type), x => x);
        }

        private async Task<Dictionary<string, JsonSchema>> GetJsonSchemaCollectionAsync(CancellationToken cancellationToken)
        {
            Dictionary<string, JsonSchema> result = new Dictionary<string, JsonSchema>();

            if (string.IsNullOrWhiteSpace(_schemaImageReference))
            {
                _diagnosticLogger.LogError("Schema image reference is null or empty.");
                _logger.LogInformation("Schema image reference is null or empty.");
                throw new ContainerRegistrySchemaException("Schema image reference is null or empty.");
            }

            List<Dictionary<string, Template>> templateCollections = await _containerRegistryTemplateProvider.GetTemplateCollectionAsync(_schemaImageReference, cancellationToken);

            // Fetch all files with suffix ".schema.json" as Json schema template.
            // All Json schema files should be in "Schema" directory under the root path of the image.
            foreach (Dictionary<string, Template> templates in templateCollections)
            {
                foreach (KeyValuePair<string, Template> templateItem in templates)
                {
                    string[] templatePathSegments = templateItem.Key.Split('/');

                    if (!string.Equals(templatePathSegments[0], ParquetSchemaConstants.JsonSchemaTemplateDirectory, StringComparison.InvariantCultureIgnoreCase))
                    {
                        continue;
                    }

                    if (templatePathSegments.Length != 2)
                    {
                        _diagnosticLogger.LogError($"All Json schema should be directly in \"Schema\" directory.");
                        _logger.LogInformation($"All Json schema should be directly in \"Schema\" directory.");
                        throw new ContainerRegistrySchemaException($"All Json schema should be directly in \"Schema\" directory.");
                    }

                    if (!templatePathSegments[1].EndsWith(ParquetSchemaConstants.JsonSchemaTemplateFileExtension, StringComparison.InvariantCultureIgnoreCase))
                    {
                        _logger.LogInformation($"{templatePathSegments[1]} doesn't have {ParquetSchemaConstants.JsonSchemaTemplateFileExtension} extension in \"Schema\" directory.");
                    }
                    else
                    {
                        // The customized schema keys are like "Patient_Customized", "Observation_Customized"...
                        string resourceType = templatePathSegments[1].Substring(0, templatePathSegments[1].Length - ParquetSchemaConstants.JsonSchemaTemplateFileExtension.Length);

                        if (!(templateItem.Value.Root is JSchemaDocument customizedSchemaDocument) || customizedSchemaDocument.Schema == null)
                        {
                            _diagnosticLogger.LogError($"Invalid Json schema template {templateItem.Key}, no JSchema content be found.");
                            _logger.LogInformation($"Invalid Json schema template {templateItem.Key}, no JSchema content be found.");
                            throw new ContainerRegistrySchemaException($"Invalid Json schema template {templateItem.Key}, no JSchema content be found.");
                        }

                        result.Add(resourceType, customizedSchemaDocument.Schema);
                    }
                }
            }

            return result;
        }

        private string GetCustomizedSchemaType(string resourceType)
        {
            return $"{resourceType}{ParquetSchemaConstants.CustomizedSchemaSuffix}";
        }
    }
}
