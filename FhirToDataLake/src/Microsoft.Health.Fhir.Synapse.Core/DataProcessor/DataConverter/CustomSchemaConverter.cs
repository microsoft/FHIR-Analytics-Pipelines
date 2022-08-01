// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Liquid.Converter;
using Microsoft.Health.Fhir.Liquid.Converter.Models;
using Microsoft.Health.Fhir.Liquid.Converter.Processors;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter
{
    /// <summary>
    /// Leverage the FHIR-Converter (https://github.com/microsoft/FHIR-Converter) to convert data to target structure.
    /// </summary>
    public class CustomSchemaConverter : IDataSchemaConverter
    {
        private readonly JsonProcessor _jsonProcessor;
        private readonly ITemplateProvider _templateProvider;
        private readonly ILogger<CustomSchemaConverter> _logger;

        public CustomSchemaConverter(
            IContainerRegistryTemplateProvider containerRegistryTemplateProvider,
            IOptions<SchemaConfiguration> schemaConfiguration,
            ILogger<CustomSchemaConverter> logger)
        {
            _logger = logger;
            if (!string.IsNullOrWhiteSpace(schemaConfiguration.Value.SchemaImageReference))
            {
                var templateCollections = containerRegistryTemplateProvider.GetTemplateCollectionAsync(
                    schemaConfiguration.Value.SchemaImageReference,
                    CancellationToken.None).Result;

                _jsonProcessor = new JsonProcessor(new ProcessorSettings());
                _templateProvider = new TemplateProvider(templateCollections);
            }
        }

        public JsonBatchData Convert(
            JsonBatchData inputData,
            string resourceType,
            CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(inputData?.Values);
            EnsureArg.IsNotNullOrWhiteSpace(resourceType);

            cancellationToken.ThrowIfCancellationRequested();

            if (_templateProvider == null)
            {
                _logger.LogError($"No valid template provider be found, maybe the schema image reference is empty or null.");
                throw new ParquetDataProcessorException($"No valid template provider be found, maybe the schema image reference is empty or null.");
            }

            List<JObject> processedData;
            try
            {
                // TODO: Update FHIR-Converter, add an interface to directly convert an Object.
                processedData = inputData.Values.Select(dataObject
                    => JObject.Parse(_jsonProcessor.Convert(dataObject.ToString(), resourceType, _templateProvider))).ToList();
            }
            catch (Exception ex)
            {
                _logger.LogError($"Convert customized data for {resourceType} failed. " + ex.Message);
                throw new ParquetDataProcessorException($"Convert customized data for {resourceType} failed.", ex);
            }

            return new JsonBatchData(processedData);
        }
    }
}
