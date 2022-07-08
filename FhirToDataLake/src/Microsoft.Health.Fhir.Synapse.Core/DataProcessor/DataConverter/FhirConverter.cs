// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Liquid.Converter;
using Microsoft.Health.Fhir.Liquid.Converter.Models;
using Microsoft.Health.Fhir.Liquid.Converter.Processors;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter
{
    public class FhirConverter
    {
        private readonly JsonProcessor _jsonProcessor;
        private readonly ITemplateProvider _templateProvider;
        private readonly ILogger<FhirConverter> _logger;

        public FhirConverter(
            IContainerRegistryTemplateProvider containerRegistryTemplateProvider,
            ILogger<FhirConverter> logger)
        {
            var templateCollections = containerRegistryTemplateProvider.GetTemplateCollectionAsync(CancellationToken.None).Result;

            _logger = logger;
            _jsonProcessor = new JsonProcessor(new ProcessorSettings());

            _templateProvider = new TemplateProvider(templateCollections);
        }

        public JsonBatchData Convert(
            JsonBatchData inputData,
            string resourceType,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            IEnumerable<JObject> processedData;
            try
            {
                processedData = inputData.Values.Select(
                    dataObject => JObject.Parse(_jsonProcessor.Convert(dataObject.ToString(), resourceType, _templateProvider)));
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
