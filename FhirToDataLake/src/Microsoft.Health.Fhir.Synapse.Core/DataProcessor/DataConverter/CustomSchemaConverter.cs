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
        private readonly ILogger<CustomSchemaConverter> _logger;
        private readonly IContainerRegistryTemplateProvider _containerRegistryTemplateProvider;
        private readonly string _schemaImageReference;

        private readonly object _templateProviderLock = new object();
        private ITemplateProvider _templateProvider;

        public CustomSchemaConverter(
            IContainerRegistryTemplateProvider containerRegistryTemplateProvider,
            IOptions<SchemaConfiguration> schemaConfiguration,
            ILogger<CustomSchemaConverter> logger)
        {
            EnsureArg.IsNotNull(schemaConfiguration, nameof(schemaConfiguration));

            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
            _containerRegistryTemplateProvider = EnsureArg.IsNotNull(containerRegistryTemplateProvider, nameof(containerRegistryTemplateProvider));
            _schemaImageReference = schemaConfiguration.Value.SchemaImageReference;

            _jsonProcessor = new JsonProcessor(new ProcessorSettings());
        }

        private ITemplateProvider TemplateProvider
        {
            get
            {
                if (_templateProvider is null)
                {
                    lock (_templateProviderLock)
                    {
                        var templateCollections = _containerRegistryTemplateProvider.GetTemplateCollectionAsync(
                             _schemaImageReference,
                             CancellationToken.None).Result;

                        _templateProvider = new TemplateProvider(templateCollections);
                    }
                }

                return _templateProvider;
            }
            set => _templateProvider = value;
        }

        public JsonBatchData Convert(
            JsonBatchData inputData,
            string resourceType,
            CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(inputData?.Values);
            EnsureArg.IsNotNullOrWhiteSpace(resourceType);

            cancellationToken.ThrowIfCancellationRequested();

            if (string.IsNullOrWhiteSpace(_schemaImageReference))
            {
                _logger.LogError($"Schema image reference is empty or null.");
                throw new ParquetDataProcessorException($"Schema image reference is empty or null.");
            }

            List<JObject> processedData;
            try
            {
                // TODO: Update FHIR-Converter, add an interface to directly convert an Object.
                processedData = inputData.Values.Select(dataObject
                    => JObject.Parse(_jsonProcessor.Convert(dataObject.ToString(), resourceType, TemplateProvider))).ToList();
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
