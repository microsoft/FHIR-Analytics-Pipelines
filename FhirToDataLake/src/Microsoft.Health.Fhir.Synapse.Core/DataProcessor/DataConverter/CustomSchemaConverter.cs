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
using Microsoft.Health.Fhir.Liquid.Converter.Exceptions;
using Microsoft.Health.Fhir.Liquid.Converter.Models;
using Microsoft.Health.Fhir.Liquid.Converter.Processors;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
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
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<CustomSchemaConverter> _logger;
        private readonly IContainerRegistryTemplateProvider _containerRegistryTemplateProvider;
        private readonly string _schemaImageReference;

        private readonly object _templateProviderLock = new object();
        private ITemplateProvider _templateProvider;

        public CustomSchemaConverter(
            IContainerRegistryTemplateProvider containerRegistryTemplateProvider,
            IOptions<SchemaConfiguration> schemaConfiguration,
            IDiagnosticLogger diagnosticLogger,
            ILogger<CustomSchemaConverter> logger)
        {
            EnsureArg.IsNotNull(schemaConfiguration, nameof(schemaConfiguration));

            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
            _containerRegistryTemplateProvider = EnsureArg.IsNotNull(containerRegistryTemplateProvider, nameof(containerRegistryTemplateProvider));
            _schemaImageReference = schemaConfiguration.Value.SchemaImageReference;

            _jsonProcessor = new JsonProcessor(new ProcessorSettings());
        }

        private ITemplateProvider TemplateProvider
        {
            get
            {
                // Do the lazy initialization.
                if (_templateProvider is null)
                {
                    lock (_templateProviderLock)
                    {
                        _templateProvider ??= new TemplateProvider(_containerRegistryTemplateProvider.GetTemplateCollectionAsync(
                            _schemaImageReference,
                            CancellationToken.None).Result);
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
                _diagnosticLogger.LogError($"Schema image reference is empty or null.");
                _logger.LogInformation($"Schema image reference is empty or null.");
                throw new ParquetDataProcessorException($"Schema image reference is empty or null.");
            }

            List<JObject> processedData;
            try
            {
                processedData = inputData.Values.Select(dataObject
                    => JObject.Parse(_jsonProcessor.Convert(dataObject, resourceType, TemplateProvider))).ToList();
            }
            catch (FhirConverterException convertException)
            {
                if (convertException.FhirConverterErrorCode == FhirConverterErrorCode.TimeoutError)
                {
                    _diagnosticLogger.LogError("Convert data operation timed out.");
                    _logger.LogInformation(convertException.InnerException, "Convert data operation timed out.");
                    throw new ParquetDataProcessorException($"Convert customized data for {resourceType} failed.", convertException);
                }

                _diagnosticLogger.LogError("Convert data failed.");
                _logger.LogInformation(convertException, "Convert data failed.");
                throw new ParquetDataProcessorException($"Convert customized data for {resourceType} failed.", convertException);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"Unhandled exception: Convert customized data for {resourceType} failed.");
                _logger.LogError(ex, "Unhandled exception: convert data process failed.");
                throw;
            }

            return new JsonBatchData(processedData);
        }
    }
}
