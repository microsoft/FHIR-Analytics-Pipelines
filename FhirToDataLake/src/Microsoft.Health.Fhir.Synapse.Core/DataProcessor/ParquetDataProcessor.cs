// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations.Arrow;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Parquet.CLR;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.DataProcessor
{
    public sealed class ParquetDataProcessor : IColumnDataProcessor
    {
        private readonly IFhirSchemaManager<FhirParquetSchemaNode> _fhirSchemaManager;
        private readonly ArrowConfiguration _arrowConfiguration;
        private readonly ILogger<ParquetDataProcessor> _logger;
        private readonly ParquetConverterWrapper _parquetConverterWrapper;
        private readonly DefaultConverter _defaultDataConverter;
        private readonly FhirConverter _fhirDataConverter;

        public ParquetDataProcessor(
            IFhirSchemaManager<FhirParquetSchemaNode> fhirSchemaManager,
            IOptions<ArrowConfiguration> arrowConfiguration,
            DefaultConverter defaultConverter,
            FhirConverter fhirConverter,
            ILogger<ParquetDataProcessor> logger)
        {
            EnsureArg.IsNotNull(fhirSchemaManager, nameof(fhirSchemaManager));
            EnsureArg.IsNotNull(arrowConfiguration, nameof(arrowConfiguration));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _fhirSchemaManager = fhirSchemaManager;
            _arrowConfiguration = arrowConfiguration.Value;
            _defaultDataConverter = defaultConverter;
            _fhirDataConverter = fhirConverter;
            _logger = logger;

            try
            {
                _parquetConverterWrapper = new ParquetConverterWrapper(_fhirSchemaManager.GetAllSchemas(), arrowConfiguration.Value);
            }
            catch (Exception ex)
            {
                _logger.LogError("Create ParquetConverterWrapper failed.");
                throw new ParquetDataProcessorException("Create ParquetConverterWrapper failed. " + ex.Message, ex);
            }

            _logger.LogInformation($"ParquetDataProcessor initialized successfully with ArrowConfiguration: {JsonConvert.SerializeObject(arrowConfiguration.Value)}.");
        }

        public Task<StreamBatchData> ProcessAsync(
            JsonBatchData inputData,
            ProcessParameters processParameters,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Get FHIR schema for the input data.
            var schema = _fhirSchemaManager.GetSchema(processParameters.SchemaType);

            if (schema == null)
            {
                _logger.LogError($"The FHIR schema node could not be found for schema type '{processParameters.SchemaType}'.");
                throw new ParquetDataProcessorException($"The FHIR schema node could not be found for schema type '{processParameters.SchemaType}'.");
            }

            // Convert data based on schema
            JsonBatchData processedData;
            if (IsCustomizedSchemaType(processParameters.SchemaType))
            {
                processedData = _fhirDataConverter.Convert(inputData, processParameters.ResourceType, cancellationToken);
            }
            else
            {
                processedData = _defaultDataConverter.Convert(inputData, schema, cancellationToken);
            }

            var inputStream = TransformJsonDataToStream(processParameters.SchemaType, processedData.Values);
            if (inputStream == null)
            {
                // Return null if no data has been converted.
                return Task.FromResult<StreamBatchData>(null);
            }

            // Convert JSON data to parquet stream.
            try
            {
                var resultStream = _parquetConverterWrapper.ConvertToParquetStream(processParameters.SchemaType, inputStream);
                return Task.FromResult(
                    new StreamBatchData(
                        resultStream,
                        processedData.Values.Count(),
                        processParameters.SchemaType));
            }
            catch (Exception ex)
            {
                _logger.LogError($"Exception happened when converting input data to parquet for \"{processParameters.SchemaType}\".");
                throw new ParquetDataProcessorException($"Exception happened when converting input data to parquet for \"{processParameters.SchemaType}\".", ex);
            }
        }

        private MemoryStream TransformJsonDataToStream(string schemaType, IEnumerable<JObject> inputData)
        {
            var content = string.Join(
                Environment.NewLine,
                inputData.Select(jsonObject => jsonObject.ToString(Formatting.None))
                         .Where(result => CheckBlockSize(schemaType, result)));

            // If no content been fetched, E.g. input data is empty or all FHIR data are larger than block size, will return null.
            return string.IsNullOrEmpty(content) ? null : new MemoryStream(Encoding.UTF8.GetBytes(content));
        }

        private bool CheckBlockSize(string schemaType, string data)
        {
            // If length of actual data is larger than BlockSize in configuration, log a warning and ignore that data, return an empty JSON string.
            // TODO: Confirm the BlockSize handle logic in arrow.lib.
            if (data.Length > _arrowConfiguration.ReadOptions.BlockSize)
            {
                _logger.LogWarning($"Single data length of {schemaType} is larger than BlockSize {_arrowConfiguration.ReadOptions.BlockSize}, will be ignored when converting to parquet.");
                return false;
            }

            // If length of actual data is closing to BlockSize in configuration, log a warning, still return data in string.
            // Temporarily use 1/3 as the a threshold to give the warning message.
            if (data.Length * 3 > _arrowConfiguration.ReadOptions.BlockSize)
            {
                _logger.LogWarning($"Single data length of {schemaType} is closing to BlockSize {_arrowConfiguration.ReadOptions.BlockSize}.");
            }

            return true;
        }

        private bool IsCustomizedSchemaType(string schemaType)
        {
            return schemaType.EndsWith(FhirParquetSchemaConstants.CustomizedSchemaSuffix);
        }
    }
}
