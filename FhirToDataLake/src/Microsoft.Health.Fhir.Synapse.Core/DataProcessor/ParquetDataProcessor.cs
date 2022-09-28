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
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Microsoft.Health.Parquet;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.DataProcessor
{
    public sealed class ParquetDataProcessor : IColumnDataProcessor
    {
        private readonly ArrowConfiguration _arrowConfiguration;
        private readonly ILogger<ParquetDataProcessor> _logger;
        private readonly ParquetConverter _parquetConverter;
        private readonly IDataSchemaConverter _defaultSchemaConverter;
        private readonly IDataSchemaConverter _customSchemaConverter;
        private readonly IDiagnosticLogger _diagnosticLogger;

        public ParquetDataProcessor(
            IFhirSchemaManager<FhirParquetSchemaNode> fhirSchemaManager,
            IOptions<ArrowConfiguration> arrowConfiguration,
            DataSchemaConverterDelegate schemaConverterDelegate,
            IDiagnosticLogger diagnosticLogger,
            ILogger<ParquetDataProcessor> logger)
        {
            EnsureArg.IsNotNull(fhirSchemaManager, nameof(fhirSchemaManager));
            EnsureArg.IsNotNull(arrowConfiguration, nameof(arrowConfiguration));
            EnsureArg.IsNotNull(schemaConverterDelegate, nameof(schemaConverterDelegate));
            EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _arrowConfiguration = arrowConfiguration.Value;
            _defaultSchemaConverter = schemaConverterDelegate(FhirParquetSchemaConstants.DefaultSchemaProviderKey);
            _customSchemaConverter = schemaConverterDelegate(FhirParquetSchemaConstants.CustomSchemaProviderKey);
            _diagnosticLogger = diagnosticLogger;
            _logger = logger;

            var schemaSet = fhirSchemaManager.GetAllSchemaContent();
            _parquetConverter = ParquetConverter.CreateWithSchemaSet(schemaSet);

            _diagnosticLogger.LogInformation($"ParquetDataProcessor initialized successfully with {schemaSet.Count()} parquet schemas.");
            _logger.LogInformation($"ParquetDataProcessor initialized successfully with {schemaSet.Count()} parquet schemas.");
        }

        public Task<StreamBatchData> ProcessAsync(
            JsonBatchData inputData,
            ProcessParameters processParameters,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Convert data based on schema
            JsonBatchData processedData;

            // Currently the default schema type of each resource type is themselves
            if (string.Equals(processParameters.SchemaType, processParameters.ResourceType, StringComparison.InvariantCulture))
            {
                processedData = _defaultSchemaConverter.Convert(inputData, processParameters.SchemaType, cancellationToken);
            }
            else
            {
                processedData = _customSchemaConverter.Convert(inputData, processParameters.ResourceType, cancellationToken);
            }

            var inputContent = string.Join(
                Environment.NewLine,
                processedData.Values.Select(jsonObject => jsonObject.ToString(Formatting.None))
                         .Where(result => CheckBlockSize(processParameters.SchemaType, result)));
            if (string.IsNullOrEmpty(inputContent))
            {
                // Return null if no data has been converted.
                return Task.FromResult<StreamBatchData>(null);
            }

            // Convert JSON data to parquet stream.
            try
            {
                var resultStream = _parquetConverter.ConvertJsonToParquet(processParameters.SchemaType, inputContent);
                return Task.FromResult(
                    new StreamBatchData(
                        resultStream,
                        processedData.Values.Count(),
                        processParameters.SchemaType));
            }
            catch (ParquetException parquetEx)
            {
                _diagnosticLogger.LogError($"Exception happened when converting input data to parquet for \"{processParameters.SchemaType}\".");
                _logger.LogInformation(parquetEx, $"Exception happened when converting input data to parquet for \"{processParameters.SchemaType}\".");
                throw new ParquetDataProcessorException($"Exception happened when converting input data to parquet for \"{processParameters.SchemaType}\".", parquetEx);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"Unhandeled exception when converting input data to parquet for \"{processParameters.SchemaType}\".");
                _logger.LogError($"Unhandeled exception when converting input data to parquet for \"{processParameters.SchemaType}\".");
                throw new ParquetDataProcessorException($"Unhandeled exception when converting input data to parquet for \"{processParameters.SchemaType}\".", ex);
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
                _diagnosticLogger.LogWarning($"Single data length of {schemaType} is larger than BlockSize {_arrowConfiguration.ReadOptions.BlockSize}, will be ignored when converting to parquet.");
                _logger.LogInformation($"Single data length of {schemaType} is larger than BlockSize {_arrowConfiguration.ReadOptions.BlockSize}, will be ignored when converting to parquet.");
                return false;
            }

            // If length of actual data is closing to BlockSize in configuration, log a warning, still return data in string.
            // Temporarily use 1/3 as the a threshold to give the warning message.
            if (data.Length * 3 > _arrowConfiguration.ReadOptions.BlockSize)
            {
                _diagnosticLogger.LogWarning($"Single data length of {schemaType} is closing to BlockSize {_arrowConfiguration.ReadOptions.BlockSize}.");
                _logger.LogInformation($"Single data length of {schemaType} is closing to BlockSize {_arrowConfiguration.ReadOptions.BlockSize}.");
            }

            return true;
        }
    }
}
