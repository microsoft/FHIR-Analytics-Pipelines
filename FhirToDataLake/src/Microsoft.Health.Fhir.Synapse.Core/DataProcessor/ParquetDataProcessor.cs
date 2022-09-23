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
        private readonly IDataSchemaConverter _defaultSchemaConverter;
        private readonly IDataSchemaConverter _customSchemaConverter;
        private readonly IFhirSchemaManager<FhirParquetSchemaNode> _fhirSchemaManager;

        private readonly object _parquetConverterLock = new object();
        private ParquetConverter _parquetConverter;

        public ParquetDataProcessor(
            IFhirSchemaManager<FhirParquetSchemaNode> fhirSchemaManager,
            IOptions<ArrowConfiguration> arrowConfiguration,
            DataSchemaConverterDelegate schemaConverterDelegate,
            ILogger<ParquetDataProcessor> logger)
        {
            EnsureArg.IsNotNull(fhirSchemaManager, nameof(fhirSchemaManager));
            EnsureArg.IsNotNull(arrowConfiguration, nameof(arrowConfiguration));
            EnsureArg.IsNotNull(schemaConverterDelegate, nameof(schemaConverterDelegate));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _arrowConfiguration = arrowConfiguration.Value;
            _defaultSchemaConverter = schemaConverterDelegate(FhirParquetSchemaConstants.DefaultSchemaProviderKey);
            _customSchemaConverter = schemaConverterDelegate(FhirParquetSchemaConstants.CustomSchemaProviderKey);
            _fhirSchemaManager = fhirSchemaManager;
            _logger = logger;
        }

        private ParquetConverter ParquetConverter
        {
            get
            {
                // Do the lazy initialization.
                if (_parquetConverter is null)
                {
                    lock (_parquetConverterLock)
                    {
                        // Check null again to avoid duplicate initialization.
                        if (_parquetConverter is null)
                        {
                            var schemaSet = _fhirSchemaManager.GetAllSchemaContent();
                            _parquetConverter = ParquetConverter.CreateWithSchemaSet(schemaSet);
                            _logger.LogInformation($"ParquetDataProcessor initialized successfully with {schemaSet.Count()} parquet schemas.");
                        }
                    }
                }

                return _parquetConverter;
            }
            set => _parquetConverter = value;
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
                var resultStream = ParquetConverter.ConvertJsonToParquet(processParameters.SchemaType, inputContent);
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
    }
}
