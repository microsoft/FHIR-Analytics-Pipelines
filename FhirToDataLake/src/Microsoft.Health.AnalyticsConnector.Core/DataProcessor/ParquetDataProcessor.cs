// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Models.Data;
using Microsoft.Health.AnalyticsConnector.Core.DataProcessor.DataConverter;
using Microsoft.Health.AnalyticsConnector.Core.Exceptions;
using Microsoft.Health.AnalyticsConnector.Core.Fhir;
using Microsoft.Health.AnalyticsConnector.SchemaManagement;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Parquet;
using Microsoft.Health.Parquet;
using Newtonsoft.Json;

namespace Microsoft.Health.AnalyticsConnector.Core.DataProcessor
{
    public sealed class ParquetDataProcessor : IColumnDataProcessor
    {
        private const int LargeResourceSize = 100 << 20;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<ParquetDataProcessor> _logger;
        private readonly IDataSchemaConverter _defaultSchemaConverter;
        private readonly IDataSchemaConverter _customSchemaConverter;
        private readonly ISchemaManager<ParquetSchemaNode> _schemaManager;

        private readonly object _parquetConverterLock = new object();
        private ParquetConverter _parquetConverter;

        public ParquetDataProcessor(
            ISchemaManager<ParquetSchemaNode> schemaManager,
            DataSchemaConverterDelegate schemaConverterDelegate,
            IDiagnosticLogger diagnosticLogger,
            ILogger<ParquetDataProcessor> logger)
        {
            EnsureArg.IsNotNull(schemaManager, nameof(schemaManager));
            EnsureArg.IsNotNull(schemaConverterDelegate, nameof(schemaConverterDelegate));
            EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _defaultSchemaConverter = schemaConverterDelegate(ParquetSchemaConstants.DefaultSchemaProviderKey);
            _customSchemaConverter = schemaConverterDelegate(ParquetSchemaConstants.CustomSchemaProviderKey);
            _schemaManager = schemaManager;
            _diagnosticLogger = diagnosticLogger;
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
                            Dictionary<string, string> schemaSet = _schemaManager.GetAllSchemaContent();
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

            string inputContent = string.Join(
                Environment.NewLine,
                processedData.Values.Select(jsonObject => {
                    var str = jsonObject.ToString(Formatting.None);

                    // add a log for large resource
                    if (str.Length > LargeResourceSize)
                    {
                        _diagnosticLogger.LogInformation($"Single data length of schema type {processParameters.SchemaType} for resource type {processParameters.ResourceType} is larger than {LargeResourceSize} Bytes.");
                        _logger.LogInformation($"Single data length of schema type {processParameters.SchemaType} for resource type {processParameters.ResourceType} is larger than {LargeResourceSize} Bytes.");
                    }

                    return str;
                }));
            if (string.IsNullOrEmpty(inputContent))
            {
                // Return StreamBatchData with null Value if no data has been converted.
                return Task.FromResult(new StreamBatchData(null, 0, processParameters.SchemaType));
            }

            // Convert JSON data to parquet stream.
            try
            {
                Stream resultStream = ParquetConverter.ConvertJsonToParquet(processParameters.SchemaType, inputContent);
                return Task.FromResult(
                    new StreamBatchData(
                        resultStream,
                        processedData.Values.Count(),
                        processParameters.SchemaType));
            }
            catch (ParquetException parquetEx)
            {
                _diagnosticLogger.LogError($"Exception happened when converting input data to parquet for \"{processParameters.SchemaType}\". Reason: {parquetEx.Message}");
                _logger.LogInformation(parquetEx, $"Exception happened when converting input data to parquet for \"{processParameters.SchemaType}\". Reason: {0}", parquetEx.Message);
                throw new ParquetDataProcessorException($"Exception happened when converting input data to parquet for \"{processParameters.SchemaType}\".", parquetEx);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"Unknown exception when converting input data to parquet for \"{processParameters.SchemaType}\".");
                _logger.LogError(ex, $"Unhandled exception when converting input data to parquet for \"{processParameters.SchemaType}\".");
                throw;
            }
        }
    }
}
