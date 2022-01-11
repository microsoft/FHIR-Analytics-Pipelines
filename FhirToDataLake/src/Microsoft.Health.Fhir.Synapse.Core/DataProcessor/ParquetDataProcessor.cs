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
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
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

        public ParquetDataProcessor(
            IFhirSchemaManager<FhirParquetSchemaNode> fhirSchemaManager,
            IOptions<ArrowConfiguration> arrowConfiguration,
            ILogger<ParquetDataProcessor> logger)
        {
            EnsureArg.IsNotNull(fhirSchemaManager, nameof(fhirSchemaManager));
            EnsureArg.IsNotNull(arrowConfiguration, nameof(arrowConfiguration));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _fhirSchemaManager = fhirSchemaManager;
            _arrowConfiguration = arrowConfiguration.Value;
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
            TaskContext context,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Preprocess data
            JsonBatchData preprocessedData = Preprocess(inputData, context, cancellationToken);

            // Get FHIR schema for the input data.
            var schema = _fhirSchemaManager.GetSchema(context.ResourceType);

            if (schema == null)
            {
                _logger.LogError($"The FHIR schema node could not be found for resource type '{context.ResourceType}'.");
                throw new ParquetDataProcessorException($"The FHIR schema node could not be found for resource type '{context.ResourceType}'.");
            }

            var inputStream = ConvertJsonDataToStream(context.ResourceType, preprocessedData.Values);
            if (inputStream == null)
            {
                // Return null if no data has been converted.
                return Task.FromResult<StreamBatchData>(null);
            }

            // Convert JSON data to parquet stream.
            try
            {
                var resultStream = _parquetConverterWrapper.ConvertToParquetStream(context.ResourceType, inputStream);
                return Task.FromResult(new StreamBatchData(resultStream));
            }
            catch (Exception ex)
            {
                _logger.LogError($"Exception happened when converting input data to parquet for \"{context.ResourceType}\".");
                throw new ParquetDataProcessorException($"Exception happened when converting input data to parquet for \"{context.ResourceType}\".", ex);
            }
        }

        public JsonBatchData Preprocess(
            JsonBatchData inputData,
            TaskContext context,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Get FHIR schema for the input data.
            var schema = _fhirSchemaManager.GetSchema(context.ResourceType);

            if (schema == null)
            {
                _logger.LogError($"The FHIR schema node could not be found for resource type '{context.ResourceType}'.");
                throw new ParquetDataProcessorException($"The FHIR schema node could not be found for resource type '{context.ResourceType}'.");
            }

            var processedJsonData = inputData.Values
                .Select(json => ProcessStructObject(json, schema))
                .Where(processedResult => processedResult != null);

            context.SkippedCount += inputData.Values.Count() - processedJsonData.Count();

            return new JsonBatchData(processedJsonData);
        }

        private JObject ProcessStructObject(JToken structItem, FhirParquetSchemaNode schemaNode)
        {
            if (structItem is not JObject fhirJObject)
            {
                _logger.LogError($"Current FHIR object is not a valid JObject: {schemaNode.GetNodePath()}.");
                throw new ParquetDataProcessorException($"Current FHIR object is not a valid JObject: {schemaNode.GetNodePath()}.");
            }

            JObject processedObject = new JObject();

            foreach (var subItem in fhirJObject)
            {
                JToken subObject = subItem.Value;

                // Process choice type FHIR resource.
                if (schemaNode.ContainsChoiceDataType(subItem.Key))
                {
                    var choiceTypeName = schemaNode.ChoiceTypeNodes[subItem.Key].Item1;
                    var choiceTypeDataType = schemaNode.ChoiceTypeNodes[subItem.Key].Item2;

                    if (!schemaNode.SubNodes[choiceTypeName].SubNodes.ContainsKey(choiceTypeDataType))
                    {
                        _logger.LogError($"Data type \"{choiceTypeDataType}\" cannot be found in choice type property, {schemaNode.GetNodePath()}.");
                        throw new ParquetDataProcessorException($"Data type \"{choiceTypeDataType}\" cannot be found in choice type property, {schemaNode.GetNodePath()}.");
                    }

                    var dataTypeNode = schemaNode.SubNodes[choiceTypeName].SubNodes[choiceTypeDataType];
                    processedObject.Add(choiceTypeName, ProcessChoiceTypeObject(subObject, dataTypeNode));
                }
                else
                {
                    // Ignore FHIR data node if it doesn't exist in schema.
                    if (schemaNode.SubNodes == null || !schemaNode.SubNodes.ContainsKey(subItem.Key))
                    {
                        continue;
                    }

                    FhirParquetSchemaNode subNode = schemaNode.SubNodes[subItem.Key];

                    if (subNode.IsRepeated)
                    {
                        // Process array FHIR resource.
                        processedObject.Add(subNode.Name, ProcessArrayObject(subObject, subNode));
                    }
                    else if (subNode.IsLeaf)
                    {
                        // Process leaf FHIR resource.
                        processedObject.Add(subNode.Name, ProcessLeafObject(subObject, subNode));
                    }
                    else
                    {
                        // Process struct FHIR resource.
                        processedObject.Add(subNode.Name, ProcessStructObject(subObject, subNode));
                    }
                }
            }

            return processedObject;
        }

        private JArray ProcessArrayObject(JToken arrayItem, FhirParquetSchemaNode schemaNode)
        {
            if (arrayItem is not JArray fhirArrayObject)
            {
                _logger.LogError($"Current FHIR object is not a valid JArray: {schemaNode.GetNodePath()}.");
                throw new ParquetDataProcessorException($"Current FHIR object is not a valid JArray: {schemaNode.GetNodePath()}.");
            }

            JArray arrayObject = new JArray();
            foreach (var item in fhirArrayObject)
            {
                if (schemaNode.IsLeaf)
                {
                    arrayObject.Add(ProcessLeafObject(item, schemaNode));
                }
                else
                {
                    arrayObject.Add(ProcessStructObject(item, schemaNode));
                }
            }

            return arrayObject;
        }

        private JValue ProcessLeafObject(JToken fhirObject, FhirParquetSchemaNode schemaNode)
        {
            if (schemaNode.Type == FhirParquetSchemaNodeConstants.JsonStringType)
            {
                return new JValue(fhirObject.ToString(Formatting.None));
            }

            if (fhirObject is not JValue fhirLeafObject)
            {
                _logger.LogError($"Invalid data: complex object found in leaf schema node {schemaNode.GetNodePath()}.");
                throw new ParquetDataProcessorException($"Invalid data: complex object found in leaf schema node {schemaNode.GetNodePath()}.");
            }

            return fhirLeafObject;
        }

        private JObject ProcessChoiceTypeObject(JToken fhirObject, FhirParquetSchemaNode schemaNode)
        {
            JObject choiceRootObject = new JObject();
            if (schemaNode.IsLeaf)
            {
                choiceRootObject.Add(schemaNode.Name, ProcessLeafObject(fhirObject, schemaNode));
            }
            else
            {
                choiceRootObject.Add(schemaNode.Name, ProcessStructObject(fhirObject, schemaNode));
            }

            return choiceRootObject;
        }

        private MemoryStream ConvertJsonDataToStream(string resourceType, IEnumerable<JObject> inputData)
        {
            var content = string.Join(
                Environment.NewLine,
                inputData.Select(jsonObject => jsonObject.ToString(Formatting.None))
                         .Where(result => CheckBlockSize(resourceType, result)));

            // If no content been fetched, E.g. input data is empty or all FHIR data are larger than block size, will return null.
            return string.IsNullOrEmpty(content) ? null : new MemoryStream(Encoding.UTF8.GetBytes(content));
        }

        private bool CheckBlockSize(string resourceType, string data)
        {
            // If length of actual data is larger than BlockSize in configuration, log a warning and ignore that data, return an empty JSON string.
            // TODO: Confirm the BlockSize handle logic in arrow.lib.
            if (data.Length > _arrowConfiguration.ReadOptions.BlockSize)
            {
                _logger.LogWarning($"Single data length of {resourceType} is larger than BlockSize {_arrowConfiguration.ReadOptions.BlockSize}, will be ignored when converting to parquet.");
                return false;
            }

            // If length of actual data is closing to BlockSize in configuration, log a warning, still return data in string.
            // Temporarily use 1/3 as the a threshold to give the warning message.
            if (data.Length * 3 > _arrowConfiguration.ReadOptions.BlockSize)
            {
                _logger.LogWarning($"Single data length of {resourceType} is closing to BlockSize {_arrowConfiguration.ReadOptions.BlockSize}.");
            }

            return true;
        }
    }
}
