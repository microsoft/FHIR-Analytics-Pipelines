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
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter
{
    public class DefaultSchemaConverter : IDataSchemaConverter
    {
        private readonly IFhirSchemaManager<FhirParquetSchemaNode> _fhirSchemaManager;
        private readonly DataSourceType _dataSourceType;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<DefaultSchemaConverter> _logger;

        public DefaultSchemaConverter(
            IFhirSchemaManager<FhirParquetSchemaNode> fhirSchemaManager,
            IOptions<DataSourceConfiguration> dataSourceConfiguration,
            IDiagnosticLogger diagnosticLogger,
            ILogger<DefaultSchemaConverter> logger)
        {
            _fhirSchemaManager = EnsureArg.IsNotNull(fhirSchemaManager, nameof(fhirSchemaManager));
            _dataSourceType = EnsureArg.EnumIsDefined(dataSourceConfiguration.Value.Type, nameof(dataSourceConfiguration.Value.Type));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        public JsonBatchData Convert(
            JsonBatchData inputData,
            string schemaType,
            CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(inputData?.Values);
            EnsureArg.IsNotNullOrWhiteSpace(schemaType);

            cancellationToken.ThrowIfCancellationRequested();

            // Get FHIR schema for the input data.
            FhirParquetSchemaNode schema = _fhirSchemaManager.GetSchema(schemaType);
            if (schema == null)
            {
                _diagnosticLogger.LogError($"The FHIR schema node could not be found for schema type '{schemaType}'.");
                _logger.LogInformation($"The FHIR schema node could not be found for schema type '{schemaType}'.");
                throw new ParquetDataProcessorException($"The FHIR schema node could not be found for schema type '{schemaType}'.");
            }

            IEnumerable<JObject> processedJsonData = inputData.Values
                .Select(json =>
                {
                    if (json == null)
                    {
                        _diagnosticLogger.LogError($"The input FHIR data is null for schema type '{schemaType}'.");
                        _logger.LogInformation($"The input FHIR data is null for schema type '{schemaType}'.");
                        throw new ParquetDataProcessorException($"The input FHIR data is null for schema type '{schemaType}'.");
                    }

                    return _dataSourceType == DataSourceType.DICOM ? ProcessDicomMetadataObject(json, schema) : ProcessStructObject(json, schema);
                })
                .Where(processedResult => processedResult != null);

            return new JsonBatchData(processedJsonData);
        }

        private JObject ProcessDicomMetadataObject(JToken metadata, FhirParquetSchemaNode schemaNode)
        {
            if (metadata is not JObject metadataObject)
            {
                _logger.LogError($"Current DICOM object is not a valid JObject: {metadata.Path}.");
                throw new ParquetDataProcessorException($"Current DICOM object is not a valid JObject: {metadata.Path}.");
            }

            var processedObject = new JObject();

            foreach (var item in metadataObject)
            {
                var subNodePair = schemaNode.SubNodes
                    .Where(x => string.Equals(x.Value.Name, item.Key, StringComparison.OrdinalIgnoreCase))
                    .FirstOrDefault();
                var subNodeKeyword = subNodePair.Key;
                var subNode = subNodePair.Value;

                // Ignore DICOM metadata node if it doesn't exist in schema
                if (subNodeKeyword == null)
                {
                    continue;
                }

                // Ignore empty tag, BulkDataURI and InlineBinary
                // Ignore SQ
                if (item.Value is not JObject jObject ||
                    !jObject.ContainsKey("vr") ||
                    !jObject.ContainsKey("Value") ||
                    jObject["Value"] is not JArray ||
                    string.Equals(jObject["vr"].ToString(), "SQ"))
                {
                    continue;
                }

                var subValueArray = jObject["Value"] as JArray;

                if (subNode.IsRepeated)
                {
                    processedObject.Add(subNodeKeyword, ProcessArrayObject(subValueArray, subNode));
                }
                else
                {
                    if (subValueArray.Count > 1)
                    {
                        _logger.LogInformation($"Multiple values appear in an unique tag. Keyword: {subNodeKeyword}");
                    }

                    if (subNode.IsLeaf)
                    {
                        var singleElement = subValueArray.ToString();
                        processedObject.Add(subNodeKeyword, ProcessLeafObject(singleElement, subNode));
                    }
                    else
                    {
                        var singleElement = subValueArray.First;
                        processedObject.Add(subNodeKeyword, ProcessDicomPnObject(singleElement, subNode));
                    }
                }
            }

            return processedObject;
        }

        private JObject ProcessDicomPnObject(JToken pnItem, FhirParquetSchemaNode schemaNode)
        {
            if (pnItem is not JObject jObject)
            {
                _logger.LogError($"Current PN object is not a valid JObject: {pnItem.Path}.");
                throw new ParquetDataProcessorException($"Current PN object is not a valid JObject: {pnItem.Path}.");
            }

            return jObject;
        }

        private JObject ProcessStructObject(JToken structItem, FhirParquetSchemaNode schemaNode)
        {
            if (structItem is not JObject fhirJObject)
            {
                _diagnosticLogger.LogError($"Current FHIR object is not a valid JObject: {structItem.Path}.");
                _logger.LogInformation($"Current FHIR object is not a valid JObject: {structItem.Path}.");
                throw new ParquetDataProcessorException($"Current FHIR object is not a valid JObject: {structItem.Path}.");
            }

            var processedObject = new JObject();

            foreach (JProperty subProperty in fhirJObject.Properties())
            {
                JToken subObject = subProperty.Value;
                string subObjectKey = subProperty.Name;

                // Process choice type FHIR resource.
                if (schemaNode.ContainsChoiceDataType(subObjectKey))
                {
                    string choiceTypeName = schemaNode.ChoiceTypeNodes[subObjectKey].Item1;
                    string choiceTypeDataType = schemaNode.ChoiceTypeNodes[subObjectKey].Item2;

                    if (!schemaNode.SubNodes[choiceTypeName].SubNodes.ContainsKey(choiceTypeDataType))
                    {
                        _diagnosticLogger.LogError($"Data type \"{choiceTypeDataType}\" cannot be found in choice type property, {subObject.Path}.");
                        _logger.LogInformation($"Data type \"{choiceTypeDataType}\" cannot be found in choice type property, {subObject.Path}.");
                        throw new ParquetDataProcessorException($"Data type \"{choiceTypeDataType}\" cannot be found in choice type property, {subObject.Path}.");
                    }

                    FhirParquetSchemaNode dataTypeNode = schemaNode.SubNodes[choiceTypeName].SubNodes[choiceTypeDataType];
                    processedObject.Add(choiceTypeName, ProcessChoiceTypeObject(subObject, dataTypeNode, choiceTypeDataType));
                }
                else
                {
                    // Ignore FHIR data node if it doesn't exist in schema.
                    if (schemaNode.SubNodes == null || !schemaNode.SubNodes.ContainsKey(subObjectKey))
                    {
                        continue;
                    }

                    FhirParquetSchemaNode subNode = schemaNode.SubNodes[subObjectKey];

                    if (subNode.IsRepeated)
                    {
                        // Process array FHIR resource.
                        processedObject.Add(subObjectKey, ProcessArrayObject(subObject, subNode));
                    }
                    else if (subNode.IsLeaf)
                    {
                        // Process leaf FHIR resource.
                        processedObject.Add(subObjectKey, ProcessLeafObject(subObject, subNode));
                    }
                    else
                    {
                        // Process struct FHIR resource.
                        processedObject.Add(subObjectKey, ProcessStructObject(subObject, subNode));
                    }
                }
            }

            return processedObject;
        }

        private JArray ProcessArrayObject(JToken arrayItem, FhirParquetSchemaNode schemaNode)
        {
            if (arrayItem is not JArray fhirArrayObject)
            {
                _diagnosticLogger.LogError($"Current FHIR object is not a valid JArray: {arrayItem.Path}.");
                _logger.LogInformation($"Current FHIR object is not a valid JArray: {arrayItem.Path}.");
                throw new ParquetDataProcessorException($"Current FHIR object is not a valid JArray: {arrayItem.Path}.");
            }

            var arrayObject = new JArray();
            foreach (JToken item in fhirArrayObject)
            {
                if (schemaNode.IsLeaf)
                {
                    arrayObject.Add(ProcessLeafObject(item, schemaNode));
                }
                else
                {
                    arrayObject.Add(_dataSourceType == DataSourceType.DICOM ? ProcessDicomPnObject(item, schemaNode) : ProcessStructObject(item, schemaNode));
                }
            }

            return arrayObject;
        }

        private JValue ProcessLeafObject(JToken fhirObject, FhirParquetSchemaNode schemaNode)
        {
            if (schemaNode.Type == FhirParquetSchemaConstants.JsonStringType)
            {
                return new JValue(fhirObject.ToString(Formatting.None));
            }

            if (fhirObject is not JValue fhirLeafObject)
            {
                _diagnosticLogger.LogError($"Invalid data: complex object found in leaf schema node {fhirObject.Path}.");
                _logger.LogInformation($"Invalid data: complex object found in leaf schema node {fhirObject.Path}.");
                throw new ParquetDataProcessorException($"Invalid data: complex object found in leaf schema node {fhirObject.Path}.");
            }

            // Convert every type to string for DICOM
            return _dataSourceType == DataSourceType.DICOM ? new JValue(fhirLeafObject.ToString()) : fhirLeafObject;
        }

        private JObject ProcessChoiceTypeObject(JToken fhirObject, FhirParquetSchemaNode schemaNode, string schemaNodeKey)
        {
            var choiceRootObject = new JObject();
            if (schemaNode.IsLeaf)
            {
                choiceRootObject.Add(schemaNodeKey, ProcessLeafObject(fhirObject, schemaNode));
            }
            else
            {
                choiceRootObject.Add(schemaNodeKey, ProcessStructObject(fhirObject, schemaNode));
            }

            return choiceRootObject;
        }
    }
}
