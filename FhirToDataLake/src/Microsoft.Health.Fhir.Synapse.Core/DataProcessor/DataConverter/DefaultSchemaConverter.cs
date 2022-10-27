// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using EnsureThat;
using Microsoft.Extensions.Logging;
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
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<DefaultSchemaConverter> _logger;

        public DefaultSchemaConverter(
            IFhirSchemaManager<FhirParquetSchemaNode> fhirSchemaManager,
            IDiagnosticLogger diagnosticLogger,
            ILogger<DefaultSchemaConverter> logger)
        {
            _fhirSchemaManager = EnsureArg.IsNotNull(fhirSchemaManager, nameof(fhirSchemaManager));
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
            var schema = _fhirSchemaManager.GetSchema(schemaType);
            if (schema == null)
            {
                _diagnosticLogger.LogError($"The FHIR schema node could not be found for schema type '{schemaType}'.");
                _logger.LogInformation($"The FHIR schema node could not be found for schema type '{schemaType}'.");
                throw new ParquetDataProcessorException($"The FHIR schema node could not be found for schema type '{schemaType}'.");
            }

            var nodePath = new Stack<string>();
            nodePath.Push(schemaType);

            var processedJsonData = inputData.Values
                .Select(json => ProcessStructObject(json, schema, nodePath))
                .Where(processedResult => processedResult != null);

            nodePath.Pop();
            return new JsonBatchData(processedJsonData);
        }

        private JObject ProcessStructObject(JToken structItem, FhirParquetSchemaNode schemaNode, Stack<string> nodePath)
        {
            if (structItem is not JObject fhirJObject)
            {
                _diagnosticLogger.LogError($"Current FHIR object is not a valid JObject: {string.Join('.', nodePath)}.");
                _logger.LogInformation($"Current FHIR object is not a valid JObject: {string.Join('.', nodePath)}.");
                throw new ParquetDataProcessorException($"Current FHIR object is not a valid JObject: {string.Join('.', nodePath)}.");
            }

            var processedObject = new JObject();

            foreach (var subItem in fhirJObject)
            {
                var subObject = subItem.Value;
                var subObjectKey = subItem.Key;
                nodePath.Push(subObjectKey);

                // Process choice type FHIR resource.
                if (schemaNode.ContainsChoiceDataType(subObjectKey))
                {
                    var choiceTypeName = schemaNode.ChoiceTypeNodes[subObjectKey].Item1;
                    var choiceTypeDataType = schemaNode.ChoiceTypeNodes[subObjectKey].Item2;

                    if (!schemaNode.SubNodes[choiceTypeName].SubNodes.ContainsKey(choiceTypeDataType))
                    {
                        _diagnosticLogger.LogError($"Data type \"{choiceTypeDataType}\" cannot be found in choice type property, {string.Join('.', nodePath)}.");
                        _logger.LogInformation($"Data type \"{choiceTypeDataType}\" cannot be found in choice type property, {string.Join('.', nodePath)}.");
                        throw new ParquetDataProcessorException($"Data type \"{choiceTypeDataType}\" cannot be found in choice type property, {string.Join('.', nodePath)}.");
                    }

                    var dataTypeNode = schemaNode.SubNodes[choiceTypeName].SubNodes[choiceTypeDataType];
                    processedObject.Add(choiceTypeName, ProcessChoiceTypeObject(subObject, dataTypeNode, choiceTypeDataType, nodePath));
                }
                else
                {
                    // Ignore FHIR data node if it doesn't exist in schema.
                    if (schemaNode.SubNodes == null || !schemaNode.SubNodes.ContainsKey(subObjectKey))
                    {
                        continue;
                    }

                    var subNode = schemaNode.SubNodes[subObjectKey];

                    if (subNode.IsRepeated)
                    {
                        // Process array FHIR resource.
                        processedObject.Add(subObjectKey, ProcessArrayObject(subObject, subNode, nodePath));
                    }
                    else if (subNode.IsLeaf)
                    {
                        // Process leaf FHIR resource.
                        processedObject.Add(subObjectKey, ProcessLeafObject(subObject, subNode, nodePath));
                    }
                    else
                    {
                        // Process struct FHIR resource.
                        processedObject.Add(subObjectKey, ProcessStructObject(subObject, subNode, nodePath));
                    }
                }

                nodePath.Pop();
            }

            return processedObject;
        }

        private JArray ProcessArrayObject(JToken arrayItem, FhirParquetSchemaNode schemaNode, Stack<string> nodePath)
        {
            if (arrayItem is not JArray fhirArrayObject)
            {
                _diagnosticLogger.LogError($"Current FHIR object is not a valid JArray: {string.Join('.', nodePath)}.");
                _logger.LogInformation($"Current FHIR object is not a valid JArray: {string.Join('.', nodePath)}.");
                throw new ParquetDataProcessorException($"Current FHIR object is not a valid JArray: {string.Join('.', nodePath)}.");
            }

            var arrayObject = new JArray();
            foreach (var item in fhirArrayObject)
            {
                if (schemaNode.IsLeaf)
                {
                    arrayObject.Add(ProcessLeafObject(item, schemaNode, nodePath));
                }
                else
                {
                    arrayObject.Add(ProcessStructObject(item, schemaNode, nodePath));
                }
            }

            return arrayObject;
        }

        private JValue ProcessLeafObject(JToken fhirObject, FhirParquetSchemaNode schemaNode, Stack<string> nodePath)
        {
            if (schemaNode.Type == FhirParquetSchemaConstants.JsonStringType)
            {
                return new JValue(fhirObject.ToString(Formatting.None));
            }

            if (fhirObject is not JValue fhirLeafObject)
            {
                _diagnosticLogger.LogError($"Invalid data: complex object found in leaf schema node {string.Join('.', nodePath)}.");
                _logger.LogInformation($"Invalid data: complex object found in leaf schema node {string.Join('.', nodePath)}.");
                throw new ParquetDataProcessorException($"Invalid data: complex object found in leaf schema node {string.Join('.', nodePath)}.");
            }

            return fhirLeafObject;
        }

        private JObject ProcessChoiceTypeObject(JToken fhirObject, FhirParquetSchemaNode schemaNode, string schemaNodeKey, Stack<string> nodePath)
        {
            var choiceRootObject = new JObject();
            if (schemaNode.IsLeaf)
            {
                choiceRootObject.Add(schemaNodeKey, ProcessLeafObject(fhirObject, schemaNode, nodePath));
            }
            else
            {
                choiceRootObject.Add(schemaNodeKey, ProcessStructObject(fhirObject, schemaNode, nodePath));
            }

            return choiceRootObject;
        }
    }
}
