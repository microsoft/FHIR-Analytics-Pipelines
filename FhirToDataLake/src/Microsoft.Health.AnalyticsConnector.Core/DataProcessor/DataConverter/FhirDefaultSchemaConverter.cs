// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Models.Data;
using Microsoft.Health.AnalyticsConnector.Core.Exceptions;
using Microsoft.Health.AnalyticsConnector.SchemaManagement;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Parquet;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.AnalyticsConnector.Core.DataProcessor.DataConverter
{
    public class FhirDefaultSchemaConverter : IDataSchemaConverter
    {
        private readonly ISchemaManager<ParquetSchemaNode> _schemaManager;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<FhirDefaultSchemaConverter> _logger;

        public FhirDefaultSchemaConverter(
            ISchemaManager<ParquetSchemaNode> schemaManager,
            IDiagnosticLogger diagnosticLogger,
            ILogger<FhirDefaultSchemaConverter> logger)
        {
            _schemaManager = EnsureArg.IsNotNull(schemaManager, nameof(schemaManager));
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
            ParquetSchemaNode schema = _schemaManager.GetSchema(schemaType);
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

                    return ProcessStructObject(json, schema);
                })
                .Where(processedResult => processedResult != null);

            return new JsonBatchData(processedJsonData);
        }

        private JObject ProcessStructObject(JToken structItem, ParquetSchemaNode schemaNode)
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

                    ParquetSchemaNode dataTypeNode = schemaNode.SubNodes[choiceTypeName].SubNodes[choiceTypeDataType];
                    processedObject.Add(choiceTypeName, ProcessChoiceTypeObject(subObject, dataTypeNode, choiceTypeDataType));
                }
                else
                {
                    // Ignore FHIR data node if it doesn't exist in schema.
                    if (schemaNode.SubNodes == null || !schemaNode.SubNodes.ContainsKey(subObjectKey))
                    {
                        continue;
                    }

                    ParquetSchemaNode subNode = schemaNode.SubNodes[subObjectKey];

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

        private JArray ProcessArrayObject(JToken arrayItem, ParquetSchemaNode schemaNode)
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
                    arrayObject.Add(ProcessStructObject(item, schemaNode));
                }
            }

            return arrayObject;
        }

        private JValue ProcessLeafObject(JToken fhirObject, ParquetSchemaNode schemaNode)
        {
            if (schemaNode.Type == ParquetSchemaConstants.JsonStringType)
            {
                return new JValue(fhirObject.ToString(Formatting.None));
            }

            if (fhirObject is not JValue fhirLeafObject)
            {
                _diagnosticLogger.LogError($"Invalid data: complex object found in leaf schema node {fhirObject.Path}.");
                _logger.LogInformation($"Invalid data: complex object found in leaf schema node {fhirObject.Path}.");
                throw new ParquetDataProcessorException($"Invalid data: complex object found in leaf schema node {fhirObject.Path}.");
            }

            return fhirLeafObject;
        }

        private JObject ProcessChoiceTypeObject(JToken fhirObject, ParquetSchemaNode schemaNode, string schemaNodeKey)
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
