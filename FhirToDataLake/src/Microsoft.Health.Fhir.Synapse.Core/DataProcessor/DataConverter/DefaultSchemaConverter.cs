// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Linq;
using System.Threading;
using EnsureThat;
using Microsoft.Extensions.Logging;
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
        private readonly ILogger<DefaultSchemaConverter> _logger;

        public DefaultSchemaConverter(
            IFhirSchemaManager<FhirParquetSchemaNode> fhirSchemaManager,
            ILogger<DefaultSchemaConverter> logger)
        {
            _fhirSchemaManager = fhirSchemaManager;
            _logger = logger;
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
                _logger.LogError($"The FHIR schema node could not be found for schema type '{schemaType}'.");
                throw new ParquetDataProcessorException($"The FHIR schema node could not be found for schema type '{schemaType}'.");
            }

            var processedJsonData = inputData.Values
                .Select(json => ProcessDicomMetadataObject(json, schema))
                .Where(processedResult => processedResult != null);

            return new JsonBatchData(processedJsonData);
        }

        private JObject ProcessDicomMetadataObject(JToken metadata, FhirParquetSchemaNode schemaNode)
        {
            if (metadata is not JObject jObject)
            {
                _logger.LogError($"Current DICOM object is not a valid JObject: {schemaNode.GetNodePath()}.");
                throw new ParquetDataProcessorException($"Current DICOM object is not a valid JObject: {schemaNode.GetNodePath()}.");
            }

            var processedObject = new JObject();

            foreach (var subItem in jObject)
            {
                // Ignore DICOM metadata node if it doesn't exist in schema
                if (!schemaNode.SubNodes.ContainsKey(subItem.Key))
                {
                    continue;
                }

                // Ignore SQ, BulkDataURI and InlineData
                if (subItem.Value is not JObject subJObject ||
                    !subJObject.ContainsKey("vr") ||
                    !subJObject.ContainsKey("Value") ||
                    string.Equals(subJObject["vr"].ToString(), "SQ") ||
                    subJObject["Value"] is not JArray)
                {
                    continue;
                }

                var subNode = schemaNode.SubNodes[subItem.Key];
                var subValueArray = subJObject["Value"] as JArray;

                if (subNode.IsRepeated)
                {
                    processedObject.Add(subNode.Name, ProcessArrayObject(subValueArray, subNode));
                }
                else
                {
                    if (subValueArray.Count > 1)
                    {
                        _logger.LogError("Multiple values appear in an unrepeatable tag.");
                        throw new ParquetDataProcessorException("Multiple values appear in an unrepeatable tag.");
                    }

                    var singleElement = subValueArray.First;
                    if (subNode.IsLeaf)
                    {
                        processedObject.Add(subNode.Name, ProcessLeafObject(singleElement, subNode));
                    }
                    else
                    {
                        processedObject.Add(subNode.Name, ProcessPnObject(singleElement, subNode));
                    }
                }
            }

            return processedObject;
        }

        private JObject ProcessPnObject(JToken pnItem, FhirParquetSchemaNode schemaNode)
        {
            if (pnItem is not JObject jObject)
            {
                _logger.LogError($"Current PN object is not a valid JObject: {schemaNode.GetNodePath()}.");
                throw new ParquetDataProcessorException($"Current PN object is not a valid JObject: {schemaNode.GetNodePath()}.");
            }

            if (jObject.Count > 1)
            {
                _logger.LogError("PN should contain only one of Alphabetic, Ideographic and Phonetic.");
                throw new ParquetDataProcessorException("PN should contain only one of Alphabetic, Ideographic and Phonetic.");
            }

            return jObject;
        }

        private JObject ProcessStructObject(JToken structItem, FhirParquetSchemaNode schemaNode)
        {
            if (structItem is not JObject fhirJObject)
            {
                _logger.LogError($"Current FHIR object is not a valid JObject: {schemaNode.GetNodePath()}.");
                throw new ParquetDataProcessorException($"Current FHIR object is not a valid JObject: {schemaNode.GetNodePath()}.");
            }

            var processedObject = new JObject();

            foreach (var subItem in fhirJObject)
            {
                var subObject = subItem.Value;

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

                    var subNode = schemaNode.SubNodes[subItem.Key];

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

            var arrayObject = new JArray();
            foreach (var item in fhirArrayObject)
            {
                if (schemaNode.IsLeaf)
                {
                    arrayObject.Add(ProcessLeafObject(item, schemaNode));
                }
                else
                {
                    arrayObject.Add(ProcessPnObject(item, schemaNode));
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
                _logger.LogError($"Invalid data: complex object found in leaf schema node {schemaNode.GetNodePath()}.");
                throw new ParquetDataProcessorException($"Invalid data: complex object found in leaf schema node {schemaNode.GetNodePath()}.");
            }

            // Convert every type to string for current ParquetConverter
            //return new JValue(fhirLeafObject.ToString());
            return fhirLeafObject;
        }

        private JObject ProcessChoiceTypeObject(JToken fhirObject, FhirParquetSchemaNode schemaNode)
        {
            var choiceRootObject = new JObject();
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
    }
}
