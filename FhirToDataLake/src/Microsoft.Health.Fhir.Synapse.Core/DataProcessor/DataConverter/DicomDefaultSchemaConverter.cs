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
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.Dicom;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter
{
    public class DicomDefaultSchemaConverter : IDataSchemaConverter
    {
        private readonly ISchemaManager<ParquetSchemaNode> _schemaManager;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<DicomDefaultSchemaConverter> _logger;

        public DicomDefaultSchemaConverter(
            ISchemaManager<ParquetSchemaNode> schemaManager,
            IDiagnosticLogger diagnosticLogger,
            ILogger<DicomDefaultSchemaConverter> logger)
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

            // Get schema for the input data.
            ParquetSchemaNode schema = _schemaManager.GetSchema(schemaType);
            if (schema == null)
            {
                _diagnosticLogger.LogError($"The DICOM schema node could not be found for schema type '{schemaType}'.");
                _logger.LogInformation($"The DICOM schema node could not be found for schema type '{schemaType}'.");
                throw new ParquetDataProcessorException($"The DICOM schema node could not be found for schema type '{schemaType}'.");
            }

            IEnumerable<JObject> processedJsonData = inputData.Values
                .Select(json =>
                {
                    return ProcessDicomMetadataObject(json, schema);
                })
                .Where(processedResult => processedResult != null);

            return new JsonBatchData(processedJsonData);
        }

        private JObject ProcessDicomMetadataObject(JToken metadata, ParquetSchemaNode schemaNode)
        {
            if (metadata == null)
            {
                _diagnosticLogger.LogError($"The input DICOM metadata is null for schema type '{schemaNode.Type}'.");
                _logger.LogInformation($"The input DICOM metadata is null for schema type '{schemaNode.Type}'.");
                throw new ParquetDataProcessorException($"The input DICOM metadata is null for schema type '{schemaNode.Type}'.");
            }

            if (metadata is not JObject metadataObject)
            {
                _diagnosticLogger.LogError($"Current DICOM metadata is not a valid JObject: {metadata.Path}.");
                _logger.LogInformation($"Current DICOM metadata is not a valid JObject: {metadata.Path}.");
                throw new ParquetDataProcessorException($"Current DICOM metadata is not a valid JObject: {metadata.Path}.");
            }

            var processedObject = new JObject();

            foreach (var item in metadataObject)
            {
                // Find schema node by name
                var subNodePair = schemaNode.SubNodes
                    .Where(x => string.Equals(x.Value.Name, item.Key, StringComparison.OrdinalIgnoreCase))
                    .FirstOrDefault();

                var subNodeKeyword = subNodePair.Key;
                var subNode = subNodePair.Value;

                // Handle our additional self-defined properties that have specific prefix.
                if (subNodeKeyword != null && subNodeKeyword[0] == DicomConstants.AdditionalColumnPrefix)
                {
                    if (subNode.IsRepeated)
                    {
                        processedObject.Add(subNodeKeyword, ProcessArrayObject(item.Value, subNode));
                    }
                    else
                    {
                        processedObject.Add(subNodeKeyword, ProcessLeafObject(item.Value, subNode));
                    }
                }
                else
                {
                    // Each metadata native tag should be JObject containing a child object "vr"
                    // Reference: https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_F.2.2.html
                    if (item.Value is not JObject jObject ||
                        !jObject.ContainsKey(DicomConstants.Vr))
                    {
                        _diagnosticLogger.LogError($"Current DICOM tag is not a valid JObject: {item.Key}.");
                        _logger.LogInformation($"Current DICOM tag is not a valid JObject: {item.Key}.");
                        throw new ParquetDataProcessorException($"Current DICOM tag is not a valid JObject: {item.Key}.");
                    }

                    // Ignore DICOM metadata node if it doesn't exist in schema, including data type SQ
                    // Ignore empty tag, BulkDataURI and InlineBinary
                    // Value field should be an array
                    if (subNodeKeyword == null ||
                        !jObject.ContainsKey(DicomConstants.Value) ||
                        jObject[DicomConstants.Value] is not JArray)
                    {
                        continue;
                    }

                    var subValueArray = jObject[DicomConstants.Value] as JArray;

                    if (subNode.IsRepeated)
                    {
                        processedObject.Add(subNodeKeyword, ProcessArrayObject(subValueArray, subNode));
                    }
                    else
                    {
                        if (subValueArray.Count > 1)
                        {
                            _diagnosticLogger.LogWarning($"Multiple values appear in an unique tag. Keyword: {subNodeKeyword}");
                            _logger.LogInformation($"Multiple values appear in an unique tag. Keyword: {subNodeKeyword}");
                        }

                        var singleElement = subValueArray.First();

                        if (subNode.IsLeaf)
                        {
                            processedObject.Add(subNodeKeyword, ProcessLeafObject(singleElement, subNode));
                        }
                        else
                        {
                            processedObject.Add(subNodeKeyword, ProcessDicomPnObject(singleElement));
                        }
                    }
                }
            }

            return processedObject;
        }

        private JObject ProcessDicomPnObject(JToken pnItem)
        {
            if (pnItem is not JObject pnObject)
            {
                _diagnosticLogger.LogError($"Current DICOM PN object is not a valid JObject: {pnItem.Path}.");
                _logger.LogInformation($"Current DICOM PN object is not a valid JObject: {pnItem.Path}.");
                throw new ParquetDataProcessorException($"Current DICOM PN object is not a valid JObject: {pnItem.Path}.");
            }

            return pnObject;
        }

        private JArray ProcessArrayObject(JToken arrayItem, ParquetSchemaNode schemaNode)
        {
            if (arrayItem is not JArray array)
            {
                _diagnosticLogger.LogError($"Current DICOM metadata object is not a valid JArray: {arrayItem.Path}.");
                _logger.LogInformation($"Current DICOM metadata object is not a valid JArray: {arrayItem.Path}.");
                throw new ParquetDataProcessorException($"Current DICOM metadata object is not a valid JArray: {arrayItem.Path}.");
            }

            var arrayObject = new JArray();
            foreach (JToken item in array)
            {
                if (schemaNode.IsLeaf)
                {
                    arrayObject.Add(ProcessLeafObject(item, schemaNode));
                }
                else
                {
                    arrayObject.Add(ProcessDicomPnObject(item));
                }
            }

            return arrayObject;
        }

        private JValue ProcessLeafObject(JToken leafItem, ParquetSchemaNode schemaNode)
        {
            if (schemaNode.Type == ParquetSchemaConstants.JsonStringType)
            {
                return new JValue(leafItem.ToString(Formatting.None));
            }

            if (leafItem is not JValue leafValue)
            {
                _diagnosticLogger.LogError($"Invalid data: complex object found in leaf schema node {leafItem.Path}.");
                _logger.LogInformation($"Invalid data: complex object found in leaf schema node {leafItem.Path}.");
                throw new ParquetDataProcessorException($"Invalid data: complex object found in leaf schema node {leafItem.Path}.");
            }

            // Currently convert every data type to string for DICOM
            // TODO: make data type more specific
            return new JValue(leafValue.ToString());
        }
    }
}
