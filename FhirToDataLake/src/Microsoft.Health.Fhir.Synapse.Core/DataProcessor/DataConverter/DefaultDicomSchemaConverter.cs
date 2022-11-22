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
    public class DefaultDicomSchemaConverter : IDataSchemaConverter
    {
        private readonly ISchemaManager<ParquetSchemaNode> _schemaManager;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<DefaultDicomSchemaConverter> _logger;

        public DefaultDicomSchemaConverter(
            ISchemaManager<ParquetSchemaNode> schemaManager,
            IDiagnosticLogger diagnosticLogger,
            ILogger<DefaultDicomSchemaConverter> logger)
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
                    if (json == null)
                    {
                        _diagnosticLogger.LogError($"The input DICOM metadata is null for schema type '{schemaType}'.");
                        _logger.LogInformation($"The input DICOM metadata is null for schema type '{schemaType}'.");
                        throw new ParquetDataProcessorException($"The input DICOM metadata is null for schema type '{schemaType}'.");
                    }

                    return ProcessDicomMetadataObject(json, schema);
                })
                .Where(processedResult => processedResult != null);

            return new JsonBatchData(processedJsonData);
        }

        private JObject ProcessDicomMetadataObject(JToken metadata, ParquetSchemaNode schemaNode)
        {
            if (metadata is not JObject metadataObject)
            {
                _logger.LogError($"Current DICOM object is not a valid JObject: {metadata.Path}.");
                throw new ParquetDataProcessorException($"Current DICOM object is not a valid JObject: {metadata.Path}.");
            }

            var processedObject = new JObject();

            foreach (var item in metadataObject)
            {
                // Find node by name
                var subNodePair = schemaNode.SubNodes
                    .Where(x => string.Equals(x.Value.Name, item.Key, StringComparison.OrdinalIgnoreCase))
                    .FirstOrDefault();

                var subNodeKeyword = subNodePair.Key;
                var subNode = subNodePair.Value;

                if (item.Value is JValue jValue)
                {
                    // Handle our self-defined properties that start with '_'.
                    if (subNode.IsRepeated)
                    {
                        processedObject.Add(subNodeKeyword, ProcessArrayObject(jValue, subNode));
                    }
                    else
                    {
                        processedObject.Add(subNodeKeyword, ProcessLeafObject(jValue, subNode));
                    }
                }
                else
                {
                    // Each metadata native tag should be JObject containing a child object "vr"
                    if (item.Value is not JObject jObject ||
                        !jObject.ContainsKey(DicomConstants.Vr))
                    {
                        _logger.LogError($"Current DICOM tag is not a valid JObject: {item.Key}.");
                        throw new ParquetDataProcessorException($"Current DICOM tag is not a valid JObject: {item.Key}.");
                    }

                    // Ignore DICOM metadata node if it doesn't exist in schema
                    // Ignore SQ
                    // Ignore empty tag, BulkDataURI and InlineBinary
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
                _logger.LogError($"Current DICOM PN object is not a valid JObject: {pnItem.Path}.");
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

            // Convert every type to string for DICOM
            return new JValue(leafValue.ToString());
        }
    }
}
