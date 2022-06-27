// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Liquid.Converter;
using Microsoft.Health.Fhir.Liquid.Converter.Models;
using Microsoft.Health.Fhir.Liquid.Converter.Processors;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter
{
    public class FhirConverter
    {
        private readonly string SchemaDirectory = "../../../../../../schemaTemplates";
        private readonly JsonProcessor _jsonProcessor;
        private readonly ILogger<FhirConverter> _logger;

        public FhirConverter(ILogger<FhirConverter> logger)
        {
            _jsonProcessor = new JsonProcessor(new ProcessorSettings());
            _logger = logger;
        }

        public JsonBatchData Convert(
            JsonBatchData inputData,
            string resourceType,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var results = new List<JObject>();

            var templateProvider = new TemplateProvider(SchemaDirectory, DataType.Json);
            foreach (var jsonData in inputData.Values)
            {
                var result = _jsonProcessor.Convert(jsonData.ToString(), resourceType, templateProvider);
                results.Add(JObject.Parse(result));
            }

            return new JsonBatchData(results);
        }
    }
}
