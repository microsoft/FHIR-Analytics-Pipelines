// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.AnalyticsConnector.JobManagement.Extensions;
using Microsoft.Health.AnalyticsConnector.JobManagement.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs.Models
{
    public class FhirToDataLakeAzureStorageJobInfo : AzureStorageJobInfo
    {
        public override string JobIdentifier()
        {
            string result = (TryParseAsFhirToDataLakeOrchestratorJobInputData() ??
                             TryParseAsFhirToDataLakeProcessingJobInputData()) ?? Definition;

            return result.ComputeHash();
        }

        private string TryParseAsFhirToDataLakeOrchestratorJobInputData()
        {
            try
            {
                var inputData = JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobInputData>(Definition);
                if (inputData is { JobType: JobType.Orchestrator })
                {
                    return inputData.JobVersion switch
                    {
                        JobVersion.V1 => GetIdentifierText(inputData, FhirToDatalakeJobVersionManager.FhirToDataLakeOrchestratorJobIdentifierPropertiesV1),
                        JobVersion.V2 => GetIdentifierText(inputData, FhirToDatalakeJobVersionManager.FhirToDataLakeOrchestratorJobIdentifierPropertiesV2),
                        _ => null,
                    };
                }
            }
            catch (Exception)
            {
                // ignored
            }

            return null;
        }

        private string TryParseAsFhirToDataLakeProcessingJobInputData()
        {
            try
            {
                var inputData = JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobInputData>(Definition);
                if (inputData is { JobType: JobType.Processing })
                {
                    return inputData.JobVersion switch
                    {
                        JobVersion.V1 => GetIdentifierText(inputData, FhirToDatalakeJobVersionManager.FhirToDataLakeProcessingJobIdentifierPropertiesV1),
                        JobVersion.V2 => GetIdentifierText(inputData, FhirToDatalakeJobVersionManager.FhirToDataLakeProcessingJobIdentifierPropertiesV2),
                        _ => null,
                    };
                }
            }
            catch (Exception)
            {
                // ignored
            }

            return null;
        }

        private static string GetIdentifierText(object inputData, List<string> selectedProperties)
        {
            var inputDataJObject = JObject.FromObject(inputData);
            var identifierJObject = new JObject();
            foreach (var property in selectedProperties)
            {
                identifierJObject.Add(inputDataJObject.Property(property));
            }

            // When getting the job identifier, the default version (v1) serializes job input data using `JsonConvert.SerializeObject()`, which removes the JSON format in the output string.
            // We select specific properties for different job versions to calculate job identifier now, so `jObject.tostring()` is used, we need to remove the JSON format in the output string for `jObject.tostring()`  function by specifying "Formatting.None";
            return identifierJObject.ToString(Formatting.None);
        }
    }
}
