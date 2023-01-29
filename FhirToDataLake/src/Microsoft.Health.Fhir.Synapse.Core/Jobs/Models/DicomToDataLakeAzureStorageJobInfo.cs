// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.JobManagement.Extensions;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class DicomToDataLakeAzureStorageJobInfo : AzureStorageJobInfo
    {
        // Remove end offset field in Definition to generate job identifier,
        // as the end offset is related to the trigger created time, and may be different if there are two instances try to create a new trigger simultaneously
        public override string JobIdentifier()
        {
            string result = (TryParseAsDicomToDataLakeOrchestratorJobInputData() ??
                             TryParseAsDicomToDataLakeProcessingJobInputData()) ?? Definition.ComputeHash();

            return result.ComputeHash();
        }

        private string TryParseAsDicomToDataLakeOrchestratorJobInputData()
        {
            try
            {
                var inputData = JsonConvert.DeserializeObject<DicomToDataLakeOrchestratorJobInputData>(Definition);
                if (inputData is { JobType: JobType.Orchestrator })
                {
                    return inputData.JobVersion switch
                    {
                        JobVersion.V1 => GetIdentifierText(inputData, DicomToDatalakeJobVersionManager.DicomToDataLakeOrchestratorJobIdentifierPropertiesV1),
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

        private string TryParseAsDicomToDataLakeProcessingJobInputData()
        {
            try
            {
                var inputData = JsonConvert.DeserializeObject<DicomToDataLakeProcessingJobInputData>(Definition);
                if (inputData is { JobType: JobType.Processing })
                {
                    return inputData.JobVersion switch
                    {
                        JobVersion.V1 => GetIdentifierText(inputData, DicomToDatalakeJobVersionManager.DicomToDataLakeProcessingJobIdentifierPropertiesV1),
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