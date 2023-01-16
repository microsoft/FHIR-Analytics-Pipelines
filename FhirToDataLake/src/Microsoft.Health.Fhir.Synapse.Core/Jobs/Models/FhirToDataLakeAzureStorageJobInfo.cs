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
                        SupportedJobVersion.V1 => GetIdentifierText(inputData, JobVersionManager.FhirToDataLakeOrchestratorJobIdentifierPropertiesV1),
                        SupportedJobVersion.V2 => GetIdentifierText(inputData, JobVersionManager.FhirToDataLakeOrchestratorJobIdentifierPropertiesV2),
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
                        SupportedJobVersion.V1 => GetIdentifierText(inputData, JobVersionManager.FhirToDataLakeProcessingJobIdentifierPropertiesV1),
                        SupportedJobVersion.V2 => GetIdentifierText(inputData, JobVersionManager.FhirToDataLakeProcessingJobIdentifierPropertiesV2),
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
            // the job version is added in input data to handle possible version compatibility issues when the version is updated. It is not related to the job definition, so we need to remove the job version property when calculate job identifier.
            var inputDataJObject = JObject.FromObject(inputData);
            var identifierJObject = new JObject();
            foreach (var property in selectedProperties)
            {
                identifierJObject.Add(inputDataJObject.Property(property));
            }

            return identifierJObject.ToString(Formatting.None);
        }
    }
}