﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.Fhir.Synapse.JobManagement.Extensions;

namespace Microsoft.Health.Fhir.Synapse.JobManagement.Models
{
    public class FhirToDataLakeAzureStorageJobInfo : AzureStorageJobInfo
    {
        public override string JobIdentifier()
        {
            return Definition.ComputeHash();
        }
    }
}