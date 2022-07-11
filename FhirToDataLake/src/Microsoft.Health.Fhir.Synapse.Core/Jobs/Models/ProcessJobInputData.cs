// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class ProcessJobInputData
    {
        public string FhirServerUrl { get; set; }

        public string DataLakeUrl { get; set; }

        public string ContainerName { get; set; }

        public IEnumerable<string> ResourceTypes { get; set; }

        public DateTimeOffset DataStart { get; set; }

        public DateTimeOffset DataEnd { get; set; }

        public int TypeId { get; set; }
    }
}
