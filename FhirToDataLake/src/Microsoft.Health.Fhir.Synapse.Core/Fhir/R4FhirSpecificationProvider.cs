// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------
extern alias FhirR4;

using System.Collections.Generic;
using System.Linq;
using R4FhirModelInfo = FhirR4::Hl7.Fhir.Model.ModelInfo;

namespace Microsoft.Health.Fhir.Synapse.Core.Fhir
{
    public class R4FhirSpecificationProvider : IFhirSpecificationProvider
    {
        private readonly IEnumerable<string> _excludeTypes = new List<string> { "StructureDefinition" };

        public IEnumerable<string> GetAllResourceTypes()
        {
            return R4FhirModelInfo.SupportedResources.Except(_excludeTypes);
        }

        public bool IsValidFhirResourceType(string resourceType)
        {
            return R4FhirModelInfo.IsKnownResource(resourceType);
        }
    }
}
