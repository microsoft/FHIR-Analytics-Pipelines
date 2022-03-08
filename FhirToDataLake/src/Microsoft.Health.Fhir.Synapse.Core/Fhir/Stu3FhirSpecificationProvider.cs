// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------
extern alias FhirStu3;

using System.Collections.Generic;
using System.Linq;
using Stu3FhirModelInfo = FhirStu3::Hl7.Fhir.Model.ModelInfo;

namespace Microsoft.Health.Fhir.Synapse.Core.Fhir
{
    public class Stu3FhirSpecificationProvider : IFhirSpecificationProvider
    {
        private readonly IEnumerable<string> _excludeTypes = new List<string> { "StructureDefinition" };

        public IEnumerable<string> GetAllResourceTypes()
        {
            return Stu3FhirModelInfo.SupportedResources.Except(_excludeTypes);
        }

        public bool IsValidFhirResourceType(string resourceType)
        {
            return Stu3FhirModelInfo.IsKnownResource(resourceType);
        }
    }
}
