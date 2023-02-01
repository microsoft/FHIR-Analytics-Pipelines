// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------
extern alias FhirStu3;

using System;
using System.Collections.Generic;
using System.Linq;
using Stu3FhirModelInfo = FhirStu3::Hl7.Fhir.Model.ModelInfo;

namespace Microsoft.Health.AnalyticsConnector.Core.Fhir.SpecificationProviders
{
    public class Stu3FhirSpecificationProvider : BaseFhirSpecificationProvider
    {
        public Stu3FhirSpecificationProvider()
            : base(null, null, null)
        {
        }

        protected override IEnumerable<string> CompartmentEmbeddedFiles { get; } = null;

        protected override string SearchParameterEmbeddedFile { get; } = null;

        public override IEnumerable<string> GetAllResourceTypes()
        {
            return Stu3FhirModelInfo.SupportedResources.Except(ExcludeTypes);
        }

        public override bool IsValidFhirResourceType(string resourceType)
        {
            return Stu3FhirModelInfo.IsKnownResource(resourceType);
        }

        protected override FhirCapabilityData BuildCapabilityDataFromMetadata(string metaData)
        {
            throw new NotImplementedException();
        }

        protected override Dictionary<string, HashSet<string>> BuildCompartmentResourceTypesLookupFromCompartmentContext(string compartmentContext, string compartmentFile)
        {
            throw new NotImplementedException();
        }

        protected override bool IsValidCompartmentType(string compartmentType)
        {
            throw new NotImplementedException();
        }
    }
}