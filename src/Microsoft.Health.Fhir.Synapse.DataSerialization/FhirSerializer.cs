// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------
extern alias FhirStu3;
extern alias FhirR4;

using System;
using Hl7.Fhir.ElementModel;
using Hl7.Fhir.Serialization;
using Hl7.Fhir.Specification;
using R4StructureDefinitionSummaryProvider = FhirR4.Hl7.Fhir.Specification.PocoStructureDefinitionSummaryProvider;
using Stu3StructureDefinitionSummaryProvider = FhirStu3.Hl7.Fhir.Specification.PocoStructureDefinitionSummaryProvider;

namespace Microsoft.Health.Fhir.Synapse.DataSerialization
{
    public class FhirSerializer : IFhirSerializer
    {
        public const string FhirVersionR4 = "R4";
        public const string FhirVersionStu3 = "Stu3";

        private readonly IStructureDefinitionSummaryProvider _r4Provider = new R4StructureDefinitionSummaryProvider();
        private readonly IStructureDefinitionSummaryProvider _stu3Provider = new Stu3StructureDefinitionSummaryProvider();

        // To do: inject FHIR version value.
        private string TargetFhirVersion = "R4";

        public ITypedElement DeserializeToElement(
            string resourceContent,
            FhirJsonParsingSettings settings = null)
        {

            if (string.Equals(TargetFhirVersion, FhirVersionStu3, StringComparison.OrdinalIgnoreCase))
            {
                return FhirJsonNode.Parse(resourceContent, settings: settings).ToTypedElement(_stu3Provider);
            }
            else
            {
                return FhirJsonNode.Parse(resourceContent).ToTypedElement(_r4Provider);
            }
        }

        public string Serialize(
            ITypedElement element,
            FhirJsonSerializationSettings settings = null)
        {
            return element.ToJson(settings);
        }
    }
}
