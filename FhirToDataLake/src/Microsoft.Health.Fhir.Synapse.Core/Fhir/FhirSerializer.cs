// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------
extern alias FhirStu3;
extern alias FhirR4;

using EnsureThat;
using Hl7.Fhir.ElementModel;
using Hl7.Fhir.Serialization;
using Hl7.Fhir.Specification;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using R4StructureDefinitionSummaryProvider = FhirR4::Hl7.Fhir.Specification.PocoStructureDefinitionSummaryProvider;
using Stu3StructureDefinitionSummaryProvider = FhirStu3::Hl7.Fhir.Specification.PocoStructureDefinitionSummaryProvider;

namespace Microsoft.Health.Fhir.Synapse.Core.Fhir
{
    public class FhirSerializer : IFhirSerializer
    {
        private readonly FhirVersion _fhirVersion;

        private readonly IStructureDefinitionSummaryProvider _r4Provider = new R4StructureDefinitionSummaryProvider();
        private readonly IStructureDefinitionSummaryProvider _stu3Provider = new Stu3StructureDefinitionSummaryProvider();

        public FhirSerializer(IOptions<FhirServerConfiguration> fhirConfiguration)
        {
            EnsureArg.IsNotNull(fhirConfiguration, nameof(fhirConfiguration));

            _fhirVersion = fhirConfiguration.Value.Version;
        }

        public ITypedElement DeserializeToElement(
            string resourceContent,
            FhirJsonParsingSettings settings = null)
        {
            if (_fhirVersion == FhirVersion.Stu3)
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
