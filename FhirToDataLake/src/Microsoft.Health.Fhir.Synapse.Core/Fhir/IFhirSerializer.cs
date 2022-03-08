// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Hl7.Fhir.ElementModel;
using Hl7.Fhir.Serialization;

namespace Microsoft.Health.Fhir.Synapse.Core.Fhir
{
    public interface IFhirSerializer
    {
        public string Serialize(ITypedElement element, FhirJsonSerializationSettings settings = null);

        public ITypedElement DeserializeToElement(string resource, FhirJsonParsingSettings settings = null);
    }
}
