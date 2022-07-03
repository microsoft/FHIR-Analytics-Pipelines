// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption
{
    public class MetadataOptions : BaseFhirApiOptions
    {
        public override string RelativeUri()
        {
            return "metadata";
        }
    }
}
