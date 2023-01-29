// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.AnalyticsConnector.DataClient.Api.Fhir;

namespace Microsoft.Health.AnalyticsConnector.DataClient.Models.FhirApiOption
{
    public class MetadataOptions : BaseApiOptions
    {
        public override string RelativeUri()
        {
            return FhirApiConstants.MetadataKey;
        }
    }
}
