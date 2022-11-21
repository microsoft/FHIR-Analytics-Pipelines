// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Models.DicomApiOption
{
    public class ChangeFeedOffsetOptions : BaseDicomApiOptions
    {
        public ChangeFeedOffsetOptions(
            DicomApiVersion dicomApiVersion,
            List<KeyValuePair<string, string>> queryParameters)
        {
            DicomApiVersion = dicomApiVersion;
            QueryParameters = queryParameters ?? new List<KeyValuePair<string, string>>();
            IsAccessTokenRequired = true;
        }

        public override string RelativeUri()
        {
            return $"{DicomApiConstants.VersionMap[DicomApiVersion]}/{DicomApiConstants.ChangeFeedKey}";
        }
    }
}
