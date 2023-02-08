// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Health.AnalyticsConnector.DataClient.Api.Dicom;

namespace Microsoft.Health.AnalyticsConnector.DataClient.Models.DicomApiOption
{
    public class ChangeFeedLatestOptions : BaseApiOptions
    {
        public ChangeFeedLatestOptions(List<KeyValuePair<string, string>> queryParameters)
        {
            QueryParameters = queryParameters ?? new List<KeyValuePair<string, string>>();
            IsAccessTokenRequired = true;
        }

        public override string RelativeUri()
        {
            return $"{DicomApiConstants.ChangeFeedKey}/{DicomApiConstants.LatestKey}";
        }
    }
}
