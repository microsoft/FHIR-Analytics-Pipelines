// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Models
{
    public class BaseApiOptions
    {
        public List<KeyValuePair<string, string>> QueryParameters { get; set; } = null;

        public bool IsAccessTokenRequired { get; set; } = false;

        public virtual string RelativeUri()
        {
            return string.Empty;
        }
    }
}
