// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure.Core;

namespace Microsoft.Health.Fhir.Synapse.Common.Authentication
{
    public interface ITokenCredentialProvider
    {
        public TokenCredential GetCredential();
    }
}
