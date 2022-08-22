// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure.Data.Tables;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public interface IAzureTableClientFactory
    {
        public TableClient Create();
    }
}