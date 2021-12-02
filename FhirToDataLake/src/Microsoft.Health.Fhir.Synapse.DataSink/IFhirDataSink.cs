// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.DataSink
{
    public interface IFhirDataSink
    {
        public string StorageUrl { get; }

        public string Location { get; }
    }
}
