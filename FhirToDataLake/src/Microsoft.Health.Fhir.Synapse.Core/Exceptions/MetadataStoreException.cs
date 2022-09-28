// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Core.Exceptions
{
    /// <summary>
    /// Failed to start a job.
    /// </summary>
    public class MetadataStoreException : Exception
    {
        public MetadataStoreException(string message)
            : base(message)
        {
        }

        public MetadataStoreException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
