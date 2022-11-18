﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;

namespace Microsoft.Health.Fhir.Synapse.Core.Exceptions
{
    /// <summary>
    /// Failed to process internal metadata storage.
    /// </summary>
    public class MetadataStoreException : SynapsePipelineInternalException
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
