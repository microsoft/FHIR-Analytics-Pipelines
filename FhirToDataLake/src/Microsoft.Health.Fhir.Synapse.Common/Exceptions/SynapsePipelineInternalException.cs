// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Common.Exceptions
{
    /// <summary>
    /// Internal resources dependency error.
    /// </summary>
    public class SynapsePipelineInternalException : Exception
    {
        public SynapsePipelineInternalException(string message)
            : base(message)
        {
        }

        public SynapsePipelineInternalException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
