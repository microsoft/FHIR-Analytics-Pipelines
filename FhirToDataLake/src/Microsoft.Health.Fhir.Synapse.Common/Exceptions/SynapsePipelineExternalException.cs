// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Common.Exceptions
{
    /// <summary>
    /// External resources dependency error.
    /// </summary>
    public class SynapsePipelineExternalException : Exception
    {
        public SynapsePipelineExternalException(string message)
            : base(message)
        {
        }

        public SynapsePipelineExternalException(string message, Exception inner)
            : base(message, inner)
        {

        }
    }
}
