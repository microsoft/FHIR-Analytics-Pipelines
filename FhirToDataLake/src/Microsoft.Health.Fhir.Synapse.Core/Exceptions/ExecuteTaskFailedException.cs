// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Core.Exceptions
{
    public class ExecuteTaskFailedException : Exception
    {
        public ExecuteTaskFailedException(string message)
            : base(message)
        {
        }

        public ExecuteTaskFailedException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
