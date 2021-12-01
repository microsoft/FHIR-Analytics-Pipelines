﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Scheduler.Exceptions
{
    public class CompleteJobFailedException : Exception
    {
        public CompleteJobFailedException(string message)
            : base(message)
        {
        }

        public CompleteJobFailedException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
