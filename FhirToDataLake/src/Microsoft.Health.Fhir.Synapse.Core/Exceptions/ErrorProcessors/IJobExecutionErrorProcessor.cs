// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Core.Exceptions.ErrorProcessors
{
    public interface IJobExecutionErrorProcessor
    {
        public void Process(Exception ex, string message = "");
    }
}
