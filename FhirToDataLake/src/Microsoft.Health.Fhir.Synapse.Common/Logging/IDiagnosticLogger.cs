// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common.Logging
{
    public interface IDiagnosticLogger
    {
        public void LogInformation(string message);

        public void LogWarning(string message);

        public void LogError(string errorMessage);
    }
}