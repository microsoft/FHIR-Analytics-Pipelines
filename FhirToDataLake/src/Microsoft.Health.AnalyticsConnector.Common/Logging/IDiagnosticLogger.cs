// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.AnalyticsConnector.Common.Logging
{
    /// <summary>
    /// Diagnostic logger reports to customer's interface.
    /// </summary>
    public interface IDiagnosticLogger
    {
        public void LogInformation(string message);

        public void LogWarning(string message);

        public void LogError(string errorMessage);
    }
}