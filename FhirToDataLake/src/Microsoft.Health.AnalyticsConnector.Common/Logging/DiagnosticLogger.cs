// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Diagnostics;

namespace Microsoft.Health.AnalyticsConnector.Common.Logging
{
    public class DiagnosticLogger : IDiagnosticLogger
    {
        public void LogInformation(string message)
        {
            Debug.WriteLine("Info: " + message);
        }

        public void LogWarning(string message)
        {
            Debug.WriteLine("Warning: " + message);
        }

        public void LogError(string errorMessage)
        {
            Debug.WriteLine("Error: " + errorMessage);
        }
    }
}