// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Common.Logging
{
    public class DiagnosticLogger : IDiagnosticLogger
    {
        public void LogInformation(string message)
        {
            Console.WriteLine("Info: " + message);
        }

        public void LogWarning(string message)
        {
            Console.WriteLine("Warning: " + message);
        }

        public void LogError(string errorMessage)
        {
            Console.WriteLine("Error: " + errorMessage);
        }
    }
}