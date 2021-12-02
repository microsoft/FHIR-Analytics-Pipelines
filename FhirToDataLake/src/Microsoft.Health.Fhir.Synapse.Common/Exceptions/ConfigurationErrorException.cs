// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Common.Exceptions
{
    /// <summary>
    /// Configuration error.
    /// </summary>
    public class ConfigurationErrorException : Exception
    {
        public ConfigurationErrorException(string message)
            : base(message)
        {
        }
    }
}
