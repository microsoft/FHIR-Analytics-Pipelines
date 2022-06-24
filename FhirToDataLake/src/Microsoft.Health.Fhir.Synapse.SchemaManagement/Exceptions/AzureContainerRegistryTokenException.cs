// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions
{
    public class AzureContainerRegistryTokenException : Exception
    {
        public AzureContainerRegistryTokenException(string message)
            : base(message)
        {
        }

        public AzureContainerRegistryTokenException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
