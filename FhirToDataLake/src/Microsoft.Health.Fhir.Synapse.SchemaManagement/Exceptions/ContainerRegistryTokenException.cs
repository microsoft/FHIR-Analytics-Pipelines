// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions
{
    public class ContainerRegistryTokenException : SynapsePipelineRetriableException
    {
        public ContainerRegistryTokenException(string message)
            : base(message)
        {
        }

        public ContainerRegistryTokenException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
