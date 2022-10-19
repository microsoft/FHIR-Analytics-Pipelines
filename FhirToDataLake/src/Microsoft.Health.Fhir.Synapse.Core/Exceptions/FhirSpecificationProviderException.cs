// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;

namespace Microsoft.Health.Fhir.Synapse.Core.Exceptions
{
    public class FhirSpecificationProviderException : SynapsePipelineExternalException
    {
        public FhirSpecificationProviderException(string message)
            : base(message)
        {
        }

        public FhirSpecificationProviderException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
