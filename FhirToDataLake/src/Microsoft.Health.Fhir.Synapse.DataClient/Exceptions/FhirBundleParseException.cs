// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Exceptions
{
    public class FhirBundleParseException : Exception
    {
        public FhirBundleParseException(string message)
            : base(message)
        {
        }

        public FhirBundleParseException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
