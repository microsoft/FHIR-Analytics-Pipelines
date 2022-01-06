// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.DataWriter.Exceptions
{
    public class AzureBlobOperationFailedException : Exception
    {
        public AzureBlobOperationFailedException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
