// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.DataSerialization.Json.Exceptions
{
    public class JsonDataProcessorException : DataSerializationException
    {
        public JsonDataProcessorException(string message)
            : base(message)
        {
        }

        public JsonDataProcessorException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
