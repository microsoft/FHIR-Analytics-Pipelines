// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Core.Exceptions
{
    public class DataConversionException : DataSerializationException
    {
        public DataConversionException(string message)
            : base(message)
        {
        }

        public DataConversionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
