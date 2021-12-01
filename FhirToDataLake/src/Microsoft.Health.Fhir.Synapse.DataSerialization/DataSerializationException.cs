﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.DataSerialization
{
    public class DataSerializationException : Exception
    {
        public DataSerializationException(string message)
            : base(message)
        {
        }

        public DataSerializationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
