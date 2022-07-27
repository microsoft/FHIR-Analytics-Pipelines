// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Core.Exceptions
{
    public class GroupMemberExtractorException : Exception
    {
        public GroupMemberExtractorException(string message)
            : base(message)
        {
        }

        public GroupMemberExtractorException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
