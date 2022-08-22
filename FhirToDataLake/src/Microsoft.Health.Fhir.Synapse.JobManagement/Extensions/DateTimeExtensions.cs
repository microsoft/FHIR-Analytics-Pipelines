// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.JobManagement.Extensions
{
    public static class DateTimeExtensions
    {
        public static DateTime SetKind(this DateTime dateTime, DateTimeKind dateTimeKind)
        {
            return DateTime.SpecifyKind(dateTime, dateTimeKind);
        }
    }
}