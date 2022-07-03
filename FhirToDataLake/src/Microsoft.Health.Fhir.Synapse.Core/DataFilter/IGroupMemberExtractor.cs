// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;

namespace Microsoft.Health.Fhir.Synapse.Core.DataFilter
{
    public interface IGroupMemberExtractor
    {
        /// <summary>
        /// Gets a list of Patients that are included in a Group, either as members or as members of nested Groups. Implicit members of a group are not included.
        /// Patients deduplication is executed on the result.
        /// </summary>
        /// <param name="groupId">The id of the Group</param>
        /// <param name="queryParameters"> Query parameters to filter groups.</param>
        /// <param name="groupMembershipTime">Only returns Patients that were in the Group at this time.</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>PatientWrapper list.</returns>
        public Task<List<PatientWrapper>> GetGroupPatients(
            string groupId,
            List<KeyValuePair<string, string>> queryParameters,
            DateTimeOffset groupMembershipTime,
            CancellationToken cancellationToken);
    }
}
