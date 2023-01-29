// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Health.AnalyticsConnector.Core.DataFilter
{
    public interface IGroupMemberExtractor
    {
        /// <summary>
        /// Gets a list of Patients that are included in a Group, either as members or as members of nested Groups. Implicit members of a group are not included.
        /// Patients deduplication is executed on the result.
        /// </summary>
        /// <param name="groupId">The group id.</param>
        /// <param name="queryParameters">The query parameters to filter groups.</param>
        /// <param name="groupMembershipTime">Only returns patients that were in the group at this time.</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Patient id hashset.</returns>
        Task<HashSet<string>> GetGroupPatientsAsync(
            string groupId,
            List<KeyValuePair<string, string>> queryParameters,
            DateTimeOffset groupMembershipTime,
            CancellationToken cancellationToken);
    }
}