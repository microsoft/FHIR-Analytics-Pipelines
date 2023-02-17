// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs.Models
{
    public class JobCandidatePool
    {
        private List<SubJobInfo> _jobCandidateList = new ();
        private int _currentResourceCount;

        public void PushJobCandidates(SubJobInfo subJob)
        {
            // Push small job into candidate list.
            _jobCandidateList.Add(subJob);
            _currentResourceCount += subJob.ResourceCount;
        }

        public List<SubJobInfo> PopJobCandidates()
        {
            if (!_jobCandidateList.Any())
            {
                return null;
            }

            List<SubJobInfo> result = new List<SubJobInfo>(_jobCandidateList);
            _jobCandidateList.Clear();
            _currentResourceCount = 0;
            return result;
        }

        public int GetResourceCount()
        {
            return _currentResourceCount;
        }
    }
}
