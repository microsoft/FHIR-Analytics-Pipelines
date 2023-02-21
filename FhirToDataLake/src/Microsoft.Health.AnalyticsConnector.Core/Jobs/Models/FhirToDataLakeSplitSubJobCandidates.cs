// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs.Models
{
    public class FhirToDataLakeSplitSubJobCandidates
    {
        private List<FhirToDataLakeSplitSubJobInfo> _jobCandidateList = new ();
        private int _currentResourceCount;

        public void AddJobCandidates(FhirToDataLakeSplitSubJobInfo subJob)
        {
            // Add small job into candidate list.
            _jobCandidateList.Add(subJob);
            _currentResourceCount += subJob.ResourceCount;
        }

        public FhirToDataLakeSplitProcessingJobInfo GenerateProcessingJob()
        {
            if (!_jobCandidateList.Any())
            {
                _currentResourceCount = 0;
                return new FhirToDataLakeSplitProcessingJobInfo();
            }

            FhirToDataLakeSplitProcessingJobInfo result = new ()
            {
                ResourceCount = _currentResourceCount,
                SubJobInfos = new List<FhirToDataLakeSplitSubJobInfo>(_jobCandidateList),
            };
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
