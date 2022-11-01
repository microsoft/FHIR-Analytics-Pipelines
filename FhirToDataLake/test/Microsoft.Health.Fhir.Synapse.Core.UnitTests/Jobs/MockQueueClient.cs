// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.JobManagement;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    public class MockQueueClient : IQueueClient
    {
        private List<JobInfo> _jobInfos = new List<JobInfo>();
        private long _largestId = 1;

        public Action EnqueueFaultAction { get; set; }

        public Action DequeueFaultAction { get; set; }

        public Action HeartbeatFaultAction { get; set; }

        public Action CompleteFaultAction { get; set; }

        public Func<MockQueueClient, long, CancellationToken, JobInfo> GetJobByIdFunc { get; set; }

        public List<JobInfo> JobInfos => _jobInfos;

        public bool Initialized { get; set; } = true;

        public Task CancelJobByGroupIdAsync(byte queueType, long groupId, CancellationToken cancellationToken)
        {
            foreach (JobInfo jobInfo in _jobInfos.Where(t => t.GroupId == groupId))
            {
                if (jobInfo.Status == JobStatus.Created)
                {
                    jobInfo.Status = JobStatus.Cancelled;
                }

                if (jobInfo.Status == JobStatus.Running)
                {
                    jobInfo.CancelRequested = true;
                }
            }

            return Task.CompletedTask;
        }

        public Task CancelJobByIdAsync(byte queueType, long jobId, CancellationToken cancellationToken)
        {
            foreach (JobInfo jobInfo in _jobInfos.Where(t => t.Id == jobId))
            {
                if (jobInfo.Status == JobStatus.Created)
                {
                    jobInfo.Status = JobStatus.Cancelled;
                }

                if (jobInfo.Status == JobStatus.Running)
                {
                    jobInfo.CancelRequested = true;
                }
            }

            return Task.CompletedTask;
        }

        public async Task CompleteJobAsync(JobInfo jobInfo, bool requestCancellationOnFailure, CancellationToken cancellationToken)
        {
            CompleteFaultAction?.Invoke();

            JobInfo jobInfoStore = _jobInfos.FirstOrDefault(t => t.Id == jobInfo.Id);
            jobInfoStore.Status = jobInfo.Status;
            jobInfoStore.Result = jobInfo.Result;

            if (requestCancellationOnFailure && jobInfo.Status == JobStatus.Failed)
            {
                await CancelJobByGroupIdAsync(jobInfo.QueueType, jobInfo.GroupId, cancellationToken);
            }
        }

        public Task<JobInfo> DequeueAsync(byte queueType, string worker, int heartbeatTimeoutSec, CancellationToken cancellationToken)
        {
            DequeueFaultAction?.Invoke();

            JobInfo job = _jobInfos.FirstOrDefault(t => t.Status == JobStatus.Created || (t.Status == JobStatus.Running && (DateTime.Now - t.HeartbeatDateTime) > TimeSpan.FromSeconds(heartbeatTimeoutSec)));
            if (job != null)
            {
                job.Status = JobStatus.Running;
                job.HeartbeatDateTime = DateTime.Now;
            }

            return Task.FromResult(job);
        }

        public Task<IEnumerable<JobInfo>> EnqueueAsync(byte queueType, string[] definitions, long? groupId, bool forceOneActiveJobGroup, bool isCompleted, CancellationToken cancellationToken)
        {
            EnqueueFaultAction?.Invoke();

            List<JobInfo> result = new List<JobInfo>();

            long gId = groupId ?? _largestId++;
            foreach (string definition in definitions)
            {
                if (_jobInfos.Any(t => t.Definition.Equals(definition)))
                {
                    result.Add(_jobInfos.First(t => t.Definition.Equals(definition)));
                    continue;
                }

                result.Add(new JobInfo
                {
                    Definition = definition,
                    Id = _largestId,
                    GroupId = gId,
                    Status = JobStatus.Created,
                    HeartbeatDateTime = DateTime.Now,
                });
                _largestId++;
            }

            _jobInfos.AddRange(result);
            return Task.FromResult<IEnumerable<JobInfo>>(result);
        }

        public Task<IEnumerable<JobInfo>> GetJobByGroupIdAsync(byte queueType, long groupId, bool returnDefinition, CancellationToken cancellationToken)
        {
            IEnumerable<JobInfo> result = _jobInfos.Where(t => t.GroupId == groupId);
            return Task.FromResult(result);
        }

        public Task<JobInfo> GetJobByIdAsync(byte queueType, long jobId, bool returnDefinition, CancellationToken cancellationToken)
        {
            if (GetJobByIdFunc != null)
            {
                return Task.FromResult(GetJobByIdFunc(this, jobId, cancellationToken));
            }

            JobInfo result = _jobInfos.FirstOrDefault(t => t.Id == jobId);
            return Task.FromResult(result);
        }

        public Task<IEnumerable<JobInfo>> GetJobsByIdsAsync(byte queueType, long[] jobIds, bool returnDefinition, CancellationToken cancellationToken)
        {
            if (GetJobByIdFunc != null)
            {
                return Task.FromResult(jobIds.Select(jobId => GetJobByIdFunc(this, jobId, cancellationToken)));
            }

            IEnumerable<JobInfo> result = _jobInfos.Where(t => jobIds.Contains(t.Id));
            return Task.FromResult(result);
        }

        public bool IsInitialized()
        {
            return Initialized;
        }

        public Task<bool> KeepAliveJobAsync(JobInfo jobInfo, CancellationToken cancellationToken)
        {
            HeartbeatFaultAction?.Invoke();

            JobInfo job = _jobInfos.FirstOrDefault(t => t.Id == jobInfo.Id);
            if (job == null)
            {
                throw new JobNotExistException("not exist");
            }

            job.HeartbeatDateTime = DateTime.Now;
            job.Result = jobInfo.Result;

            return Task.FromResult(job.CancelRequested);
        }
    }
}
