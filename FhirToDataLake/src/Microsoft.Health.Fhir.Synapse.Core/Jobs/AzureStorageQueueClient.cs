// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Azure.Identity;
using Azure.Storage.Queues;
using AzureTableTaskQueue.Extensions;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class AzureStorageQueueClient : IQueueClient
    {
        private readonly TableClient _tableClient;
        private readonly QueueClient _queueClient;

        private readonly JobConfiguration _jobConfiguration;
        private readonly ILogger<AzureStorageQueueClient> _logger;

        private ConcurrentDictionary<JobInfo, Tuple<string, string>> jobMessageMap = new ConcurrentDictionary<JobInfo, Tuple<string, string>>();
        private const int QueueMessageVisibilityTimeoutInSeconds = 60;

        public AzureStorageQueueClient(
            IOptions<JobConfiguration> jobConfiguration,
            ILogger<AzureStorageQueueClient> logger)
        {
            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _jobConfiguration = jobConfiguration.Value;
            _logger = logger;

            _tableClient = new TableClient(new Uri(_jobConfiguration.TableUrl), _jobConfiguration.AgentId, new DefaultAzureCredential());
            _queueClient = new QueueClient(new Uri($"{_jobConfiguration.QueueUrl}/{_jobConfiguration.AgentId}"), new DefaultAzureCredential());
            _tableClient.CreateIfNotExists();
            _queueClient.CreateIfNotExists();
        }

        public async Task CancelJobByGroupIdAsync(byte queueType, long groupId, CancellationToken cancellationToken)
        {
            var jobEntities = new List<TableEntity>();

            // Query all job ids in a group, using range query for rowkey to ignore lock/index entities in same partition
            var queryResult = _tableClient.QueryAsync<TableEntity>(filter: $"PartitionKey eq '{JobTableKeyProvider.JobPartitionKey(queueType, groupId)}' and RowKey ge '{groupId:D20}' and RowKey lt '{groupId + 1:D20}'", cancellationToken: cancellationToken);
            await foreach (var pageResult in queryResult.AsPages())
            {
                foreach (var entity in pageResult.Values)
                {
                    entity["CancelRequested"] = true;
                    jobEntities.Add(entity);
                }
            }

            IEnumerable<TableTransactionAction> transactionActions = jobEntities.Select(entity => new TableTransactionAction(TableTransactionActionType.UpdateMerge, entity));
            var responseList = await _tableClient.SubmitTransactionAsync(transactionActions, cancellationToken);
            var batchFailed = responseList.Value.Any(response => response.IsError);
            if (batchFailed)
            {
                string errorMessage = responseList.Value.Where(response => response.IsError).Select(response => response.ReasonPhrase).First();
                _logger.LogError($"Failed to cancel jobs in group {groupId}. Reason: {errorMessage}");
                throw new Exception($"Failed to cancel jobs in group {groupId}. Reason: {errorMessage}");
            }

            _logger.LogInformation($"Cancel jobs in group {groupId} successfully.");
        }

        public async Task CancelJobByIdAsync(byte queueType, long jobId, CancellationToken cancellationToken)
        {
            // Load reverse index entity and get job partition/row key.
            var reversePartitionKey = JobTableKeyProvider.JobReverseIndexPartitionKey(queueType, jobId);
            var reverseIndexResponse = await _tableClient.GetEntityAsync<TableEntity>(reversePartitionKey, reversePartitionKey, cancellationToken: cancellationToken);
            var reverseIndexEntity = reverseIndexResponse.Value;
            string partitionKey = reverseIndexEntity["pk"].ToString();
            string rowKey = reverseIndexEntity["rk"].ToString();

            var response = await _tableClient.GetEntityAsync<TableEntity>(partitionKey, rowKey, cancellationToken: cancellationToken);
            var entity = response.Value;
            entity["CancelRequested"] = true;
            await _tableClient.UpdateEntityAsync(entity, ETag.All, cancellationToken: cancellationToken);

            _logger.LogInformation($"Cancel job {jobId} successfully.");
        }

        public async Task CompleteJobAsync(JobInfo jobInfo, bool requestCancellationOnFailure, CancellationToken cancellationToken)
        {
            var entity = jobInfo.ToTableEntity();

            await _tableClient.UpdateEntityAsync(entity, ETag.All, cancellationToken: cancellationToken);

            var message = jobMessageMap[jobInfo];
            string messageId = message.Item1;
            string messagePopreceipt = message.Item2;
            await _queueClient.DeleteMessageAsync(messageId, messagePopreceipt, cancellationToken: cancellationToken);

            jobMessageMap.TryRemove(jobInfo, out _);

            if (requestCancellationOnFailure && jobInfo.Status == JobStatus.Failed)
            {
                await CancelJobByGroupIdAsync(jobInfo.QueueType, jobInfo.GroupId, cancellationToken);
            }

            _logger.LogInformation($"Complete job {jobInfo.Id} successfully.");
        }

        public async Task<JobInfo> DequeueAsync(byte queueType, string worker, int heartbeatTimeoutSec, CancellationToken cancellationToken)
        {
            var message = await _queueClient.ReceiveMessageAsync(TimeSpan.FromSeconds(QueueMessageVisibilityTimeoutInSeconds), cancellationToken);
            if (message.Value != null)
            {
                var jobMessage = JsonConvert.DeserializeObject<JobMessage>(message.Value.Body.ToString());
                var jobResponse = await _tableClient.GetEntityAsync<TableEntity>(jobMessage.PartitionKey, jobMessage.RowKey, cancellationToken: cancellationToken);
                var job = jobResponse.Value.ToJobInfo();
                var messageId = jobResponse.Value["MessageId"]?.ToString();
                if (job.Status == JobStatus.Completed
                    || job.Status == JobStatus.Failed
                    || job.Status == JobStatus.Cancelled
                    || !string.Equals(messageId, message.Value.MessageId, StringComparison.OrdinalIgnoreCase))
                {
                    _logger.LogWarning($"Discard queue message {message.Value.MessageId}");
                    await _queueClient.DeleteMessageAsync(message.Value.MessageId, message.Value.PopReceipt);
                    return null;
                }

                if (job.Status == JobStatus.Running && job.HeartbeatDateTime.AddSeconds(heartbeatTimeoutSec) > DateTime.UtcNow)
                {
                    _logger.LogWarning($"Job {job.Id} is still active.");
                    return null;
                }

                job.Status = JobStatus.Running;
                job.Version = DateTimeOffset.UtcNow.Ticks;
                try
                {
                    await _tableClient.UpdateEntityAsync(job.ToTableEntity(), jobResponse.Value.ETag, cancellationToken: cancellationToken);
                    jobMessageMap[job] = new Tuple<string, string>(message.Value.MessageId, message.Value.PopReceipt);
                }
                catch (RequestFailedException ex) when (ex.ErrorCode == "ConditionNotMet")
                {
                    _logger.LogWarning(ex, $"Failed to update job status when dequeueing job {job.Id}");
                    return null;
                }

                _logger.LogInformation($"Successfully dequeued job '{job.Id}'.");
                return job;
            }

            return null;
        }

        public async Task<IEnumerable<JobInfo>> EnqueueAsync(byte queueType, string[] definitions, long? groupId, bool forceOneActiveJobGroup, bool isCompleted, CancellationToken cancellationToken)
        {
            // Step 1: insert job entities and job lock entities in one transaction.
            List<long> jobIds = await GetIncrementalJobIds(definitions.Count());
            List<JobInfo> jobs = new List<JobInfo>();
            for (int i = 0; i < definitions.Length; i++)
            {
                var job = new JobInfo();
                job.QueueType = queueType;
                job.Id = jobIds[i];
                job.Definition = definitions[i];
                job.GroupId = groupId ?? 0;
                job.CancelRequested = false;
                job.CreateDate = DateTime.UtcNow;
                job.HeartbeatDateTime = DateTime.UtcNow;
                jobs.Add(job);
            }

            List<TableEntity> jobEntities = jobs.Select(job => job.ToTableEntity()).ToList();
            List<TableEntity> jobLockEntities = new List<TableEntity>();
            for (int i = 0; i < definitions.Length; i++)
            {
                var lockEntity = new TableEntity(jobEntities[i].PartitionKey, JobTableKeyProvider.JobLockKey(definitions[i]));
                lockEntity["rk"] = jobEntities[i].RowKey;
                jobLockEntities.Add(lockEntity);
            }

            IEnumerable<TableTransactionAction> transactionActions = jobEntities
                .Select(entity => new TableTransactionAction(TableTransactionActionType.Add, entity))
                .Concat(jobLockEntities.Select(entity => new TableTransactionAction(TableTransactionActionType.Add, entity)));

            try
            {
                var responseList = await _tableClient.SubmitTransactionAsync(transactionActions, cancellationToken);
            }
            catch (RequestFailedException ex) when (string.Equals(ex.ErrorCode, "EntityAlreadyExists", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning("Insert multiple job records conflicted. Will fetch existing jobs.");
                List<TableEntity> existingJobEntities = new List<TableEntity>();
                foreach (var lockEntity in jobLockEntities)
                {
                    var existingIndexEntity = await _tableClient.GetEntityAsync<TableEntity>(lockEntity.PartitionKey, lockEntity.RowKey);
                    var existingJobEntity = await _tableClient.GetEntityAsync<TableEntity>(lockEntity.PartitionKey, existingIndexEntity.Value["rk"].ToString());
                    existingJobEntities.Add(existingJobEntity.Value);
                }

                jobEntities = existingJobEntities;
            }

            // Step 2: if queue message not present in job entities, insert reverse index entity and push message to queue.
            foreach (var entity in jobEntities)
            {
                if (string.IsNullOrEmpty(entity["MessageId"]?.ToString()))
                {
                    // reverse index for queueType_jobId
                    var reversePartitionKey = JobTableKeyProvider.JobReverseIndexPartitionKey(_jobConfiguration.QueueType, (long)entity["Id"]);
                    var reverseIdIndexEntity = new TableEntity(reversePartitionKey, reversePartitionKey);
                    reverseIdIndexEntity["pk"] = entity.PartitionKey;
                    reverseIdIndexEntity["rk"] = entity.RowKey;
                    await _tableClient.AddEntityAsync(reverseIdIndexEntity);

                    var response = await _queueClient.SendMessageAsync(new JobMessage(entity.PartitionKey, entity.RowKey).ToString());
                    var messageId = response.Value.MessageId;
                    entity["MessageId"] = messageId;
                    await _tableClient.UpdateEntityAsync(entity, ETag.All);
                }
            }

            _logger.LogInformation($"Successfully enqueued job '{string.Join(",", jobIds)}'.");

            return jobs;
        }

        // return definition is ignored for now.
        public async Task<IEnumerable<JobInfo>> GetJobByGroupIdAsync(byte queueType, long groupId, bool returnDefinition, CancellationToken cancellationToken)
        {
            var jobs = new List<JobInfo>();
            var queryResult = _tableClient.QueryAsync<TableEntity>(filter: $"PartitionKey eq '{JobTableKeyProvider.JobPartitionKey(queueType, groupId)}' and RowKey ge '{groupId:D20}' and RowKey lt '{groupId + 1:D20}'", cancellationToken: cancellationToken);
            await foreach (var pageResult in queryResult.AsPages())
            {
                jobs.AddRange(pageResult.Values.Select(entity => entity.ToJobInfo()));
            }

            _logger.LogInformation($"Successfully queried jobs in group {groupId}.");
            return jobs;
        }

        // return definition is ignored for now.
        public async Task<JobInfo> GetJobByIdAsync(byte queueType, long jobId, bool returnDefinition, CancellationToken cancellationToken)
        {
            var reversePartitionKey = JobTableKeyProvider.JobReverseIndexPartitionKey(_jobConfiguration.QueueType, jobId);
            var reverseIndexResponse = await _tableClient.GetEntityAsync<TableEntity>(reversePartitionKey, reversePartitionKey, cancellationToken: cancellationToken);
            var reverseIndexEntity = reverseIndexResponse.Value;
            string partitionKey = reverseIndexEntity["pk"].ToString();
            string rowKey = reverseIndexEntity["rk"].ToString();

            var response = await _tableClient.GetEntityAsync<TableEntity>(partitionKey, rowKey, cancellationToken: cancellationToken);

            return response.Value.ToJobInfo();
        }

        // return definition is ignored for now.
        public async Task<IEnumerable<JobInfo>> GetJobsByIdsAsync(byte queueType, long[] jobIds, bool returnDefinition, CancellationToken cancellationToken)
        {
            var result = new List<JobInfo>();
            foreach (var id in jobIds)
            {
                result.Add(await GetJobByIdAsync(queueType, id, returnDefinition, cancellationToken));
            }

            return result;
        }

        public bool IsInitialized()
        {
            try
            {
                _tableClient.CreateIfNotExists();
                _queueClient.CreateIfNotExists();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Initialize job store failed.");
                return false;
            }

            return true;
        }

        public async Task<JobInfo> KeepAliveJobAsync(JobInfo jobInfo, CancellationToken cancellationToken)
        {
            string partitionKey = JobTableKeyProvider.JobPartitionKey(_jobConfiguration.QueueType, jobInfo.GroupId);
            string rowKey = JobTableKeyProvider.JobRowKey(jobInfo.GroupId, jobInfo.Id);
            var jobEntity = (await _tableClient.GetEntityAsync<TableEntity>(partitionKey, rowKey)).Value;
            if ((long)jobEntity["Version"] != jobInfo.Version)
            {
                _logger.LogError($"Job {jobInfo.Id} precondition failed, version does not match.");
                throw new JobNotExistException($"Job {jobInfo.Id} precondition failed, version does not match.");
            }

            jobEntity["HeartbeatDateTime"] = DateTime.UtcNow;
            jobEntity["Result"] = jobInfo.Result;

            await _tableClient.UpdateEntityAsync(jobEntity, jobEntity.ETag, cancellationToken: cancellationToken);
            if (jobMessageMap.TryGetValue(jobInfo, out Tuple<string, string> message))
            {
                string messageId = message.Item1;
                string messagePopreceipt = message.Item2;
                try
                {
                    var response = await _queueClient.UpdateMessageAsync(messageId, messagePopreceipt, visibilityTimeout: TimeSpan.FromMinutes(1));
                    var updatedMessage = response.Value;
                    jobMessageMap[jobInfo] = new Tuple<string, string>(messageId, updatedMessage.PopReceipt);
                }
                catch (RequestFailedException ex) when (string.Equals(ex.ErrorCode, "MessageNotFound", StringComparison.OrdinalIgnoreCase))
                {
                    if (jobInfo.Status == JobStatus.Completed)
                    {
                        _logger.LogInformation($"Job {jobInfo.Id} has been completed. Keep alive failed");
                    }
                }
            }

            return jobInfo;
        }

        private async Task<List<long>> GetIncrementalJobIds(int count)
        {
            string partitionKey = JobTableKeyProvider.JobIdPartitionKey(_jobConfiguration.QueueType);

            JobIdEntity entity;
            try
            {
                var response = await _tableClient.GetEntityAsync<JobIdEntity>(partitionKey, partitionKey);
                entity = response.Value;
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == "ResourceNotFound")
            {
                // Create new entity if not exist
                var newEntity = new JobIdEntity
                {
                    PartitionKey = partitionKey,
                    RowKey = partitionKey,
                    Id = 0,
                };
                await _tableClient.AddEntityAsync(newEntity);
                entity = (await _tableClient.GetEntityAsync<JobIdEntity>(partitionKey, partitionKey)).Value;
            }

            var result = new List<long>();
            for (int i = 0; i < count; i++)
            {
                result.Add(entity.Id + i + 1);
            }

            entity.Id += count;
            try
            {
                await _tableClient.UpdateEntityAsync(entity, entity.ETag);
                return result;
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == "ConditionNotMet")
            {
                _logger.LogWarning("Update id entity conflicts, will try again.");
                return await GetIncrementalJobIds(count);
            }
        }
    }
}
