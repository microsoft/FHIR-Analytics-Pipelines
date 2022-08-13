// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure;
using Azure.Data.Tables;
using Azure.Identity;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.JobManagement.Extensions;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models.AzureStorage;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.JobManagement
{
    public class AzureStorageQueueClient<TJobInfo> : IQueueClient
        where TJobInfo : AzureStorageJobInfo, new()
    {
        private readonly TableClient _azureJobInfoTableClient;

        private readonly QueueClient _azureJobMessageQueueClient;

        private readonly ILogger<AzureStorageQueueClient<TJobInfo>> _logger;

        private const int QueueMessageVisibilityTimeoutInSeconds = 60;

        public AzureStorageQueueClient(
            IStorage storage,
            IOptions<JobConfiguration> jobConfiguration,
            ILogger<AzureStorageQueueClient<TJobInfo>> logger)
        {
            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            EnsureArg.IsNotNull(storage, nameof(storage));

            _logger = EnsureArg.IsNotNull(logger, nameof(logger));

            // TODO: create table client with only uri
            if (storage.UseConnectionString)
            {
                _azureJobInfoTableClient = new TableClient(storage.TableUrl, storage.TableName);
                _azureJobMessageQueueClient = new QueueClient(storage.QueueUrl, storage.QueueName);
            }
            else
            {
                _azureJobInfoTableClient = new TableClient(
                    new Uri(storage.TableUrl),
                    storage.TableName,
                    new DefaultAzureCredential());
                _azureJobMessageQueueClient = new QueueClient(
                    new Uri($"{storage.QueueUrl}{storage.QueueName}"),
                    new DefaultAzureCredential());
            }
        }

        public bool IsInitialized()
        {
            try
            {
                _azureJobInfoTableClient.CreateIfNotExists();
                _azureJobMessageQueueClient.CreateIfNotExists();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize azure storage client.");
                return false;
            }

            _logger.LogInformation("Initialize azure storage client successfully.");
            return true;
        }

        public async Task<IEnumerable<JobInfo>> EnqueueAsync(
            byte queueType,
            string[] definitions,
            long? groupId,
            bool forceOneActiveJobGroup,
            bool isCompleted,
            CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start to enqueue {definitions.Length} jobs.");

            // step 1: get incremental job ids
            var jobIds = await GetIncrementalJobIds(queueType, definitions.Length, cancellationToken);

            // handle for each job
            var jobInfos = new List<TJobInfo>();
            for (var i = 0; i < definitions.Length; i++)
            {
                // step 2: create jobInfo entity and job lock entity
                var newJobInfo = new TJobInfo
                {
                    Id = jobIds[i],
                    QueueType = queueType,
                    Status = JobStatus.Created,
                    GroupId = groupId ?? 0,
                    Definition = definitions[i],
                    Result = string.Empty,
                    CancelRequested = false,
                    CreateDate = DateTime.UtcNow,
                    HeartbeatDateTime = DateTime.UtcNow,
                };

                var jobInfoEntity = newJobInfo.ToTableEntity();

                var jobLockEntity = new JobLockEntity
                {
                    PartitionKey = jobInfoEntity.PartitionKey,
                    RowKey = AzureStorageKeyProvider.JobLockRowKey(newJobInfo.JobIdentifier()),
                    JobInfoEntityRowKey = jobInfoEntity.RowKey,
                };

                // step 3: insert jobInfo entity and job lock entity in one transaction.
                IEnumerable<TableTransactionAction> transactionAddActions = new List<TableTransactionAction>
                {
                    new (TableTransactionActionType.Add, jobInfoEntity),
                    new (TableTransactionActionType.Add, jobLockEntity),
                };

                try
                {
                    _ = await _azureJobInfoTableClient.SubmitTransactionAsync(transactionAddActions, cancellationToken);
                }
                catch (RequestFailedException ex) when (IsSpecifiedErrorCode(ex, AzureStorageErrorCode.AddEntityAlreadyExistsErrorCode))
                {
                    // TODO: need to add unit tests for the following cases
                    // case 1: multi agent instances enqueue job at the same time
                    // case 2: enqueue fails due to the below steps, while add entity successfully, when retry to enqueue, the entity exists
                    // case 3: re-enqueue running processing/orchestrator job
                    // Note: for cancelled/failed jobs, we don't allow resume it.
                    _logger.LogWarning(ex,"Failed to add entities, the entities already exist. Will fetch the existing jobs.");
                }

                // step 4: get the existing or newly added jobInfo entity and job lock entity
                jobLockEntity = (await _azureJobInfoTableClient.GetEntityAsync<JobLockEntity>(
                    jobLockEntity.PartitionKey,
                    jobLockEntity.RowKey,
                    cancellationToken: cancellationToken)).Value;
                jobInfoEntity = (await _azureJobInfoTableClient.GetEntityAsync<JobInfoEntity>(
                    jobLockEntity.PartitionKey,
                    jobLockEntity.JobInfoEntityRowKey,
                    cancellationToken: cancellationToken)).Value;

                // step 5: try to add reverse index for jobInfo entity
                try
                {
                    var reverseIndexEntity = new JobReverseIndexEntity
                    {
                        PartitionKey = AzureStorageKeyProvider.JobReverseIndexPartitionKey(queueType, jobInfoEntity.Id),
                        RowKey = AzureStorageKeyProvider.JobReverseIndexRowKey(queueType, jobInfoEntity.Id),
                        JobInfoEntityPartitionKey = jobInfoEntity.PartitionKey,
                        JobInfoEntityRowKey = jobInfoEntity.RowKey,
                    };

                    await _azureJobInfoTableClient.AddEntityAsync(reverseIndexEntity, cancellationToken: cancellationToken);
                }
                catch (RequestFailedException ex) when (IsSpecifiedErrorCode(ex, AzureStorageErrorCode.AddEntityAlreadyExistsErrorCode))
                {
                    _logger.LogInformation(ex, "The job reverse index entity already exists.");
                }

                // step 6: if queue message not present in job lock entity, push message to queue.
                // TODO: add unit test: What if processing job failed, and the message is deleted, while the message id is still in table entity
                if (string.IsNullOrEmpty(jobLockEntity.JobMessageId))
                {
                    var response = await _azureJobMessageQueueClient.SendMessageAsync(new JobMessage(jobInfoEntity.PartitionKey, jobInfoEntity.RowKey).ToString(), cancellationToken);

                    jobLockEntity.JobMessagePopReceipt = response.Value.PopReceipt;
                    jobLockEntity.JobMessageId = response.Value.MessageId;

                    // step 7: update message id and message pop receipt to job lock entity
                    await _azureJobInfoTableClient.UpdateEntityAsync(
                        jobLockEntity,
                        jobLockEntity.ETag,
                        cancellationToken: cancellationToken);

                    // TODO: need to update jobInfoEntity
                    /*
                    IEnumerable<TableTransactionAction> transactionUpdateActions = new List<TableTransactionAction>
                    {
                        new (TableTransactionActionType.UpdateReplace, jobInfoEntity),
                        new (TableTransactionActionType.UpdateReplace, jobLockEntity),
                    };

                    _ = await _azureJobInfoTableClient.SubmitTransactionAsync(transactionUpdateActions, cancellationToken);
                    */
                }

                jobInfos.Add(jobInfoEntity.ToJobInfo<TJobInfo>());
            }

            _logger.LogInformation($"Enqueue jobs '{string.Join(",", jobInfos.Select(jobInfo => jobInfo.Id).ToList())}' successfully.");

            return jobInfos;
        }

        public async Task<JobInfo> DequeueAsync(byte queueType, string worker, int heartbeatTimeoutSec, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Start to dequeue.");

            // step 1: receive message from message queue
            var message = await _azureJobMessageQueueClient.ReceiveMessageAsync(
                TimeSpan.FromSeconds(QueueMessageVisibilityTimeoutInSeconds),
                cancellationToken);

            if (message.Value == null)
            {
                _logger.LogWarning($"Failed to receive message from queue, the queue message is null.");
                return null;
            }

            var jobMessage = JsonConvert.DeserializeObject<JobMessage>(message.Value.Body.ToString());

            if (jobMessage == null)
            {
                _logger.LogWarning("Failed to deserialize message, the deserialized job message is null.");
                return null;
            }

            // step 2: get jobInfo entity to check job status, delete this message if job is already completed/failed/cancelled
            var jobInfoEntityResponse = await _azureJobInfoTableClient.GetEntityAsync<JobInfoEntity>(
                jobMessage.PartitionKey,
                jobMessage.RowKey,
                cancellationToken: cancellationToken);
            var jobInfo = jobInfoEntityResponse.Value.ToJobInfo<TJobInfo>();

            if (jobInfo.Status is JobStatus.Completed
                    or JobStatus.Failed
                    or JobStatus.Cancelled)
            {
                _logger.LogWarning($"Discard queue message {message.Value.MessageId}, the job status is {jobInfo.Status}.");
                await _azureJobMessageQueueClient.DeleteMessageAsync(
                    message.Value.MessageId,
                    message.Value.PopReceipt,
                    cancellationToken);
                return null;
            }

            // step 3: get job lock entity to check message id
            var jobLockEntityResponse = await _azureJobInfoTableClient.GetEntityAsync<JobLockEntity>(
                jobMessage.PartitionKey,
                AzureStorageKeyProvider.JobLockRowKey(jobInfo.JobIdentifier()),
                cancellationToken: cancellationToken);

            var jobLockEntity = jobLockEntityResponse.Value;

            if (!string.Equals(jobLockEntity.JobMessageId, message.Value.MessageId, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning($"Discard queue message {message.Value.MessageId}, the message id is inconsistent with the one in the table entity.");
                await _azureJobMessageQueueClient.DeleteMessageAsync(
                    message.Value.MessageId,
                    message.Value.PopReceipt,
                    cancellationToken);
                return null;
            }

            // step 4: skip it if the job is running and still active
            if (jobInfo.Status == JobStatus.Running && jobInfo.HeartbeatDateTime.AddSeconds(heartbeatTimeoutSec) > DateTime.UtcNow)
            {
                _logger.LogWarning($"Job {jobInfo.Id} is still active.");
                return null;
            }

            // step 5: update jobInfo entity's status to running, also update version and heartbeat
            // TODO: there may be multi running jobs for this jobInfo, how to handle it
            jobInfo.Status = JobStatus.Running;

            // TODO: where to update the job startDate and endDate
            // TODO: need to use version here？
            jobInfo.Version = DateTimeOffset.UtcNow.Ticks;
            jobInfo.HeartbeatDateTime = DateTime.UtcNow;

            var jobInfoEntity = jobInfo.ToTableEntity();

            // step 6: update message pop receipt to job lock entity
            jobLockEntity.JobMessagePopReceipt = message.Value.PopReceipt;

            // step 7: transaction update jobInfo entity and job lock entity
            IEnumerable<TableTransactionAction> transactionUpdateActions = new List<TableTransactionAction>
            {
                new (TableTransactionActionType.UpdateReplace, jobInfoEntity, jobInfoEntityResponse.Value.ETag),
                new (TableTransactionActionType.UpdateReplace, jobLockEntity, jobLockEntity.ETag),
            };

            _ = await _azureJobInfoTableClient.SubmitTransactionAsync(transactionUpdateActions, cancellationToken);

            _logger.LogInformation($"Dequeue job '{jobInfo.Id}'. Successfully ");
            return jobInfo;
        }

        public async Task<JobInfo> GetJobByIdAsync(byte queueType, long jobId, bool returnDefinition, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start to get job {jobId}.");

            // step 1: get job reverse index entity
            var jobReverseIndexEntity = await GetJobReverseIndexEntityByIdAsync(queueType, jobId, cancellationToken);

            // step 2: get job info entity
            var jobInfoEntityResponse = await _azureJobInfoTableClient.GetEntityAsync<JobInfoEntity>(
                jobReverseIndexEntity.JobInfoEntityPartitionKey,
                jobReverseIndexEntity.JobInfoEntityRowKey,
                cancellationToken: cancellationToken);
            var jobInfoEntity = jobInfoEntityResponse.Value;

            // step 3: convert to job info.
            var jobInfo = jobInfoEntity.ToJobInfo<TJobInfo>();

            _logger.LogInformation($"Get job {jobId} successfully.");
            return jobInfo;
        }

        public async Task<IEnumerable<JobInfo>> GetJobsByIdsAsync(byte queueType, long[] jobIds, bool returnDefinition, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start to get jobs {string.Join(",", jobIds)}.");

            var result = new List<JobInfo>();
            foreach (var id in jobIds)
            {
                result.Add(await GetJobByIdAsync(queueType, id, returnDefinition, cancellationToken));
            }

            _logger.LogInformation($"Get jobs {string.Join(",", jobIds)} successfully.");
            return result;
        }

        public async Task<IEnumerable<JobInfo>> GetJobByGroupIdAsync(byte queueType, long groupId, bool returnDefinition, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start to get jobs in group {groupId}.");

            var jobs = new List<JobInfo>();

            // job lock entity has the same partition key, so we need to query the row key here
            var queryResult = _azureJobInfoTableClient.QueryAsync<JobInfoEntity>(
                filter:
                $"PartitionKey eq '{AzureStorageKeyProvider.JobInfoPartitionKey(queueType, groupId)}' and RowKey ge '{groupId:D20}' and RowKey lt '{groupId + 1:D20}'",
                cancellationToken: cancellationToken);
            await foreach (var pageResult in queryResult.AsPages().WithCancellation(cancellationToken))
            {
                jobs.AddRange(pageResult.Values.Select(entity => entity.ToJobInfo<TJobInfo>()));
            }

            _logger.LogInformation($"Get jobs in group {groupId} successfully.");
            return jobs;
        }

        public async Task<bool> KeepAliveJobAsync(JobInfo jobInfo, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start to keep alive for job {jobInfo.Id}.");

            // step 1: get jobInfo entity
            var partitionKey = AzureStorageKeyProvider.JobInfoPartitionKey(jobInfo.QueueType, jobInfo.GroupId);
            var rowKey = AzureStorageKeyProvider.JobInfoRowKey(jobInfo.GroupId, jobInfo.Id);
            var jobInfoEntity = (await _azureJobInfoTableClient.GetEntityAsync<JobInfoEntity>(partitionKey, rowKey, cancellationToken: cancellationToken)).Value;

            // step 2: check version
            // the version is assigned when dequeue,
            // if the version does not match, means there are more than one running jobs for it, only the last one keep alive
            if (jobInfoEntity.Version != jobInfo.Version)
            {
                _logger.LogError($"Job {jobInfo.Id} precondition failed, version does not match.");

                throw new JobExecutionException($"Job {jobInfo.Id} precondition failed, version does not match.");
            }

            // step 3: get job lock entity
            var jobLockEntity = (await _azureJobInfoTableClient.GetEntityAsync<JobLockEntity>(
                jobInfoEntity.PartitionKey,
                AzureStorageKeyProvider.JobLockRowKey(((TJobInfo)jobInfo).JobIdentifier()),
                cancellationToken: cancellationToken)).Value;

            // step 4: update message visibility timeout
            Response<UpdateReceipt>? response = null;
            try
            {
                response = await _azureJobMessageQueueClient.UpdateMessageAsync(
                    jobLockEntity.JobMessageId,
                    jobLockEntity.JobMessagePopReceipt,
                    visibilityTimeout: TimeSpan.FromSeconds(QueueMessageVisibilityTimeoutInSeconds),
                    cancellationToken: cancellationToken);
            }
            catch (RequestFailedException ex) when (string.Equals(ex.ErrorCode, "MessageNotFound", StringComparison.OrdinalIgnoreCase))
            {
                // TODO: log for other cases the message is not found
                // TODO: need to return here?
                if (jobInfo.Status is JobStatus.Completed or JobStatus.Failed or JobStatus.Cancelled)
                {
                    _logger.LogInformation($"Job {jobInfo.Id} has been completed. Keep alive failed");
                }
            }

            // step 5: sync result to jobInfo entity
            // TODO: unit test: what if update message successfully while update table entity failed.
            jobInfoEntity.HeartbeatDateTime = DateTime.UtcNow;
            jobInfoEntity.Result = jobInfo.Result;

            // step 6: update message pop receipt to job lock entity
            jobLockEntity.JobMessagePopReceipt = response?.Value.PopReceipt;

            // step 7: transaction update jobInfo entity and job lock entity
            IEnumerable<TableTransactionAction> transactionUpdateActions = new List<TableTransactionAction>
            {
                new (TableTransactionActionType.UpdateReplace, jobInfoEntity, jobInfoEntity.ETag),
                new (TableTransactionActionType.UpdateReplace, jobLockEntity, jobLockEntity.ETag),
            };

            _ = await _azureJobInfoTableClient.SubmitTransactionAsync(transactionUpdateActions, cancellationToken);

            // step 8: check if cancel requested
            var shouldCancel = (await GetJobByIdAsync(jobInfo.QueueType, jobInfo.Id, false, cancellationToken)).CancelRequested;

            _logger.LogInformation($"Keep alive for job {jobInfo.Id} successfully.");

            return shouldCancel;
        }

        public async Task CancelJobByGroupIdAsync(byte queueType, long groupId, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start to cancel jobs in group {groupId}.");

            var jobInfoEntities = new List<JobInfoEntity>();

            // step 1: query all job ids in a group, using range query for row key to ignore lock/index entities in same partition
            var queryResult = _azureJobInfoTableClient.QueryAsync<JobInfoEntity>(filter: $"PartitionKey eq '{AzureStorageKeyProvider.JobInfoPartitionKey(queueType, groupId)}' and RowKey ge '{groupId:D20}' and RowKey lt '{groupId + 1:D20}'", cancellationToken: cancellationToken);
            await foreach (var pageResult in queryResult.AsPages().WithCancellation(cancellationToken))
            {
                foreach (var entity in pageResult.Values)
                {
                    // step 2: cancel job.
                    entity.CancelRequested = true;
                    if (entity.Status == (int)JobStatus.Created)
                    {
                        entity.Status = (int)JobStatus.Cancelled;
                    }

                    jobInfoEntities.Add(entity);
                }
            }

            // step 3: transaction update the cancelled jobs.
            var transactionActions = jobInfoEntities.Select(entity => new TableTransactionAction(TableTransactionActionType.UpdateReplace, entity, entity.ETag));
            var responseList = await _azureJobInfoTableClient.SubmitTransactionAsync(transactionActions, cancellationToken);
            var batchFailed = responseList.Value.Any(response => response.IsError);

            // step 4: log error and throw exceptions for failure
            if (batchFailed)
            {
                var errorMessage = responseList.Value.Where(response => response.IsError).Select(response => response.ReasonPhrase).First();
                _logger.LogError($"Failed to cancel jobs in group {groupId}. Reason: {errorMessage}");
                throw new JobExecutionException($"Failed to cancel jobs in group {groupId}. Reason: {errorMessage}");
            }

            _logger.LogInformation($"Cancel jobs in group {groupId} successfully.");
        }

        public async Task CancelJobByIdAsync(byte queueType, long jobId, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start to cancel job {jobId}.");

            // step 1:Load reverse index entity.
            var reverseIndexEntity = await GetJobReverseIndexEntityByIdAsync(queueType, jobId, cancellationToken);

            // step 2: get jobInfo entity
            var response = await _azureJobInfoTableClient.GetEntityAsync<JobInfoEntity>(reverseIndexEntity.JobInfoEntityPartitionKey, reverseIndexEntity.JobInfoEntityRowKey, cancellationToken: cancellationToken);
            var jobInfoEntity = response.Value;

            // step 3: set jobInfo entity's cancel request to true.
            jobInfoEntity.CancelRequested = true;

            // only set job status to cancelled when the job status is created.
            if (jobInfoEntity.Status == (int)JobStatus.Created)
            {
                jobInfoEntity.Status = (int)JobStatus.Cancelled;
            }

            // step 4: update job info entity to table.
            // cancel job anyway, so Etag is all.
            await _azureJobInfoTableClient.UpdateEntityAsync(jobInfoEntity, ETag.All, cancellationToken: cancellationToken);

            _logger.LogInformation($"Cancel job {jobId} successfully.");
        }

        public async Task CompleteJobAsync(JobInfo jobInfo, bool requestCancellationOnFailure, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start to complete job {jobInfo.Id}.");

            // step 1: get job info entity
            var partitionKey = AzureStorageKeyProvider.JobInfoPartitionKey(jobInfo.QueueType, jobInfo.GroupId);
            var rowKey = AzureStorageKeyProvider.JobInfoRowKey(jobInfo.GroupId, jobInfo.Id);
            var existingJobInfoEntity = (await _azureJobInfoTableClient.GetEntityAsync<JobInfoEntity>(partitionKey, rowKey, cancellationToken: cancellationToken)).Value;

            var jobInfoEntity = jobInfo.ToTableEntity();

            // step 2: update status
            // TODO: double check the expected behavous
            if (jobInfoEntity.Status == (int)JobStatus.Completed && (existingJobInfoEntity.Status == (int)JobStatus.Cancelled || existingJobInfoEntity.CancelRequested))
            {
                jobInfoEntity.Status = (int)JobStatus.Cancelled;
            }

            // step 3: update job info entity to table
            await _azureJobInfoTableClient.UpdateEntityAsync(jobInfoEntity, existingJobInfoEntity.ETag, cancellationToken: cancellationToken);

            // step 4: get job lock entity to get job message id and job pop receipt
            var jobLockEntity = (await _azureJobInfoTableClient.GetEntityAsync<JobLockEntity>(
                jobInfoEntity.PartitionKey,
                AzureStorageKeyProvider.JobLockRowKey(((TJobInfo)jobInfo).JobIdentifier()),
                cancellationToken: cancellationToken)).Value;

            // step 5: delete message
            await _azureJobMessageQueueClient.DeleteMessageAsync(jobLockEntity.JobMessageId, jobLockEntity.JobMessagePopReceipt, cancellationToken: cancellationToken);

            // TODO: need to check message id and message pop receipt is for job entity or for jobInfos
            // TODO: clear message id and message pop receipt for failed/cancel job.

            // TODO: how about job is cancelled, and request cancel
            // step 6: cancel jobs if requested
            if (requestCancellationOnFailure && jobInfo.Status == JobStatus.Failed)
            {
                await CancelJobByGroupIdAsync(jobInfo.QueueType, jobInfo.GroupId, cancellationToken);
            }

            _logger.LogInformation($"Complete job {jobInfo.Id} successfully.");
        }

        /// <summary>
        /// Get incremental job ids
        /// </summary>
        /// <param name="queueType">the queue type.</param>
        /// <param name="count">the count of job ids to be retrieved.</param>
        /// <param name="cancellationToken">the cancellation token.</param>
        /// <returns>The job ids, throw exceptions if fails.</returns>
        private async Task<List<long>> GetIncrementalJobIds(byte queueType, int count, CancellationToken cancellationToken)
        {
            var partitionKey = AzureStorageKeyProvider.JobIdPartitionKey(queueType);
            var rowKey = AzureStorageKeyProvider.JobIdRowKey(queueType);
            JobIdEntity entity;
            try
            {
                var response = await _azureJobInfoTableClient.GetEntityAsync<JobIdEntity>(partitionKey, rowKey, cancellationToken: cancellationToken);
                entity = response.Value;
            }
            catch (RequestFailedException ex) when (IsSpecifiedErrorCode(ex, AzureStorageErrorCode.GetEntityNotFoundErrorCode))
            {
                _logger.LogWarning(ex, "Failed to get job id entity, the entity doesn't exist, will create one.");

                // create new entity if not exist
                var initialJobIdEntity = new JobIdEntity
                {
                    PartitionKey = partitionKey,
                    RowKey = rowKey,
                    NextJobId = 0,
                };
                await _azureJobInfoTableClient.AddEntityAsync(initialJobIdEntity, cancellationToken);

                // get the job id entity again
                entity = (await _azureJobInfoTableClient.GetEntityAsync<JobIdEntity>(partitionKey, rowKey, cancellationToken: cancellationToken)).Value;
            }

            var result = new List<long>();

            for (var i = 0; i < count; i++)
            {
                result.Add(entity.NextJobId);
                entity.NextJobId++;
            }

            try
            {
                await _azureJobInfoTableClient.UpdateEntityAsync(entity, entity.ETag, cancellationToken: cancellationToken);
            }
            catch (RequestFailedException ex) when (IsSpecifiedErrorCode(ex, AzureStorageErrorCode.UpdateEntityPreconditionFailedErrorCode))
            {
                _logger.LogWarning(ex, "Update job id entity conflicts, will try again.");

                // try to get job ids again
                result = await GetIncrementalJobIds(queueType, count, cancellationToken);
            }

            return result;
        }

        private async Task<JobReverseIndexEntity> GetJobReverseIndexEntityByIdAsync(byte queueType, long jobId, CancellationToken cancellationToken)
        {
            var reversePartitionKey = AzureStorageKeyProvider.JobReverseIndexPartitionKey(queueType, jobId);
            var reverseRowKey = AzureStorageKeyProvider.JobReverseIndexRowKey(queueType, jobId);
            var reverseIndexResponse = await _azureJobInfoTableClient.GetEntityAsync<JobReverseIndexEntity>(reversePartitionKey, reverseRowKey, cancellationToken: cancellationToken);
            return reverseIndexResponse.Value;
        }

        private static bool IsSpecifiedErrorCode(RequestFailedException exception, string expectedErrorCode) =>
            string.Equals(exception.ErrorCode, expectedErrorCode, StringComparison.OrdinalIgnoreCase);
    }
}