// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Diagnostics;
using Azure;
using Azure.Data.Tables;
using Azure.Identity;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.JobManagement.Exceptions;
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

        /// <summary>
        /// The maximum size of a single entity, including all property values is 1 MiB,
        /// see https://docs.microsoft.com/en-us/azure/storage/tables/scalability-targets#scale-targets-for-table-storage.
        /// The definition and result are serialized string of objects,
        /// we should be careful that should not contain large fields when define the definition and result class.
        /// Add an assert here to make sure the size of definition and result aren't larger than 1MiB.
        /// </summary>
        private const int MaximumSizeOfAnEntityInBytes = 1024 * 1024;

        public AzureStorageQueueClient(
            IStorage storage,
            IOptions<JobConfiguration> jobConfiguration,
            ILogger<AzureStorageQueueClient<TJobInfo>> logger)
        {
            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            EnsureArg.IsNotNull(storage, nameof(storage));

            _logger = EnsureArg.IsNotNull(logger, nameof(logger));

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

        // The expected behaviors:
        // 1. multi agent instances enqueue same jobs concurrently, only one job entity will be created,
        //    there may be multi messages, while only one will be recorded in job lock entity, and the created jobInfo will be returned for all instances.
        // 2. re-enqueue job, no matter what the job status is now, will do nothing, and return the existing jobInfo.
        // 3. if one of the steps fails, will continue to process it when re-enqueue.
        // TODO: The parameter forceOneActiveJobGroup and isCompleted are ignored for now
        public async Task<IEnumerable<JobInfo>> EnqueueAsync(
            byte queueType,
            string[] definitions,
            long? groupId,
            bool forceOneActiveJobGroup,
            bool isCompleted,
            CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start to enqueue {definitions.Length} jobs.");

            // step 1: get incremental job ids, will try again if fails
            var jobIds = await GetIncrementalJobIds(queueType, definitions.Length, cancellationToken);

            // handle for each job
            var jobInfos = new List<TJobInfo>();
            for (var i = 0; i < definitions.Length; i++)
            {
                Trace.Assert(
                    definitions[i].Length * sizeof(char) < MaximumSizeOfAnEntityInBytes,
                    "The maximum size of a single table entity is 1MB, the size of definition is larger than 1MB.");

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
                    // Note: for cancelled/failed jobs, we don't allow resume it, will return the existing jobInfo.
                    _logger.LogWarning(ex, "Failed to add entities, the entities already exist. Will fetch the existing jobs.");
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
                // if processing job failed and the message is deleted, then the message id is still in table entity,
                // we don't resend message for it, and return the existing jobInfo, so will do noting about it.
                if (string.IsNullOrWhiteSpace(jobLockEntity.JobMessageId))
                {
                    var response = await _azureJobMessageQueueClient.SendMessageAsync(
                        new JobMessage(jobInfoEntity.PartitionKey, jobInfoEntity.RowKey).ToString(),
                        cancellationToken);

                    jobLockEntity.JobMessagePopReceipt = response.Value.PopReceipt;
                    jobLockEntity.JobMessageId = response.Value.MessageId;

                    // step 7: update message id and message pop receipt to job lock entity
                    // if enqueue concurrently, it is possible that
                    // 1. one job sends message and updates entity, another job do nothing
                    // 2. two jobs both send message, while only one job update entity successfully
                    try
                    {
                        await _azureJobInfoTableClient.UpdateEntityAsync(
                            jobLockEntity,
                            jobLockEntity.ETag,
                            cancellationToken: cancellationToken);
                    }
                    catch (RequestFailedException ex) when (IsSpecifiedErrorCode(ex, AzureStorageErrorCode.UpdateEntityPreconditionFailedErrorCode))
                    {
                        _logger.LogWarning(ex, "Update job info entity conflicts.");
                    }
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
            TimeSpan? visibilityTimeOut = heartbeatTimeoutSec <= 0 ? null : TimeSpan.FromSeconds(heartbeatTimeoutSec);
            var message = await _azureJobMessageQueueClient.ReceiveMessageAsync(visibilityTimeOut, cancellationToken);

            if (message.Value == null)
            {
                _logger.LogWarning("Failed to receive message from queue, the queue message is null.");
                return null;
            }

            var jobMessage = JsonConvert.DeserializeObject<JobMessage>(message.Value.Body.ToString());

            if (jobMessage == null)
            {
                _logger.LogWarning("Failed to deserialize message, the deserialized job message is null.");
                return null;
            }

            // step 2: get jobInfo entity to check job status, delete this message if job is already completed/failed/cancelled
            // if status is running and CancelRequest is true, the job will be dequeued, and jobHosting will continue to handle it
            var jobInfoEntityResponse = await _azureJobInfoTableClient.GetEntityAsync<JobInfoEntity>(
                jobMessage.PartitionKey,
                jobMessage.RowKey,
                cancellationToken: cancellationToken);
            var jobInfo = jobInfoEntityResponse.Value.ToJobInfo<TJobInfo>();

            if (jobInfo.Status is JobStatus.Completed or JobStatus.Failed or JobStatus.Cancelled)
            {
                _logger.LogWarning($"Discard queue message {message.Value.MessageId}, the job status is {jobInfo.Status}.");
                await _azureJobMessageQueueClient.DeleteMessageAsync(message.Value.MessageId, message.Value.PopReceipt, cancellationToken);

                // dequeue next job
                return await DequeueAsync(queueType, worker, heartbeatTimeoutSec, cancellationToken);
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
                await _azureJobMessageQueueClient.DeleteMessageAsync(message.Value.MessageId, message.Value.PopReceipt, cancellationToken);

                // dequeue next job
                return await DequeueAsync(queueType, worker, heartbeatTimeoutSec, cancellationToken);
            }

            // step 4: skip it if the job is running and still active
            if (jobInfo.Status == JobStatus.Running && jobInfo.HeartbeatDateTime.AddSeconds(heartbeatTimeoutSec) > DateTime.UtcNow)
            {
                _logger.LogWarning($"Job {jobInfo.Id} is still active.");

                // dequeue next job
                return await DequeueAsync(queueType, worker, heartbeatTimeoutSec, cancellationToken);
            }

            // step 5: update jobInfo entity's status to running, also update version and heartbeat
            jobInfo.Status = JobStatus.Running;

            // jobInfo's version is set when dequeue, if there are multi running jobs for this jobInfo, only the last one will keep alive
            jobInfo.Version = DateTimeOffset.UtcNow.Ticks;
            jobInfo.HeartbeatDateTime = DateTime.UtcNow;
            jobInfo.HeartbeatTimeoutSec = heartbeatTimeoutSec;
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
            var selectedProperties = returnDefinition ? null : SelectPropertiesExceptDefinition();

            var jobInfoEntityResponse = await _azureJobInfoTableClient.GetEntityAsync<JobInfoEntity>(
                jobReverseIndexEntity.JobInfoEntityPartitionKey,
                jobReverseIndexEntity.JobInfoEntityRowKey,
                selectedProperties,
                cancellationToken);
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
                // get each job by id
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
            var selectedProperties = returnDefinition ? null : SelectPropertiesExceptDefinition();

            var queryResult = _azureJobInfoTableClient.QueryAsync<JobInfoEntity>(
                filter: FilterJobInfosByGroupId(queueType, groupId),
                select: selectedProperties,
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
            var jobInfoEntity = (await _azureJobInfoTableClient.GetEntityAsync<JobInfoEntity>(
                AzureStorageKeyProvider.JobInfoPartitionKey(jobInfo.QueueType, jobInfo.GroupId),
                AzureStorageKeyProvider.JobInfoRowKey(jobInfo.GroupId, jobInfo.Id),
                cancellationToken: cancellationToken)).Value;

            // step 2: check version
            // the version is assigned when dequeue,
            // if the version does not match, means there are more than one running jobs for it, only the last one keep alive
            if (jobInfoEntity.Version != jobInfo.Version)
            {
                _logger.LogError($"Job {jobInfo.Id} precondition failed, version does not match.");

                throw new JobNotExistException($"Job {jobInfo.Id} precondition failed, version does not match.");
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
                var visibilityTimeOut =
                    jobInfoEntity.HeartbeatTimeoutSec <= 0 ? default : TimeSpan.FromSeconds(jobInfoEntity.HeartbeatTimeoutSec);
                response = await _azureJobMessageQueueClient.UpdateMessageAsync(
                    jobLockEntity.JobMessageId,
                    jobLockEntity.JobMessagePopReceipt,
                    visibilityTimeout: visibilityTimeOut,
                    cancellationToken: cancellationToken);
            }
            catch (RequestFailedException ex) when (IsSpecifiedErrorCode(ex, AzureStorageErrorCode.UpdateOrDeleteMessageNotFoundErrorCode))
            {
                if (jobInfo.Status is JobStatus.Completed or JobStatus.Failed or JobStatus.Cancelled)
                {
                    _logger.LogInformation($"Job {jobInfo.Id} has been completed. Keep alive failed");
                }
                else
                {
                    _logger.LogWarning("Failed to keep alive, the job message is not found.");
                }
            }

            // step 5: sync result to jobInfo entity
            // if update message successfully while update table entity failed, then the message pop receipt is invalid,
            // keeping alive always fails to update message, so the message will be visible and dequeue again
            // when re-dequeue, the message pop receipt is updated to table entity,
            // and the previous job will throw JobNotExistException as the version doesn't match, and jobHosting cancels the previous job.
            jobInfoEntity.HeartbeatDateTime = DateTime.UtcNow;

            Trace.Assert(
                jobInfo.Result.Length * sizeof(char) < MaximumSizeOfAnEntityInBytes,
                "The maximum size of a single table entity is 1MB, the size of result is larger than 1MB.");

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
            var queryResult = _azureJobInfoTableClient.QueryAsync<JobInfoEntity>(
                filter: FilterJobInfosByGroupId(queueType, groupId),
                cancellationToken: cancellationToken);
            await foreach (var pageResult in queryResult.AsPages().WithCancellation(cancellationToken))
            {
                foreach (var jobInfoEntity in pageResult.Values)
                {
                    // step 2: cancel job.
                    CancelJobInternal(jobInfoEntity);
                    jobInfoEntities.Add(jobInfoEntity);
                }
            }

            // step 3: transaction update the cancelled jobs.
            var transactionActions = jobInfoEntities.Select(entity =>
                new TableTransactionAction(TableTransactionActionType.UpdateReplace, entity, entity.ETag));

            var responseList = await _azureJobInfoTableClient.SubmitTransactionAsync(transactionActions, cancellationToken);
            var batchFailed = responseList.Value.Any(response => response.IsError);

            // step 4: log error and throw exceptions for failure
            if (batchFailed)
            {
                var errorMessage = responseList.Value.Where(response => response.IsError).Select(response => response.ReasonPhrase).First();
                _logger.LogError($"Failed to cancel jobs in group {groupId}. Reason: {errorMessage}");
                throw new JobManagementException($"Failed to cancel jobs in group {groupId}. Reason: {errorMessage}");
            }

            _logger.LogInformation($"Cancel jobs in group {groupId} successfully.");
        }

        public async Task CancelJobByIdAsync(byte queueType, long jobId, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start to cancel job {jobId}.");

            // step 1:Load reverse index entity.
            var reverseIndexEntity = await GetJobReverseIndexEntityByIdAsync(queueType, jobId, cancellationToken);

            // step 2: get jobInfo entity
            var jobInfoEntity = (await _azureJobInfoTableClient.GetEntityAsync<JobInfoEntity>(reverseIndexEntity.JobInfoEntityPartitionKey, reverseIndexEntity.JobInfoEntityRowKey, cancellationToken: cancellationToken)).Value;

            // step 3: cancel job
            CancelJobInternal(jobInfoEntity);

            // step 4: update job info entity to table.
            await _azureJobInfoTableClient.UpdateEntityAsync(jobInfoEntity, jobInfoEntity.ETag, cancellationToken: cancellationToken);

            _logger.LogInformation($"Cancel job {jobId} successfully.");
        }

        public async Task CompleteJobAsync(JobInfo jobInfo, bool requestCancellationOnFailure, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start to complete job {jobInfo.Id}.");

            Trace.Assert(
                jobInfo.Result.Length * sizeof(char) < MaximumSizeOfAnEntityInBytes,
                "The maximum size of a single table entity is 1MB, the size of result is larger than 1MB.");

            // step 1: check version
            var retrievedJobInfo = await GetJobByIdAsync(jobInfo.QueueType, jobInfo.Id, false, cancellationToken);

            if (retrievedJobInfo.Version != jobInfo.Version)
            {
                _logger.LogError($"Job {jobInfo.Id} precondition failed, version does not match.");
                throw new JobNotExistException($"Job {jobInfo.Id} precondition failed, version does not match.");
            }

            // step 2: get shouldCancel
            var shouldCancel = retrievedJobInfo.CancelRequested;

            // step 3: update status
            // Reference: https://github.com/microsoft/fhir-server/blob/e1117009b6db995672cc4d31457cb3e6f32e19a3/src/Microsoft.Health.Fhir.SqlServer/Features/Schema/Sql/Sprocs/PutJobStatus.sql#L16
            var jobInfoEntity = ((TJobInfo)jobInfo).ToTableEntity();
            if (jobInfoEntity.Status == (int)JobStatus.Failed)
            {
                jobInfoEntity.Status = (int)JobStatus.Failed;
            }
            else if (shouldCancel)
            {
                jobInfoEntity.Status = (int)JobStatus.Cancelled;
            }
            else
            {
                jobInfoEntity.Status = (int) JobStatus.Completed;
            }

            // step 4: update job info entity to table
            await _azureJobInfoTableClient.UpdateEntityAsync(jobInfoEntity, ETag.All, cancellationToken: cancellationToken);

            // step 5: get job lock entity to get job message id and job pop receipt
            var jobLockEntity = (await _azureJobInfoTableClient.GetEntityAsync<JobLockEntity>(
                jobInfoEntity.PartitionKey,
                AzureStorageKeyProvider.JobLockRowKey(((TJobInfo)jobInfo).JobIdentifier()),
                cancellationToken: cancellationToken)).Value;

            // step 6: delete message
            // if table entity is updated successfully while delete message fails, then the message is visible and dequeue again,
            // and the message will be deleted since the table entity's status is completed/failed/cancelled
            try
            {
                await _azureJobMessageQueueClient.DeleteMessageAsync(jobLockEntity.JobMessageId, jobLockEntity.JobMessagePopReceipt, cancellationToken: cancellationToken);
            }
            catch (RequestFailedException ex) when (IsSpecifiedErrorCode(ex, AzureStorageErrorCode.UpdateOrDeleteMessageNotFoundErrorCode))
            {
                _logger.LogWarning($"Failed to delete message, the message {jobLockEntity.JobMessageId} does not exist.");
            }

            // step 7: cancel jobs if requested
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

                try
                {
                    await _azureJobInfoTableClient.AddEntityAsync(initialJobIdEntity, cancellationToken);
                }
                catch (RequestFailedException exception) when (IsSpecifiedErrorCode(exception, AzureStorageErrorCode.AddEntityAlreadyExistsErrorCode))
                {
                    _logger.LogWarning(exception, "Failed to add job id entity, the entity already exists.");
                }

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

        private static string FilterJobInfosByGroupId(byte queueType, long groupId) =>
            $"PartitionKey eq '{AzureStorageKeyProvider.JobInfoPartitionKey(queueType, groupId)}' and RowKey ge '{groupId:D20}' and RowKey lt '{groupId + 1:D20}'";

        private static IEnumerable<string> SelectPropertiesExceptDefinition()
        {
            var type = Type.GetType("Microsoft.Health.Fhir.Synapse.JobManagement.Models.AzureStorage.JobInfoEntity");
            if (type == null)
            {
                throw new JobManagementException("Failed to get JobInfoEntity properties, the type is null.");
            }

            return type.GetProperties().Select(p => p.Name).Except(new List<string> { "Definition" });
        }

        /// <summary>
        /// when cancel a job, always set the cancelRequest to true, and set its status to cancelled only when it's created,
        /// for other cases, shouldCancel will be returned when keep alive, and jobHosting will cancel this job and set job to completed
        /// </summary>
        private static void CancelJobInternal(JobInfoEntity jobInfoEntity)
        {
            // set jobInfo entity's cancel request to true.
            jobInfoEntity.CancelRequested = true;

            // only set job status to cancelled when the job status is created.
            if (jobInfoEntity.Status == (int)JobStatus.Created)
            {
                jobInfoEntity.Status = (int)JobStatus.Cancelled;
            }
        }
    }
}