// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Concurrent;
using Azure;
using Azure.Data.Tables;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.JobManagement.Exceptions;
using Microsoft.Health.Fhir.Synapse.JobManagement.Extensions;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models.AzureStorage;
using Microsoft.Health.JobManagement;

namespace Microsoft.Health.Fhir.Synapse.JobManagement
{
    // The maximum size of a single entity, including all property values is 1 MiB,
    // see https://docs.microsoft.com/en-us/azure/storage/tables/scalability-targets#scale-targets-for-table-storage.
    // The definition and result are serialized string of objects,
    // we should be careful that should not contain large fields when define the definition and result class.
    public class AzureStorageJobQueueClient<TJobInfo> : IQueueClient
        where TJobInfo : AzureStorageJobInfo, new()
    {
        private readonly TableClient _azureJobInfoTableClient;
        private readonly QueueClient _azureJobMessageQueueClient;

        private readonly ILogger<AzureStorageJobQueueClient<TJobInfo>> _logger;

        private const int DefaultVisibilityTimeoutInSeconds = 30;

        private const short MaxThreadsCountForGettingJob = 5;

        // A transaction can include at most 100 entities, so limit the jobs count to 50
        // https://docs.microsoft.com/en-us/azure/storage/tables/scalability-targets#scale-targets-for-table-storage
        private const int MaxJobsCountForEnqueuingInABatch = 50;

        private bool _isInitialized;

        public AzureStorageJobQueueClient(
            IAzureStorageClientFactory azureStorageClientFactory,
            ILogger<AzureStorageJobQueueClient<TJobInfo>> logger)
        {
            EnsureArg.IsNotNull(azureStorageClientFactory, nameof(azureStorageClientFactory));

            _logger = EnsureArg.IsNotNull(logger, nameof(logger));

            _azureJobInfoTableClient = azureStorageClientFactory.CreateTableClient();
            _azureJobMessageQueueClient = azureStorageClientFactory.CreateQueueClient();
            _isInitialized = false;
        }

        public bool IsInitialized()
        {
            if (_isInitialized)
            {
                return _isInitialized;
            }

            // try to initialize if it is not initialized yet.
            TryInitialize();
            return _isInitialized;
        }

        // The expected behaviors:
        // 1. multi agent instances enqueue same jobs concurrently, only one job entity will be created,
        //    there may be multi messages, while only one will be recorded in job lock entity, and the created jobInfo will be returned for all instances.
        // 2. re-enqueue job, no matter what the job status is now, will do nothing, and return the existing jobInfo, which means for cancelled/failed jobs, we don't allow resume it, will return the existing jobInfo.
        // 3. if one of the steps fails, will continue to process it when re-enqueue.
        // 4. if there are multi jobs to be enqueued, all the job will operate in one transaction
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

            if (definitions.Length > MaxJobsCountForEnqueuingInABatch)
            {
                _logger.LogError($"The count of jobs to be enqueued is larger than the maximum allowed length {MaxJobsCountForEnqueuingInABatch}.");
                throw new JobManagementException(
                    $"The count of jobs to be enqueued is larger than the maximum allowed length {MaxJobsCountForEnqueuingInABatch}.");
            }

            // step 1: get incremental job ids, will try again if fails
            var jobIds = await GetIncrementalJobIds(queueType, definitions.Length, cancellationToken);

            // step 2: generate job info entities and job lock entities batch
            var jobInfos = definitions.Select((definition, i) => new TJobInfo
                {
                    Id = jobIds[i],
                    QueueType = queueType,
                    Status = JobStatus.Created,
                    GroupId = groupId ?? 0,
                    Definition = definition,
                    Result = string.Empty,
                    CancelRequested = false,
                    CreateDate = DateTime.UtcNow,
                    HeartbeatDateTime = DateTime.UtcNow,
                })
                .ToList();

            var jobInfoEntities = jobInfos.Select(jobInfo => jobInfo.ToTableEntity()).ToList();
            var jobLockEntities = jobInfoEntities.Select((jobInfoEntity, i) =>
                new TableEntity(jobInfoEntity.PartitionKey, AzureStorageKeyProvider.JobLockRowKey(jobInfos[i].JobIdentifier()))
                {
                    { JobLockEntityProperties.JobInfoEntityRowKey, jobInfoEntity.RowKey },
                }).ToList();

            // step 3: insert jobInfo entity and job lock entity in one transaction.
            var transactionActions = jobInfoEntities
                .Select(entity => new TableTransactionAction(TableTransactionActionType.Add, entity))
                .Concat(jobLockEntities.Select(entity => new TableTransactionAction(TableTransactionActionType.Add, entity)));
            try
            {
                await _azureJobInfoTableClient.SubmitTransactionAsync(transactionActions, cancellationToken);
            }
            catch (RequestFailedException ex) when (IsSpecifiedErrorCode(ex, AzureStorageErrorCode.InvalidDuplicateRowErrorCode))
            {
                _logger.LogError(ex, "There are duplicated jobs to be enqueued.");
                throw;
            }
            catch (RequestFailedException ex) when (IsSpecifiedErrorCode(ex, AzureStorageErrorCode.AddEntityAlreadyExistsErrorCode))
            {
                // step 4: get the existing job lock entities and jobInfo entities
                // need to get job lock entity firstly to get the jobInfo entity row key
                var jobEntityQueryResult = _azureJobInfoTableClient.QueryAsync<TableEntity>(
                    filter: TransactionGetByKeys(jobLockEntities.First().PartitionKey, jobLockEntities.Select(entity => entity.RowKey).ToList()),
                    cancellationToken: cancellationToken);

                var retrievedJobLockEntities = new List<TableEntity>();
                await foreach (var pageResult in jobEntityQueryResult.AsPages().WithCancellation(cancellationToken))
                {
                    retrievedJobLockEntities.AddRange(pageResult.Values);
                }

                if (!retrievedJobLockEntities.Any())
                {
                    _logger.LogError(ex, "There are duplicated jobs to be enqueued.");
                    throw;
                }

                jobLockEntities = retrievedJobLockEntities;

                // get job info entity by specifying the row key stored in job lock entity
                jobEntityQueryResult = _azureJobInfoTableClient.QueryAsync<TableEntity>(
                    filter: TransactionGetByKeys(jobLockEntities.First().PartitionKey, jobLockEntities.Select(entity => entity.GetString(JobLockEntityProperties.JobInfoEntityRowKey)).ToList()),
                    cancellationToken: cancellationToken);

                var retrievedJobInfoEntities = new List<TableEntity>();
                await foreach (var pageResult in jobEntityQueryResult.AsPages().WithCancellation(cancellationToken))
                {
                    retrievedJobInfoEntities.AddRange(pageResult.Values);
                }

                jobInfoEntities = retrievedJobInfoEntities;
                _logger.LogInformation(ex, "The entities already exist. Fetched the existing jobs.");
            }
            catch (RequestFailedException ex) when (IsSpecifiedErrorCode(ex, AzureStorageErrorCode.RequestBodyTooLargeErrorCode))
            {
                _logger.LogError(ex, "The maximum size of a single table entity is 1MB, the size of entity is larger than 1MB.");
                throw;
            }

            // step 5: try to add reverse index for jobInfo entity
            try
            {
                transactionActions = jobInfoEntities.Select(jobInfoEntity =>
                    new TableTransactionAction(TableTransactionActionType.Add, new JobReverseIndexEntity
                    {
                        PartitionKey = AzureStorageKeyProvider.JobReverseIndexPartitionKey(queueType, (long)jobInfoEntity[JobInfoEntityProperties.Id]),
                        RowKey = AzureStorageKeyProvider.JobReverseIndexRowKey(queueType, (long)jobInfoEntity[JobInfoEntityProperties.Id]),
                        JobInfoEntityPartitionKey = jobInfoEntity.PartitionKey,
                        JobInfoEntityRowKey = jobInfoEntity.RowKey,
                    }));

                _ = await _azureJobInfoTableClient.SubmitTransactionAsync(transactionActions, cancellationToken);
            }
            catch (RequestFailedException ex) when (IsSpecifiedErrorCode(ex, AzureStorageErrorCode.AddEntityAlreadyExistsErrorCode))
            {
                _logger.LogInformation(ex, "The job reverse index entities already exist.");
            }

            // for new added job lock entities, their etag are empty, need to get them to get etag.
            if (jobLockEntities.Any(jobLockEntity => string.IsNullOrWhiteSpace(jobLockEntity.ETag.ToString())))
            {
                var jobLockEntityQueryResult = _azureJobInfoTableClient.QueryAsync<TableEntity>(
                    filter: TransactionGetByKeys(jobLockEntities.First().PartitionKey, jobLockEntities.Select(entity => entity.RowKey).ToList()),
                    cancellationToken: cancellationToken);

                jobLockEntities.Clear();
                await foreach (var pageResult in jobLockEntityQueryResult.AsPages().WithCancellation(cancellationToken))
                {
                    jobLockEntities.AddRange(pageResult.Values);
                }
            }

            // step 6: if queue message not present in job lock entity, push message to queue.
            // if processing job failed and the message is deleted, then the message id is still in table entity,
            // we don't resend message for it, and return the existing jobInfo, so will do noting about it
            if (jobLockEntities.Any(jobLockEntity => !jobLockEntity.ContainsKey(JobLockEntityProperties.JobMessageId)))
            {
                foreach (var jobLockEntity in jobLockEntities.Where(jobLockEntity => !jobLockEntity.ContainsKey(JobLockEntityProperties.JobMessageId)))
                {
                    var response = await _azureJobMessageQueueClient.SendMessageAsync(
                        new JobMessage(jobLockEntity.PartitionKey, jobLockEntity.GetString(JobLockEntityProperties.JobInfoEntityRowKey), jobLockEntity.RowKey).ToString(),
                        cancellationToken);

                    jobLockEntity[JobLockEntityProperties.JobMessagePopReceipt] = response.Value.PopReceipt;
                    jobLockEntity[JobLockEntityProperties.JobMessageId] = response.Value.MessageId;
                }

                // step 7: update message id and message pop receipt to job lock entity
                // if enqueue concurrently, it is possible that
                // 1. one job sends message and updates entity, another job do nothing
                // 2. two jobs both send message, while only one job update entity successfully
                try
                {
                    var transactionUpdateActions = jobLockEntities.Select(jobLockEntity =>
                        new TableTransactionAction(TableTransactionActionType.UpdateReplace, jobLockEntity, jobLockEntity.ETag));

                    _ = await _azureJobInfoTableClient.SubmitTransactionAsync(transactionUpdateActions, cancellationToken);
                }
                catch (RequestFailedException ex) when (IsSpecifiedErrorCode(ex, AzureStorageErrorCode.UpdateEntityPreconditionFailedErrorCode))
                {
                    _logger.LogWarning(ex, "Update job info entity conflicts.");
                }
            }

            jobInfos = jobInfoEntities.Select(entity => entity.ToJobInfo<TJobInfo>()).ToList();

            _logger.LogInformation($"Enqueue jobs '{string.Join(",", jobInfos.Select(jobInfo => jobInfo.Id).ToList())}' successfully.");

            return jobInfos;
        }

        public async Task<JobInfo> DequeueAsync(byte queueType, string worker, int heartbeatTimeoutSec, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Start to dequeue.");

            // step 1: receive message from message queue
            var visibilityTimeout =
                TimeSpan.FromSeconds(heartbeatTimeoutSec <= 0
                    ? DefaultVisibilityTimeoutInSeconds
                    : heartbeatTimeoutSec);
            var message = (await _azureJobMessageQueueClient.ReceiveMessageAsync(visibilityTimeout, cancellationToken)).Value;

            if (message == null)
            {
                _logger.LogInformation("The queue is empty.");
                return null;
            }

            var jobMessage = JobMessage.Parse(message.Body.ToString());

            if (jobMessage == null)
            {
                _logger.LogWarning("Failed to deserialize message.");
                return null;
            }

            // step 2: get jobInfo entity and job lock entity
            var (jobInfoEntity, jobLockEntity) = await AcquireJobEntityByRowKeysAsync(jobMessage.PartitionKey, new List<string> { jobMessage.RowKey, jobMessage.LockRowKey }, cancellationToken);
            var jobInfo = jobInfoEntity.ToJobInfo<TJobInfo>();

            // step 3: check job status
            // delete this message if job is already completed / failed / cancelled
            // if status is running and CancelRequest is true, the job will be dequeued, and jobHosting will continue to handle it
            if (jobInfo.Status is JobStatus.Completed or JobStatus.Failed or JobStatus.Cancelled)
            {
                _logger.LogWarning($"Discard queue message {message.MessageId}, the job status is {jobInfo.Status}.");
                await _azureJobMessageQueueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, cancellationToken);

                throw new JobManagementException($"Discard queue message {message.MessageId}, the job status is {jobInfo.Status}.");
            }

            // step 4: check message id
            if (!jobLockEntity.ContainsKey(JobLockEntityProperties.JobMessageId))
            {
                // the message is enqueued and dequeued immediately before the update the message info to table entity, skip processing it this time.
                _logger.LogWarning($"The message id in job lock entity is null, skip processing this message this time.");
                return null;
            }

            if (!string.Equals(jobLockEntity.GetString(JobLockEntityProperties.JobMessageId), message.MessageId, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning($"Discard queue message {message.MessageId}, the message id is inconsistent with the one in the table entity.");
                await _azureJobMessageQueueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, cancellationToken);

                throw new JobManagementException(
                    $"Discard queue message {message.MessageId}, the message id is inconsistent with the one in the table entity.");
            }

            // step 5: skip it if the job is running and still active
            if (jobInfo.Status == JobStatus.Running && jobInfo.HeartbeatDateTime.AddSeconds(heartbeatTimeoutSec) > DateTime.UtcNow)
            {
                _logger.LogWarning($"Job {jobInfo.Id} is still active.");

                throw new JobManagementException($"Job {jobInfo.Id} is still active.");
            }

            // step 6: update jobInfo entity's status to running, also update version and heartbeat
            jobInfo.Status = JobStatus.Running;

            // jobInfo's version is set when dequeue, if there are multi running jobs for this jobInfo, only the last one will keep alive
            jobInfo.Version = DateTimeOffset.UtcNow.Ticks;
            jobInfo.HeartbeatDateTime = DateTime.UtcNow;
            jobInfo.HeartbeatTimeoutSec = heartbeatTimeoutSec;
            var updatedJobInfoEntity = jobInfo.ToTableEntity();

            // step 7: update message pop receipt to job lock entity
            jobLockEntity[JobLockEntityProperties.JobMessagePopReceipt] = message.PopReceipt;

            // step 8: transaction update jobInfo entity and job lock entity
            IEnumerable<TableTransactionAction> transactionUpdateActions = new List<TableTransactionAction>
            {
                new (TableTransactionActionType.UpdateReplace, updatedJobInfoEntity, jobInfoEntity.ETag),
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

            var jobInfoEntityResponse = await _azureJobInfoTableClient.GetEntityAsync<TableEntity>(
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

            var result = new ConcurrentBag<JobInfo>();

            // https://docs.microsoft.com/en-us/dotnet/api/system.threading.semaphoreslim?view=net-6.0
            using (var throttler = new SemaphoreSlim(MaxThreadsCountForGettingJob, MaxThreadsCountForGettingJob))
            {
                var tasks = jobIds.Select(async id =>
                {
                    await throttler.WaitAsync(cancellationToken).ConfigureAwait(false);
                    try
                    {
                        result.Add(await GetJobByIdAsync(queueType, id, returnDefinition, cancellationToken));
                    }
                    finally
                    {
                        throttler.Release();
                    }
                });

                await Task.WhenAll(tasks);
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

            var queryResult = _azureJobInfoTableClient.QueryAsync<TableEntity>(
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

            // step 1: get jobInfo entity and job lock entity
            var (jobInfoEntity, jobLockEntity) = await AcquireJobEntityByJobInfoAsync(jobInfo, cancellationToken);

            // step 2: check version
            // the version is assigned when dequeue,
            // if the version does not match, means there are more than one running jobs for it, only the last one keep alive
            if ((long)jobInfoEntity[JobInfoEntityProperties.Version] != jobInfo.Version)
            {
                _logger.LogError($"Job {jobInfo.Id} precondition failed, version does not match.");

                throw new JobNotExistException($"Job {jobInfo.Id} precondition failed, version does not match.");
            }

            // step 3: update message visibility timeout
            Response<UpdateReceipt>? response = null;
            try
            {
                var visibilityTimeout = TimeSpan.FromSeconds((long)jobInfoEntity[JobInfoEntityProperties.HeartbeatTimeoutSec] <= 0
                    ? DefaultVisibilityTimeoutInSeconds
                    : (long)jobInfoEntity[JobInfoEntityProperties.HeartbeatTimeoutSec]);
                response = await _azureJobMessageQueueClient.UpdateMessageAsync(
                    jobLockEntity.GetString(JobLockEntityProperties.JobMessageId),
                    jobLockEntity.GetString(JobLockEntityProperties.JobMessagePopReceipt),
                    visibilityTimeout: visibilityTimeout,
                    cancellationToken: cancellationToken);
            }
            catch (RequestFailedException ex) when (IsSpecifiedErrorCode(ex, AzureStorageErrorCode.UpdateOrDeleteMessageNotFoundErrorCode))
            {
                string message;
                if (jobInfo.Status is JobStatus.Completed or JobStatus.Failed or JobStatus.Cancelled)
                {
                    message = $"Job {jobInfo.Id} has been completed. Keep alive failed";
                    _logger.LogInformation(message);
                }
                else
                {
                    message = $"Failed to keep alive for job {jobInfo.Id}, the job message is not found.";
                    _logger.LogWarning(message);
                }

                throw new JobNotExistException(message, ex);
            }

            // step 5: sync result to jobInfo entity
            // if update message successfully while update table entity failed, then the message pop receipt is invalid,
            // keeping alive always fails to update message, so the message will be visible and dequeue again
            // when re-dequeue, the message pop receipt is updated to table entity,
            // and the previous job will throw JobNotExistException as the version doesn't match, and jobHosting cancels the previous job.
            jobInfoEntity[JobInfoEntityProperties.HeartbeatDateTime] = DateTime.UtcNow;

            jobInfoEntity[JobInfoEntityProperties.Result] = jobInfo.Result;

            // step 6: update message pop receipt to job lock entity
            jobLockEntity[JobLockEntityProperties.JobMessagePopReceipt] = response?.Value.PopReceipt;

            // step 7: transaction update jobInfo entity and job lock entity
            IEnumerable<TableTransactionAction> transactionUpdateActions = new List<TableTransactionAction>
            {
                new (TableTransactionActionType.UpdateReplace, jobInfoEntity, jobInfoEntity.ETag),
                new (TableTransactionActionType.UpdateReplace, jobLockEntity, jobLockEntity.ETag),
            };
            try
            {
                _ = await _azureJobInfoTableClient.SubmitTransactionAsync(transactionUpdateActions, cancellationToken);
            }
            catch (RequestFailedException ex) when (IsSpecifiedErrorCode(ex, AzureStorageErrorCode.RequestBodyTooLargeErrorCode))
            {
                _logger.LogError(ex, "The maximum size of a single table entity is 1MB, the size of result is larger than 1MB.");
                throw;
            }

            // step 8: check if cancel requested
            var shouldCancel = (await GetJobByIdAsync(jobInfo.QueueType, jobInfo.Id, false, cancellationToken)).CancelRequested;

            _logger.LogInformation($"Keep alive for job {jobInfo.Id} successfully.");

            return shouldCancel;
        }

        public async Task CancelJobByGroupIdAsync(byte queueType, long groupId, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start to cancel jobs in group {groupId}.");

            var jobInfoEntities = new List<TableEntity>();

            // step 1: query all job ids in a group, using range query for row key to ignore lock/index entities in same partition
            var queryResult = _azureJobInfoTableClient.QueryAsync<TableEntity>(
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
            var jobInfoEntity = (await _azureJobInfoTableClient.GetEntityAsync<TableEntity>(reverseIndexEntity.JobInfoEntityPartitionKey, reverseIndexEntity.JobInfoEntityRowKey, cancellationToken: cancellationToken)).Value;

            // step 3: cancel job
            CancelJobInternal(jobInfoEntity);

            // step 4: update job info entity to table.
            await _azureJobInfoTableClient.UpdateEntityAsync(jobInfoEntity, jobInfoEntity.ETag, cancellationToken: cancellationToken);

            _logger.LogInformation($"Cancel job {jobId} successfully.");
        }

        public async Task CompleteJobAsync(JobInfo jobInfo, bool requestCancellationOnFailure, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start to complete job {jobInfo.Id}.");

            // step 1: get jobInfo entity and job lock entity
            var (retrievedJobInfoEntity, jobLockEntity) = await AcquireJobEntityByJobInfoAsync(jobInfo, cancellationToken);

            // step 2: check version
            if ((long)retrievedJobInfoEntity[JobInfoEntityProperties.Version] != jobInfo.Version)
            {
                _logger.LogError($"Job {jobInfo.Id} precondition failed, version does not match.");
                throw new JobNotExistException($"Job {jobInfo.Id} precondition failed, version does not match.");
            }

            // step 3: get shouldCancel
            var shouldCancel = (bool)retrievedJobInfoEntity[JobInfoEntityProperties.CancelRequested];

            // step 4: update status
            // Reference: https://github.com/microsoft/fhir-server/blob/e1117009b6db995672cc4d31457cb3e6f32e19a3/src/Microsoft.Health.Fhir.SqlServer/Features/Schema/Sql/Sprocs/PutJobStatus.sql#L16
            var jobInfoEntity = ((TJobInfo)jobInfo).ToTableEntity();
            if ((int)jobInfoEntity[JobInfoEntityProperties.Status] == (int)JobStatus.Failed)
            {
                jobInfoEntity[JobInfoEntityProperties.Status] = (int)JobStatus.Failed;
            }
            else if (shouldCancel)
            {
                jobInfoEntity[JobInfoEntityProperties.Status] = (int)JobStatus.Cancelled;
            }
            else
            {
                jobInfoEntity[JobInfoEntityProperties.Status] = (int)JobStatus.Completed;
            }

            // step 5: update job info entity to table
            try
            {
                await _azureJobInfoTableClient.UpdateEntityAsync(jobInfoEntity, ETag.All, cancellationToken: cancellationToken);
            }
            catch (RequestFailedException ex) when (IsSpecifiedErrorCode(ex, AzureStorageErrorCode.RequestBodyTooLargeErrorCode))
            {
                _logger.LogError(ex, "The maximum size of a single table entity is 1MB, the size of result is larger than 1MB.");
                throw;
            }

            // step 6: delete message
            // if table entity is updated successfully while delete message fails, then the message is visible and dequeue again,
            // and the message will be deleted since the table entity's status is completed/failed/cancelled
            try
            {
                await _azureJobMessageQueueClient.DeleteMessageAsync(jobLockEntity.GetString(JobLockEntityProperties.JobMessageId), jobLockEntity.GetString(JobLockEntityProperties.JobMessagePopReceipt), cancellationToken: cancellationToken);
            }
            catch (RequestFailedException ex) when (IsSpecifiedErrorCode(ex, AzureStorageErrorCode.UpdateOrDeleteMessageNotFoundErrorCode))
            {
                _logger.LogWarning($"Failed to delete message, the message {jobLockEntity.GetString(JobLockEntityProperties.JobMessageId)} does not exist.");
            }

            // step 7: cancel jobs if requested
            if (requestCancellationOnFailure && jobInfo.Status == JobStatus.Failed)
            {
                await CancelJobByGroupIdAsync(jobInfo.QueueType, jobInfo.GroupId, cancellationToken);
            }

            _logger.LogInformation($"Complete job {jobInfo.Id} successfully.");
        }

        private void TryInitialize()
        {
            try
            {
                _azureJobInfoTableClient.CreateIfNotExists();
                _azureJobMessageQueueClient.CreateIfNotExists();
                _isInitialized = true;
                _logger.LogInformation("Initialize azure storage client successfully.");
            }
            catch (RequestFailedException ex) when (IsAuthenticationError(ex))
            {
                _logger.LogInformation(ex, "Failed to initialize azure storage client due to authentication issue.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize azure storage client.");
            }
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

        private async Task<Tuple<TableEntity, TableEntity>> AcquireJobEntityByJobInfoAsync(
            JobInfo jobInfo,
            CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNull(jobInfo, nameof(jobInfo));

            var pk = AzureStorageKeyProvider.JobInfoPartitionKey(jobInfo.QueueType, jobInfo.GroupId);
            var rks = new List<string>
            {
                AzureStorageKeyProvider.JobLockRowKey(((TJobInfo)jobInfo).JobIdentifier()),
                AzureStorageKeyProvider.JobInfoRowKey(jobInfo.GroupId, jobInfo.Id),
            };

            return await AcquireJobEntityByRowKeysAsync(pk, rks, cancellationToken);
        }

        private async Task<Tuple<TableEntity, TableEntity>> AcquireJobEntityByRowKeysAsync(
            string pk,
            List<string> rks,
            CancellationToken cancellationToken)
        {
            var jobEntityQueryResult = _azureJobInfoTableClient.QueryAsync<TableEntity>(
                filter: TransactionGetByKeys(pk, rks),
                cancellationToken: cancellationToken);

            var retrievedJobInfoEntities = new List<TableEntity>();
            var retrievedJobLockEntities = new List<TableEntity>();

            await foreach (var pageResult in jobEntityQueryResult.AsPages().WithCancellation(cancellationToken))
            {
                retrievedJobInfoEntities.AddRange(pageResult.Values.Where(entity => entity.ContainsKey(JobInfoEntityProperties.Id)));
                retrievedJobLockEntities.AddRange(pageResult.Values.Where(entity => !entity.ContainsKey(JobInfoEntityProperties.Id)));
            }

            return new Tuple<TableEntity, TableEntity>(retrievedJobInfoEntities.First(), retrievedJobLockEntities.First());
        }

        private static bool IsSpecifiedErrorCode(RequestFailedException exception, string expectedErrorCode) =>
            string.Equals(exception.ErrorCode, expectedErrorCode, StringComparison.OrdinalIgnoreCase);

        private static bool IsAuthenticationError(RequestFailedException exception) =>
            string.Equals(exception.ErrorCode, AzureStorageErrorCode.NoAuthenticationInformationErrorCode, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(exception.ErrorCode, AzureStorageErrorCode.InvalidAuthenticationInfoErrorCode, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(exception.ErrorCode, AzureStorageErrorCode.AuthenticationFailedErrorCode, StringComparison.OrdinalIgnoreCase);

        private static string FilterJobInfosByGroupId(byte queueType, long groupId) =>
            $"PartitionKey eq '{AzureStorageKeyProvider.JobInfoPartitionKey(queueType, groupId)}' and RowKey ge '{groupId:D20}' and RowKey lt '{groupId + 1:D20}'";

        private static string TransactionGetByKeys(string pk, List<string> rowKeys) =>
        $"PartitionKey eq '{pk}' and ({string.Join(" or ", rowKeys.Select(rowKey => $"RowKey eq '{rowKey}'"))})";

        private static IEnumerable<string> SelectPropertiesExceptDefinition()
        {
            var type = Type.GetType(typeof(JobInfoEntityProperties).FullName);
            if (type == null)
            {
                throw new JobManagementException("Failed to get JobInfoEntity properties, the type is null.");
            }

            var tableEntityProperties = new[] { "PartitionKey", "RowKey", "Timestamp", "ETag" };
            return tableEntityProperties.Concat(type.GetFields().Select(p => p.Name).Except(new List<string> { JobInfoEntityProperties.Definition }));
        }

        /// <summary>
        /// when cancel a job, always set the cancelRequest to true, and set its status to cancelled only when it's created,
        /// for other cases, shouldCancel will be returned when keep alive, and jobHosting will cancel this job and set job to completed
        /// </summary>
        private static void CancelJobInternal(TableEntity jobInfoEntity)
        {
            // set jobInfo entity's cancel request to true.
            jobInfoEntity[JobInfoEntityProperties.CancelRequested] = true;

            // only set job status to cancelled when the job status is created.
            if ((int)jobInfoEntity[JobInfoEntityProperties.Status] == (int)JobStatus.Created)
            {
                jobInfoEntity[JobInfoEntityProperties.Status] = (int)JobStatus.Cancelled;
            }
        }
    }
}