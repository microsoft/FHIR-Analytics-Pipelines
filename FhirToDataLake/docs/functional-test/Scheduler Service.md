# Scheduler Service Functional Test

## Expected Behavior

- Constructor

  1. `QueueClient`, `MetadataStore` and `JobConfiguration` are required to create a Scheduler service instance.
  2. Valid `SchedulerCronExpression` in `JobConfiguration` is required.
  3. The `StartTime` and `EndTime` are nullable in `JobConfiguration`.
  4. The `StartTime` should be greater than `EndTime`.

- RunAsync
  
  - Scheduler service is a long running service, it shouldn't stop for any exception. It stops only when the job is scheduled to end or is cancelled by the caller.

  - Scheduler service repeats a process flow at regular intervals, in each processing:
     1. Checks if the storage is initialized at first, it will skip this processing and try next time if not.
     2. Try to acquire lease, it will skip this processing and try next time if acquire lease fails.
     3. Enter long running internal processing.
     4. It will catch any exceptions thrown by the above steps, log an error message and try next time.

    *Details*

    1. **TryAcquireLeaseAsync**: try to acquire lease, return true if lease is acquired, otherwise return false, will throw unexpected or cancellation exception.
    2. **ProcessInternalAsync**: This is the internal processing function, it triggers two long-running tasks, if one of the task completed, will cancel the other one.
       1. **RenewLeaseAsync**, A while-loop to renew lease, stop and return if cancelled or any exception, should not throw any exception
       2. **CheckAndUpdateTriggerEntityAsync**: a while-loop to check and update trigger entity, return true if The job has been scheduled to end, otherwise return false, It stops only when the job is scheduled to end or is cancelled by the caller.
          1. Execute **CheckAndUpdateTriggerEntityInternalAsync** at regular intervals. **CheckAndUpdateTriggerEntityInternalAsync** will not catch and handle any exception, all the exceptions thrown.
             1. Get `CurrentTriggerEntity` at first
             2. Check the `GetCurrentTriggerEntity`'s status
                1. If trigger status is `New` then **EnqueueOrchestratorJobAsync**
                2. If trigger status is `Running` then **CheckAndUpdateOrchestratorJobStatusAsync**
                3. If trigger status is `Compelete` then **CreateNextTriggerAsync**
                4. If trigger status is `Failed` or `Cancelled` then log a error message.
          2. Catch any exception thrown by **CheckAndUpdateTriggerEntityInternalAsync**, log a error message for it and continue the loop body.

## Test Cases

- Constructor

   1. **Null Input Parameters** - if one of the input parameter is null, should throw `ArgumentNullException`.
   2. **Invalid Job Configuration**
      1. If the `schedulerCronExpression` is null, should throw `ArgumentNullException`.
      2. If the `schedulerCronExpression` is invalid string, should throw `CrontabException`.
      3. If the `StartTime` and `EndTime` are null, no exception should be thrown.

- RunAsync  
  
  1. **Check Storage Initialization** - If the storage is uninitialized, should keep retrying at regular intervals and no throw any exception.
  2. **Acquire Lease**
     1. If the `MetadataStore` is broken, and throws an exception when try to get `TriggerLeaseEntity`, should keep retrying at regular intervals and no throw any exception.
     2. If the `MetadataStore` is broken, and returns null when try to get `TriggerLeaseEntity`, should add `TriggerLeaseEntity` to table, keep retrying at regular intervals and no throw any exception.
     3. If there is no `TriggerLeaseEntity`, should no throw any exception and create a new `TriggerLeaseEntity`.
     4. If the lease is acquired by another scheduler service, and is not timeout, the scheduler service should fail to acquire lease.
     5. If the lease is acquired by another scheduler service, and is timeout, a new scheduler service should acquire the lease.
     6. **[Concurrency]** If there are two scheduler services try to acquire lease, only one service will acquire successfully, the other one should require lease fail and keep retrying.
     7. **[Concurrency]** If there is a running scheduler service, the second scheduler service should fail to acquire lease. When the first scheduler service crashes, the second scheduler service should acquire lease successfully when the lease is timeout.
     8. **[Concurrency]** If  the existing lease is timeout, and there are two instances try to acquire the lease and update the entity, only one will success, and the failed one should return false.

  3. **Renew Lease**
     1. For a long running scheduler service, the lease should be renewed.
     2. If the lease is lost(acquired by another scheduler service), should fail to renew lease and keep retrying to try to acquire lease.

  4. **Check and Update Trigger Entity**
     1. For new job, the scheduler service should create a initial trigger entity and enqueue the orchestrator job successfully, the start time is null, and the created initial trigger entity's `TriggerStartTime` should be null.
     2. If the start time is specified in job configuration, the created initial trigger entity's `TriggerStartTime` should set to it.
     3. If there is an exception thrown while enqueuing, it should retry to enqueue next time.
     4. If the job already exists and try to re-enqueue it, the existing job should be returned.
     5. If the current job is completed, should create and enqueue the next job.
     6. If the job is scheduled to the end, should stop running.
  5. **Cancellation Request**
     1. If there is cancellation request when the scheduler service starts, the scheduler should be cancelled without any delay.
     2. If the storage is uninitialized, and the scheduler service keep retrying, when there is a cancellation request, the scheduler should be cancelled without any delay.
     3. If the lease is acquired by other scheduler service, and the scheduler service keep retrying to acquire lease, when there is a cancellation request, the scheduler should be cancelled without any delay.
     4. If there is a cancellation request when the scheduler `ProcessInternalAsync`, the scheduler should be cancelled without any delay.
  6. **[Functional Test]**
      1. For new job, the scheduler service should start to run and enqueue orchestrator job, when the orchestrator job is dequeued and completed, it should create the next job.
      2. When resume a running job, the job should be pick up and continue to run.
      3. When resume a failed job, the job status should keep failure and the lease is renewed.
      4. When resume a cancelled job, the job status should keep cancellation and the lease is renewed.