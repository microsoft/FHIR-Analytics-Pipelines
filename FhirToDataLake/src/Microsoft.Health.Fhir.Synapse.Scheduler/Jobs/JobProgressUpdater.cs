// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;

namespace Microsoft.Health.Fhir.Synapse.Scheduler.Jobs
{
    public class JobProgressUpdater
    {
        private readonly IJobStore _jobStore;
        private readonly Job _job;
        private Channel<TaskContext> _progressChannel;
        private const int UploadDataInterval = 10;

        public JobProgressUpdater(IJobStore jobStore, Job job)
        {
            EnsureArg.IsNotNull(jobStore, nameof(jobStore));
            EnsureArg.IsNotNull(job, nameof(job));

            _jobStore = jobStore;
            _job = job;
            var channelOptions = new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false,
            };
            _progressChannel = Channel.CreateUnbounded<TaskContext>(channelOptions);
        }

        public async Task Consume(CancellationToken cancellationToken = default)
        {
            var channelReader = _progressChannel.Reader;
            var contextCount = 0;
            while (await channelReader.WaitToReadAsync(cancellationToken))
            {
                if (channelReader.TryRead(out TaskContext context))
                {
                    _job.ResourceProgresses[context.ResourceType] = context.ContinuationToken;
                    _job.TotalResourceCounts[context.ResourceType] = context.SearchCount;
                    _job.ProcessedResourceCounts[context.ResourceType] = context.ProcessedCount;
                    _job.SkippedResourceCounts[context.ResourceType] = context.SkippedCount;
                    _job.PartIds[context.ResourceType] = context.PartId;

                    contextCount++;
                    if (contextCount % UploadDataInterval == 0)
                    {
                        // Upload to job store.
                        await _jobStore.UpdateJobAsync(_job, cancellationToken);
                    }
                }
            }

            await _jobStore.UpdateJobAsync(_job, cancellationToken);
        }

        public async Task Produce(TaskContext context, CancellationToken cancellationToken = default)
        {
            var channelWriter = _progressChannel.Writer;
            await channelWriter.WriteAsync(context, cancellationToken);
        }

        public void Complete()
        {
            _progressChannel.Writer.Complete();
        }
    }
}
