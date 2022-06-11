// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class JobProgressUpdater
    {
        private readonly IJobStore _jobStore;
        private readonly Job _job;
        private readonly Channel<Tuple<string, int>> _progressChannel;
        private readonly ILogger<JobProgressUpdater> _logger;

        // Time interval to sync job to store.
        private const int UploadDataIntervalInSeconds = 30;

        public JobProgressUpdater(
            IJobStore jobStore,
            Job job,
            ILogger<JobProgressUpdater> logger)
        {
            EnsureArg.IsNotNull(jobStore, nameof(jobStore));
            EnsureArg.IsNotNull(job, nameof(job));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _jobStore = jobStore;
            _job = job;
            _logger = logger;
            var channelOptions = new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false,
            };
            _progressChannel = Channel.CreateUnbounded<Tuple<string, int>>(channelOptions);
        }

        public async Task Consume(CancellationToken cancellationToken = default)
        {
            var uploadTime = DateTimeOffset.MinValue;

            var channelReader = _progressChannel.Reader;
            while (await channelReader.WaitToReadAsync(cancellationToken))
            {
                if (channelReader.TryRead(out Tuple<string, int> result))
                {
                    _job.TotalResourceCounts[result.Item1] += result.Item2;
                    _logger.LogWarning($"{DateTime.Now} Progress report: {result.Item1} processed {_job.TotalResourceCounts[result.Item1]}");

                    var sum = _job.TotalResourceCounts.Values.Sum();
                    _logger.LogWarning($"{DateTime.Now} Progress summary: {sum} resource processed");

                    // await _jobStore.UpdateJobAsync(_job, cancellationToken);

                    if (uploadTime.AddSeconds(UploadDataIntervalInSeconds) < DateTimeOffset.UtcNow)
                    {
                        uploadTime = DateTimeOffset.UtcNow;

                        // Upload to job store.
                        await _jobStore.UpdateJobAsync(_job, cancellationToken);
                        // _logger.LogInformation("Update job {jobId} progress successfully.", _job.Id);
                    }
                }
            }

            await _jobStore.UpdateJobAsync(_job, cancellationToken);
        }

        public async Task Produce(Tuple<string, int> value, CancellationToken cancellationToken = default)
        {
            var channelWriter = _progressChannel.Writer;
            await channelWriter.WriteAsync(value, cancellationToken);
        }

        public void Complete()
        {
            _progressChannel.Writer.Complete();
        }
    }
}
