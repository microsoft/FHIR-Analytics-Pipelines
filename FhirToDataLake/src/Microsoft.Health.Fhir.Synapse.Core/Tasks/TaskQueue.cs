using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;

namespace Microsoft.Health.Fhir.Synapse.Core.Tasks
{
    public class TaskQueue
    {
        private readonly Channel<TaskContext> _queue;

        public TaskQueue()
        {
            var options = new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = true,
            };

            _queue = Channel.CreateUnbounded<TaskContext>(options);
        }

        public async Task Enqueue(TaskContext context)
        {
            var channelWriter = _queue.Writer;
            await channelWriter.WriteAsync(context);
        }

        public async Task<TaskContext> Dequeue()
        {
            var channelReader = _queue.Reader;
            while (await channelReader.WaitToReadAsync())
            {
                if (channelReader.TryRead(out TaskContext context))
                {
                    return context;
                }
            }

            return null;
        }

        public void Complete()
        {
            _queue.Writer.Complete();
        }
    }
}
