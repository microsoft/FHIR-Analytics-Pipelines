// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Health.Fhir.Transformation.Core.Test
{
    [TestClass]
    public class TransformationExecutorTest
    {
        [TestMethod]
        public async Task GivenListOfContent_WhenTransform_ResultShouldBeReturnedInOrder()
        {
            const int testResourceCount = 100000;
            const int tableCount = 10;
            const int itemCount = 14;

            int itemLoaded = 0;
            bool sourceOpened = false;
            bool sourceClosed = false;
            var source = new TestSource();
            source.OpenInternalAsync = () => { sourceOpened = true; return Task.CompletedTask; };
            source.CloseInternalAsync = () => { sourceClosed = true; return Task.CompletedTask; };
            source.ReadInternalAsync = () =>
            {
                if (itemLoaded >= testResourceCount)
                {
                    return Task.FromResult<string>(null);
                }

                return Task.FromResult<string>($"{itemLoaded++}");
            };

            bool sinkInit = false;
            bool sinkComplete = false;
            List<string> itemRecords = new List<string>();
            var sink = new TestSink();
            sink.InitInternalAsync = () => { sinkInit = true; return Task.CompletedTask; };
            sink.WriteInternalAsync = (tableName, columns, entity) =>
            {
                itemRecords.Add($"{tableName}_{entity["index"].valueObj}");
                return Task.CompletedTask;
            };
            sink.CompleteInternalAsync = () =>
            {
                Assert.AreEqual(testResourceCount * tableCount * itemCount, itemRecords.Count);

                int index = 0;
                int tableIndex = 0;
                int itemIndex = 0;

                for (; index < testResourceCount; ++index)
                {
                    for (; tableIndex < tableCount; ++tableIndex)
                    {
                        for (; itemIndex < itemCount; ++itemIndex)
                        {
                            int actualIndex = index * tableCount * itemCount + tableIndex * itemCount + itemIndex;
                            Assert.AreEqual($"{index}_{tableIndex}_{itemIndex}", itemRecords[actualIndex]);
                        }
                    }
                }

                sinkComplete = true;
                return Task.CompletedTask;
            };

            TransformationExecutor executor = new TransformationExecutor(source, sink, null, null);
            executor.TransformToTabularEntitiesAsync =
                async (resourceContent, parser, tabularMappings, transformer) =>
                {
                    var result = new List<(string tableName, string[] columns, Dictionary<string, (object, object)>[] entities)>();

                    for (int i = 0; i < tableCount; ++i)
                    {
                        var items = Enumerable.Range(0, itemCount).Select(itemIndex => new Dictionary<string, (object, object)>() { { "index", (itemIndex, "") } }).ToArray();
                        result.Add(($"{resourceContent}_{i}", new string[0], items));
                    }

                    await Task.CompletedTask;
                    return result;
                };
            await executor.ExecuteAsync();

            Assert.IsTrue(sourceOpened);
            Assert.IsTrue(sourceClosed);
            Assert.IsTrue(sinkInit);
            Assert.IsTrue(sinkComplete);
        }


        [TestMethod]
        public async Task GivenTransformationExecutor_WhenThrowExceptionFromSource_SameExceptionShouldBeCatchOutside()
        {
            bool sourceOpened = false;
            bool sourceClosed = false;
            var source = new TestSource();
            source.OpenInternalAsync = () => { sourceOpened = true; return Task.CompletedTask; };
            source.CloseInternalAsync = () => { sourceClosed = true; return Task.CompletedTask; };
            source.ReadInternalAsync = () =>
            {
                throw new InvalidOperationException();
            };

            bool sinkInit = false;
            bool sinkComplete = false;
            var sink = new TestSink();
            sink.InitInternalAsync = () => { sinkInit = true; return Task.CompletedTask; };
            sink.CompleteInternalAsync = () => { sinkComplete = true; return Task.CompletedTask; };

            TransformationExecutor executor = new TransformationExecutor(source, sink, null, null);
            executor.TransformToTabularEntitiesAsync =
                async (resourceContent, parser, tabularMappings, transformer) =>
                {
                    await Task.CompletedTask;

                    var result = new List<(string tableName, string[] columns, Dictionary<string, (object, object)>[] entities)>();
                    return result;
                };
            try
            {
                await executor.ExecuteAsync();
                throw new NotSupportedException();
            }
            catch (InvalidOperationException)
            {
                // InvalidOperationException should be catch here.
            }

            Assert.IsTrue(sourceOpened);
            Assert.IsTrue(sourceClosed);
            Assert.IsTrue(sinkInit);
            Assert.IsTrue(sinkComplete);
        }

        [TestMethod]
        public async Task GivenTransformationExecutor_WhenThrowExceptionFromSink_SameExceptionShouldBeCatchOutside()
        {
            bool sourceOpened = false;
            bool sourceClosed = false;
            var source = new TestSource();
            source.OpenInternalAsync = () => { sourceOpened = true; return Task.CompletedTask; };
            source.CloseInternalAsync = () => { sourceClosed = true; return Task.CompletedTask; };
            source.ReadInternalAsync = () =>
            {
                return Task.FromResult("1");
            };

            bool sinkInit = false;
            bool sinkComplete = false;
            var sink = new TestSink();
            sink.InitInternalAsync = () => { sinkInit = true; return Task.CompletedTask; };
            sink.CompleteInternalAsync = () => { sinkComplete = true; return Task.CompletedTask; };
            sink.WriteInternalAsync = (arg1, arg2, arg3) =>
            {
                throw new InvalidOperationException();
            };

            TransformationExecutor executor = new TransformationExecutor(source, sink, null, null);
            executor.TransformToTabularEntitiesAsync =
                async (resourceContent, parser, tabularMappings, transformer) =>
                {
                    await Task.CompletedTask;

                    var result = new List<(string tableName, string[] columns, Dictionary<string, (object, object)>[] entities)>();
                    var items = Enumerable.Range(0, 2).Select(itemIndex => new Dictionary<string, (object, object)>() { { "index", (itemIndex, "") } }).ToArray();
                    result.Add(("", new string[0], items));

                    return result;
                };
            try
            {
                await executor.ExecuteAsync();
                throw new NotSupportedException();
            }
            catch (InvalidOperationException)
            {
                // InvalidOperationException should be catch here.
            }

            Assert.IsTrue(sourceOpened);
            Assert.IsTrue(sourceClosed);
            Assert.IsTrue(sinkInit);
            Assert.IsTrue(sinkComplete);
        }

        [TestMethod]
        public async Task GivenTransformationExecutor_WhenThrowExceptionFromExecutor_SameExceptionShouldBeCatchOutside()
        {
            bool sourceOpened = false;
            bool sourceClosed = false;
            var source = new TestSource();
            source.OpenInternalAsync = () => { sourceOpened = true; return Task.CompletedTask; };
            source.CloseInternalAsync = () => { sourceClosed = true; return Task.CompletedTask; };
            source.ReadInternalAsync = () =>
            {
                return Task.FromResult("1");
            };

            bool sinkInit = false;
            bool sinkComplete = false;
            var sink = new TestSink();
            sink.InitInternalAsync = () => { sinkInit = true; return Task.CompletedTask; };
            sink.CompleteInternalAsync = () => { sinkComplete = true; return Task.CompletedTask; };

            TransformationExecutor executor = new TransformationExecutor(source, sink, null, null);
            executor.TransformToTabularEntitiesAsync =
                (resourceContent, parser, tabularMappings, transformer) =>
                {
                    throw new InvalidOperationException();
                };
            try
            {
                await executor.ExecuteAsync();
                throw new NotSupportedException();
            }
            catch (InvalidOperationException)
            {
                // InvalidOperationException should be catch here.
            }

            Assert.IsTrue(sourceOpened);
            Assert.IsTrue(sourceClosed);
            Assert.IsTrue(sinkInit);
            Assert.IsTrue(sinkComplete);
        }
    }

    public class TestSource : ISource
    {
        public Func<Task> CloseInternalAsync
        {
            get;
            set;
        } = null;

        public Func<Task> OpenInternalAsync
        {
            get;
            set;
        } = null;

        public Func<Task<string>> ReadInternalAsync
        {
            get;
            set;
        } = null;

        public Task CloseAsync()
        {
            return CloseInternalAsync?.Invoke() ?? Task.CompletedTask;
        }

        public Task OpenAsync()
        {
            return OpenInternalAsync?.Invoke() ?? Task.CompletedTask;
        }

        public Task<string> ReadAsync()
        {
            return ReadInternalAsync?.Invoke() ?? Task.FromResult<string>(null);
        }
    }

    public class TestSink : ISink
    {
        public Func<Task> CompleteInternalAsync
        {
            get;
            set;
        } = null;

        public Func<Task> InitInternalAsync
        {
            get;
            set;
        } = null;

        public Func<string, string[], Dictionary<string, (object valueObj, object typeObj)>, Task> WriteInternalAsync
        {
            get;
            set;
        } = null;

        public Task CompleteAsync()
        {
            return CompleteInternalAsync?.Invoke() ?? Task.CompletedTask;
        }

        public Task InitAsync()
        {
            return InitInternalAsync?.Invoke() ?? Task.CompletedTask;
        }

        public Task WriteAsync(string tableName, string[] columns, Dictionary<string, (object valueObj, object typeObj)> item)
        {
            return WriteInternalAsync?.Invoke(tableName, columns, item) ?? Task.CompletedTask;
        }
    }
}
