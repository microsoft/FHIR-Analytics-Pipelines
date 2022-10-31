// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Core.Extensions;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Extensions
{
    public class DictionaryExtensionsTests
    {
        private const string _key = "key";

        [Fact]
        public void GivenANewKey_WhenAddToDictionary_TheKeyValuePairShouldBeAddedToDictionary()
        {
            Dictionary<string, int> dic = new Dictionary<string, int> { { _key, 1 } };
            dic = dic.AddToDictionary("b", 2);
            Assert.Equal(2, dic.Count);
            Assert.True(dic.ContainsKey(_key));
            Assert.Equal(1, dic[_key]);
            Assert.True(dic.ContainsKey("b"));
            Assert.Equal(2, dic["b"]);
        }

        [Fact]
        public void GivenAnExistingKey_WhenAddToDictionary_TheValueShouldBeAddedToDictionary()
        {
            Dictionary<string, int> dic = new Dictionary<string, int> { { _key, 1 } };
            dic = dic.AddToDictionary(_key, 2);
            Assert.Single(dic);
            Assert.True(dic.ContainsKey(_key));
            Assert.Equal(3, dic[_key]);
        }

        [Fact]
        public void GivenEmptyDictionary_WhenAddToDictionary_TheKeyValuePairShouldBeAddedToDictionary()
        {
            Dictionary<string, int> dic = new Dictionary<string, int>();
            dic = dic.AddToDictionary(_key, 1);
            Assert.Single(dic);
            Assert.True(dic.ContainsKey(_key));
            Assert.Equal(1, dic[_key]);
        }

        [Fact]
        public void GivenSameKeyMultiTimes_WhenAddToDictionary_TheValuePairShouldBeAddedToDictionary()
        {
            Dictionary<string, int> dic = new Dictionary<string, int> { { _key, 1 } };
            dic = dic.AddToDictionary(_key, 1);
            Assert.Single(dic);
            Assert.True(dic.ContainsKey(_key));
            Assert.Equal(2, dic[_key]);

            dic = dic.AddToDictionary(_key, 1);
            Assert.Single(dic);
            Assert.True(dic.ContainsKey(_key));
            Assert.Equal(3, dic[_key]);

            dic = dic.AddToDictionary(_key, 1);
            Assert.Single(dic);
            Assert.True(dic.ContainsKey(_key));
            Assert.Equal(4, dic[_key]);
        }

        [Fact]
        public void GivenNullDictionary_WhenAddToDictionary_ExceptionShouldBeThrown()
        {
            Dictionary<string, int> dic = null;
            Assert.Throws<ArgumentNullException>(() => dic.AddToDictionary(_key, 1));
        }

        [Fact]
        public void GivenNullOrEmptyKey_WhenAddToDictionary_ExceptionShouldBeThrown()
        {
            Dictionary<string, int> dic = new Dictionary<string, int>();
            Assert.Throws<ArgumentNullException>(() => dic.AddToDictionary(null, 1));
            Assert.Throws<ArgumentException>(() => dic.AddToDictionary(string.Empty, 1));
        }

        [Fact]
        public void GivenValidDictionary_WhenConcatDictionaryCount_TheDictionaryShouldBeConcatCorrectly()
        {
            Dictionary<string, int> dic1 = new Dictionary<string, int> { { "key1", 1 }, { "key2", 2 } };
            Dictionary<string, int> dic2 = new Dictionary<string, int> { { "key2", 3 }, { "key3", 4 } };
            dic1 = dic1.ConcatDictionaryCount(dic2);

            Assert.Equal(3, dic1.Count);
            Assert.True(dic1.ContainsKey("key1"));
            Assert.Equal(1, dic1["key1"]);
            Assert.True(dic1.ContainsKey("key2"));
            Assert.Equal(5, dic1["key2"]);
            Assert.True(dic1.ContainsKey("key3"));
            Assert.Equal(4, dic1["key3"]);
        }

        [Fact]
        public void GivenEmptyDictionary_WhenConcatDictionaryCount_TheDictionaryShouldNoChange()
        {
            Dictionary<string, int> dic1 = new Dictionary<string, int> { { "key1", 1 }, { "key2", 2 } };
            Dictionary<string, int> dic2 = new Dictionary<string, int>();
            Dictionary<string, int> dic = dic1.ConcatDictionaryCount(dic2);

            Assert.Equal(dic1.Count, dic.Count);

            foreach ((string key, int value) in dic)
            {
                Assert.True(dic1.ContainsKey(key));
                Assert.Equal(dic1[key], dic[key]);
            }
        }

        [Fact]
        public void GivenNullDictionary_WhenConcatDictionaryCount_ExceptionShouldBeThrown()
        {
            Dictionary<string, int> dic1 = new Dictionary<string, int>();
            Assert.Throws<ArgumentNullException>(() => dic1.ConcatDictionaryCount(null));

            Dictionary<string, int> dic2 = null;
            Assert.Throws<ArgumentNullException>(() => dic2.ConcatDictionaryCount(dic1));
        }
    }
}
