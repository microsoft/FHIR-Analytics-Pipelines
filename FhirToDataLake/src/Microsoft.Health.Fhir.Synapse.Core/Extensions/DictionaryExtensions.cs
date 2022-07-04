// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using EnsureThat;

namespace Microsoft.Health.Fhir.Synapse.Core.Extensions
{
    public static class DictionaryExtensions
    {
        /// <summary>
        /// Concat two string to int dictionary, for duplicated key, add the their value together.
        /// For example. the concat result of dic1 {{"key1",1}} and dic2 {{"key1",2}, {"key2", 5}} is {{"key1",3}, {"key2", 5}}
        /// </summary>
        /// <param name="dic1">the first dictionary.</param>
        /// <param name="dic2">the second dictionary.</param>
        /// <returns>the concat dictionary.</returns>
        public static Dictionary<string, int> ConcatDictionaryCount(this Dictionary<string ,int> dic1, Dictionary<string, int> dic2)
        {
            EnsureArg.IsNotNull(dic1, nameof(dic1));
            EnsureArg.IsNotNull(dic2, nameof(dic2));

            foreach (var (type, count) in dic2)
            {
                dic1 = dic1.AddToDictionary(type, count);
            }

            return dic1;
        }

        /// <summary>
        /// Add key(string)/value(int) to a dictionary,
        /// if the key already exists, accumulate the count the existing value,
        /// if the key doesn't exist, insert the key/value.
        /// </summary>
        /// <param name="dic">the string to int dictionary.</param>
        /// <param name="key">the key to be added.</param>
        /// <param name="count">the int value to be added for the specified key.</param>
        /// <returns>the dictionary with the key/value added.</returns>
        public static Dictionary<string, int> AddToDictionary(this Dictionary<string, int> dic, string key, int count)
        {
            EnsureArg.IsNotNull(dic, nameof(dic));

            if (string.IsNullOrEmpty(key))
            {
                throw new ArgumentException($"The input key is null or empty");
            }

            if (!dic.ContainsKey(key))
            {
                dic[key] = count;
            }
            else
            {
                dic[key] += count;
            }

            return dic;
        }
    }
}
