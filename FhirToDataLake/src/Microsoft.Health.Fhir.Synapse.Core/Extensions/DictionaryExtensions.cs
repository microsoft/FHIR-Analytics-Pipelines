// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.Core.Extensions
{
    public static class DictionaryExtensions
    {
        public static Dictionary<string, int> ConcatDictionaryCount(Dictionary<string ,int> dic1, Dictionary<string, int> dic2)
        {
            foreach (var (type, count) in dic2)
            {
                dic1 = AddToDictionary(dic1, type, count);
            }

            return dic1;
        }

        public static Dictionary<string, int> AddToDictionary(Dictionary<string, int> dic1, string key, int count)
        {
            if (!dic1.ContainsKey(key))
            {
                dic1[key] = count;
            }
            else
            {
                dic1[key] += count;
            }

            return dic1;
        }
    }
}
