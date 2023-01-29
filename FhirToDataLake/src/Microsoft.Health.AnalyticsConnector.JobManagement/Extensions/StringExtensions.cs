// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Security.Cryptography;
using System.Text;

namespace Microsoft.Health.AnalyticsConnector.JobManagement.Extensions
{
    public static class StringExtensions
    {
        /// <summary>
        /// Compute hash of a string
        /// </summary>
        /// <param name="input">the input string</param>
        /// <returns>hash string</returns>
        public static string ComputeHash(this string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                return input;
            }

            var sb = new StringBuilder();

            using (var hash = SHA256.Create())
            {
                byte[] result = hash.ComputeHash(Encoding.UTF8.GetBytes(input));
                foreach (byte b in result)
                {
                    sb.Append(b.ToString("x2"));
                }
            }

            return sb.ToString();
        }
    }
}