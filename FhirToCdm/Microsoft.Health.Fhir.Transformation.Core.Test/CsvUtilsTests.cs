// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Health.Fhir.Transformation.Core.Test
{
    [TestClass]
    public class CsvUtilsTests
    {
        [TestMethod]
        public void GivenStringContainsSpecialCharactor_WhenParseCell_ExcapedStringShouldBeReturn()
        {
            Dictionary<string, string> testStrings = new Dictionary<string, string>()
            {
                { "abc123", "abc123"},
                { "abc,123", "\"abc,123\""},
                { "abc\"123", "\"abc\"\"123\""},
                { "abc,\"123", "\"abc,\"\"123\""},
                { "abc,\r\n123", "\"abc,123\""},
                { "abc\r\n123", "abc123"}
            };

            foreach (var keyValuePair in testStrings)
            {
                Assert.AreEqual(keyValuePair.Value, CsvUtils.ParseCell(keyValuePair.Key));
            }
        }

        [TestMethod]
        public void GivenObject_WhenConvertToCsvRow_ExcapedStringShouldBeReturn()
        {
            Dictionary<string, (object, object)> testObject = new Dictionary<string, (object, object)>()
            {
                { "c1", ("abc123", "string")},
                { "c3", ("abc,123", "string")},
                { "c2", ("abc\"123", "string")},
                { "c4", (123.4, "float")}
            };
            string[] columns = new string[] { "c1", "c2", "c3", "c4"};
            Assert.AreEqual("abc123,\"abc\"\"123\",\"abc,123\",123.4", CsvUtils.ConvertToCsvRow(columns, testObject));
        }

        [TestMethod]
        public void GivenWrapedLine_WhenSplited_ExcapedCellsShouldBeReturn()
        {
            List <(string,  List<string>)> testExamples = new List<(string, List<string>)>()
            {
                {("abc123,abc123", new List<string>{"abc123", "abc123"})},
                {("abc123 , abc123", new List<string>{"abc123 ", " abc123"})},
                {("\"\"abc123,abc123", new List<string>{"\"abc123", "abc123"})},
                {("\"\"abc,123\"\",abc123", new List<string>{"\"abc,123\"", "abc123"})},
                {("\"\"\"\"abc,123\"\",abc123", new List<string>{"\"\"abc,123\"", "abc123"})},
            };
            foreach (var testExample in testExamples)
            {
                var result = CsvUtils.SplitCsvLine(testExample.Item1);
                var expected = testExample.Item2;
                Assert.IsTrue(result.SequenceEqual(expected));
            }
        }
    }
}
