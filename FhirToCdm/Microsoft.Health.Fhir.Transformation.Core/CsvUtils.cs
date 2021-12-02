// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace Microsoft.Health.Fhir.Transformation.Core
{
    public static class CsvUtils
    {
        private const string DoubleQuotationMark = "\"\"";
        private const string QuotationMark = "\"";
        private const string Delimiter = ",";

        public static string ConvertToCsvRow(string[] columns, Dictionary<string, (object valueObj, object typeObj)> item)
        {
            return string.Join(",", columns.Select(column => ParseCell(DataItemToString(item[column]))));
        }

        public static string ParseCell(string cell)
        {
            cell = cell.Replace("\r", "");
            cell = cell.Replace("\n", "");

            if (cell.Contains(QuotationMark) || cell.Contains(Delimiter))
            {
                cell = cell.Replace(QuotationMark, DoubleQuotationMark);
                cell = $"{QuotationMark}{cell}{QuotationMark}";
            }

            return cell;
        }

        public static List<string> SplitCsvLine(string line)
        {
            string pattern = ",(?=([^\"\"]*\"[^\"\"]*\"\")*[^\"\"]*$)";
            var splited = new List<string>(System.Text.RegularExpressions.Regex.Split(line, pattern));
            return splited.Select(cell => cell.Contains(DoubleQuotationMark) ? cell.Replace(DoubleQuotationMark, QuotationMark) : cell).ToList();
        }

        private static string DataItemToString((object valueObj, object typeObj) item)
        {
            if (item.valueObj == null)
            {
                return string.Empty;
            }

            if (item.valueObj is DateTime)
            {
                return ((DateTime)item.valueObj).ToString(CultureInfo.InvariantCulture);
            }

            return item.valueObj.ToString();
        }
    }
}
