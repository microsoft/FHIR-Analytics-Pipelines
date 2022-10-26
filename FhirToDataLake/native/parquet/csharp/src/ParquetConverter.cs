// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;

namespace Microsoft.Health.Parquet
{
    public class ParquetConverter
    {
        private readonly IntPtr _nativeConverter;
        private const int ErrorMessageSize = 256;

        public ParquetConverter()
        {
            _nativeConverter = CreateParquetWriter();
        }

        ~ParquetConverter()
        {
            DestroyParquetWriter(_nativeConverter);
        }

        [DllImport("ParquetNative")]
        private static extern IntPtr CreateParquetWriter();

        [DllImport("ParquetNative")]
        private static extern void DestroyParquetWriter(IntPtr writer);

        [DllImport("ParquetNative")]
        private static extern int RegisterParquetSchema(IntPtr writer, string key, string value);

        [DllImport("ParquetNative")]
        private static extern int TryReleaseUnmanagedData(ref IntPtr outBuffer);

        [DllImport("ParquetNative")]
        private static extern int ConvertJsonToParquet(IntPtr writer, string key, [MarshalAs(UnmanagedType.LPUTF8Str)]string json, int inputSize, ref IntPtr outBuffer, out int outputSize, StringBuilder errorMessage);

        public Stream ConvertJsonToParquet(string schemaType, string inputJson)
        {
            // Output buffer pointer
            IntPtr outputPointer = IntPtr.Zero;

            if (inputJson == null)
            {
                throw new ParquetException(ParquetConverterErrorCodes.ReadInputJsonError, Resources.InputJsonIsNull);
            }

            // Get byte counts from input
            int inputSize = Encoding.UTF8.GetByteCount(inputJson);
            StringBuilder errorMessage = new StringBuilder(ErrorMessageSize);
            int status = ConvertJsonToParquet(_nativeConverter, schemaType, inputJson, inputSize, ref outputPointer, out int outputSize, errorMessage);
            if (status != 0)
            {
                TryReleaseUnmanagedData(ref outputPointer);
                throw new ParquetException(status, errorMessage.ToString());
            }

            if (outputPointer == IntPtr.Zero || outputSize == 0)
            {
                TryReleaseUnmanagedData(ref outputPointer);
                return null;
            }

            byte[] outputBuffer = new byte[outputSize];
            Marshal.Copy(outputPointer, outputBuffer, 0, outputSize);
            TryReleaseUnmanagedData(ref outputPointer);
            return new MemoryStream(outputBuffer);
        }

        public static ParquetConverter CreateWithSchemaSet(Dictionary<string, string> schemaSet)
        {
            var parquetConverter = new ParquetConverter();
            parquetConverter.InitializeSchemaSet(schemaSet);

            return parquetConverter;
        }

        private void InitializeSchemaSet(Dictionary<string, string> schemaSet)
        {
            foreach (var (key, value) in schemaSet)
            {
                int status = RegisterParquetSchema(_nativeConverter, key, value);
                if (status != 0)
                {
                    throw new ParquetException(status, Resources.InitializeSchemaSetError);
                }
            }
        }
    }
}
