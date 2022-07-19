using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;

namespace Microsoft.Health.Fhir.Synapse.Parquet
{
    public class ParquetConverter
    {
        [DllImport("ParquetNative")]
        private static extern int RegisterParquetSchema(string key, string value);
        
        [DllImport("ParquetNative")]
        private static extern int ReleaseUnmanagedData(ref IntPtr outBuffer);

        [DllImport("ParquetNative")]
        private static extern int ConvertJsonToParquet(string key, string json, int inputSize, ref IntPtr outBuffer, out int outputSize);

        public void InitializeSchemaSet(Dictionary<string, string> schemaSet)
        {
            foreach(var (key, value) in schemaSet)
            {
                int status = RegisterParquetSchema(key, value);
                if (status != 0)
                {
                    throw new ParquetException(status);
                }
            }
        }

        public Stream ConvertJsonToParquet(string schemaType, string inputJson)
        {
            IntPtr outputPointer = IntPtr.Zero;
			int inputSize = Encoding.UTF8.GetByteCount(inputJson);
            int status = ConvertJsonToParquet(schemaType, inputJson, inputSize, ref outputPointer, out int size);
            if (status != 0)
            {
                throw new ParquetException(status);
            }

            if (outputPointer == IntPtr.Zero || size == 0)
            {
                return null;
            }

            byte[] outputBuffer = new byte[size];
            Marshal.Copy(outputPointer, outputBuffer, 0, size);
            ReleaseUnmanagedData(ref outputPointer);
            return new MemoryStream(outputBuffer);
        }
    }
}
