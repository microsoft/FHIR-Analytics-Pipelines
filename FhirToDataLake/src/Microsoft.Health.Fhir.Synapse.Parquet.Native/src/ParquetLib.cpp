#include "ParquetLib.h"

ParquetWriter* CreateParquetWriter()
{
	return new ParquetWriter();
}

void DestroyParquetWriter(ParquetWriter* writer)
{
	delete writer;
}

// Register parquet schema data with given key (resource type)
int RegisterParquetSchema(ParquetWriter* writer, const char* schemaKey, const char* schemaData)
{	
	string key = schemaKey;
	string data = schemaData;
	return writer->RegisterSchema(key, data);
}

// Convert input json data to output parquet stream. G
// Here we need to allocate a byte array for output stream in outputData[0] manually because the target output length is determined after computation.
int ConvertJsonToParquet(ParquetWriter* writer, const char* schemaKey, const char* inputJson, int inputLength, byte** outputData, int *outputLength)
{
	string key = schemaKey;
	return writer->Write(key, inputJson, inputLength, outputData, outputLength);
}

// Release the allocated parquet stream.
int ReleaseUnmanagedData(byte** data)
{
	delete *data;
	*data = nullptr;
	return 0;
}
