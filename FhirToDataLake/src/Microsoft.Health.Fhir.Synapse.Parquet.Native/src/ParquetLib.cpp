#include "ParquetLib.h"

ParquetWriter* CreateParquetWriter()
{
	return new ParquetWriter();
}

void DestroyParquetWriter(ParquetWriter* writer)
{
	if (writer != nullptr)
	{
		delete writer;
		writer = nullptr;
	}
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
int ConvertJsonToParquet(ParquetWriter* writer, const char* schemaKey, const char* inputJson, int inputLength, byte** outputData, int *outputLength, char* errorMessage)
{
	string key = schemaKey;
	return writer->Write(key, inputJson, inputLength, outputData, outputLength, errorMessage);
}

// Release the allocated parquet stream.
int ReleaseUnmanagedData(byte** data)
{
	if (data != nullptr && *data != nullptr)
	{
		delete *data;
		*data = nullptr;
	}

	return 0;
}
