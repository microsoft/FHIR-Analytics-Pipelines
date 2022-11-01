

Target:
1. Empty input
2. Invalid input types
3. Large input
4. Empty result

## Cpp

### Fix
- Return error code when register parquet schema with empty or whitespace key.
- Use property key of schema node, rather than "Name" property value to create arrow schema.

    ```
    ...
    "resourceType": {
        "Name": "resourceType",
        "Type": "string",
        ...
        },
    ...
    ```
- Make output error message pointer in ParquetWriter_Write interface can be null, if so will only return error code and not give error message.

    ```
    int Write(const string& resourceType, const char* inputJson, int inSize, byte** outputData, int* outSize, char* errorMessage=nullptr);
    ```

### Test

- ParquetLib
    
    - int RegisterParquetSchema(ParquetWriter* writer, const char* schemaKey, const char* schemaData);
       
       1. Correctly register parquet schema to Parquet writer.
       2. **[Refine]** Can return error code if schema content is empty, white space or null.
       3. **[Add]** Can return error code if schema key is empty, white space or null.
   
    - int ConvertJsonToParquet(ParquetWriter* writer, const char* schemaKey, const char* inputJson, int inputLength, byte** outputData, int* outputLength, char* errorMessage);

      1. Correctly convert Json data to Parquet.
      2. **[Add]** Can return error code if schema key is null.

    - int TryReleaseUnmanagedData(byte** data);
      
      1. **[Add]** Confirm the data is released.

1. ParquetWriter

    - int RegisterSchema(const string& schemaKey, const string& schemaData);
      1. Correct register schema with given schema key.
      2. **[Refine]** Can return error code if schema content is empty, white space, invalid Json or null.
      3. **[Refine]** Can return error code if schema key is empty, white space or null.

    - int Write(const string& resourceType, const char* inputJson, int inSize, byte** outputData, int* outSize, char* errorMessage=nullptr);
      1. Correct write single and batch Patient data.
      2. **[Add]** Can only return error code when error message pointer is null.
      3. **[Add]** Can give error message and return error code if input data is null.
      4. **[Add]** Can give error message and return error code if output data pointer or output data size is null.

2. SchemaManager
    - int AddSchema(const string& schemaKey, const string& schemaJson);
      1. Correct add schema with given schema key.
      2. **[Add]** Can return false if schema content is empty, white space or invalid Json.
      3. **[Add]** Can return false if schema key is empty or white space.

    - shared_ptr<arrow::Schema> GetSchema(const string& schemaKey);
      1. Correct return schema with given schema key.

## CSharp

### Fix
- Add try release unmanaged data when converting fail and throw exception.
- Fix unit tests name "ConvertingToJson" -> "ConvertingToParquet".

### Test
- ParquetConverter

    - Stream ConvertJsonToParquet(string schemaType, string inputJson);
      1. Correct convert Json to Parquet table.
      2. **[Refine]** Can throw exceptions when input Json data is empty, invalid Json or null.
      3. **[Add]** Can throw exceptions when input schema key is empty, white space or null.
      4. **[Add]** Correct convert Json to Parquet table for large input data.
      5. **[Add]** Correct convert Json to Parquet table for multiple trigger convert.

    - ParquetConverter CreateWithSchemaSet(Dictionary<string, string> schemaSet)
      1. Correct create a ParquetConverter instance with schema set.
      2. **[Add]** Can throw exceptions for empty, white space, invalid Json for null schema content.
      3. **[Add]** Can throw exceptions for empty or white spac schema key.

## Document

- Add ```git submodule``` in github Native readme file.

