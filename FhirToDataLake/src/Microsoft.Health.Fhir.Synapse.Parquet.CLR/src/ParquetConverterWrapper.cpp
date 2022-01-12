#pragma unmanaged
#include <arrow/api.h>
#include "../../Microsoft.Health.Fhir.Synapse.Parquet.Native/ParquetConverter.h"
#pragma managed

#include "ParquetConverterWrapper.h"

using namespace Collections::Generic;

namespace Microsoft
{
    namespace Health
    {
        namespace Fhir
        {
            namespace Synapse
            {
                namespace Parquet
                {
                    namespace CLR
                    {
                        ParquetConverterWrapper::ParquetConverterWrapper(Dictionary<String^, FhirParquetSchemaNode^>^ schemaMap)
                            : ParquetConverterWrapper(schemaMap, gcnew ArrowConfiguration())
                        {
                        }

                        ParquetConverterWrapper::ParquetConverterWrapper(Dictionary<String^, FhirParquetSchemaNode^>^ schemaMap, ArrowConfiguration^ arrowConfiguration)
                            : ManagedObject(new Native::ParquetConverter(CreateNativeArrowParquetOption(arrowConfiguration)))
                        {
                            if (schemaMap == nullptr)
                            {
                                throw gcnew ArgumentNullException("Schema map should not be null.");
                            }
                            
                            _arrowSchemaManager = gcnew ArrowSchemaManager(schemaMap);
                        }

                        IO::MemoryStream^ ParquetConverterWrapper::ConvertToParquetStream(String^ resourceType, IO::MemoryStream^ stream)
                        {
                            if (resourceType == nullptr)
                            {
                                throw gcnew ArgumentNullException("The input resource type should not be null");
                            }

                            if (stream == nullptr || stream->Length == 0)
                            {
                                throw gcnew ArgumentException("The input stream should not be null or empty");
                            }

                            // Convert C# array<unsigned char> to C++ char*
                            // Reference: https://docs.microsoft.com/en-us/cpp/extensions/how-to-pin-pointers-and-arrays?view=msvc-160
                            auto bytes = stream->ToArray();
                            const pin_ptr<unsigned char> arrayPin = &bytes[0];
                            unsigned char* uchars = arrayPin;
                            const char* chars = reinterpret_cast<char*>(uchars);

                            // Exception will be thrown if the schema is not found
                            const auto schema = _arrowSchemaManager->GetArrowSchema(resourceType);

                            // Convert input to parquet buffer
                            std::shared_ptr<arrow::Buffer> buffer;
                            const Native::Status status = _instance->ConvertToParquetBuffer(chars, bytes->Length, schema, buffer);
                            if (!status.IsOk())
                            {
                                throw gcnew InvalidOperationException("Convert to parquet buffer failed: " + gcnew String(status.ToString().c_str()));
                            }

                            // Convert parquet buffer to output stream
                            array<unsigned char>^ outputBuffer = gcnew array<unsigned char>(buffer->size());
                            System::Runtime::InteropServices::Marshal::Copy(IntPtr((void*)buffer->data()), outputBuffer, 0, buffer->size());
                            IO::MemoryStream^ outputStream = gcnew IO::MemoryStream(outputBuffer);

                            return outputStream;
                        }

                        Native::ArrowParquetOption ParquetConverterWrapper::CreateNativeArrowParquetOption(ArrowConfiguration^ arrowConfiguration)
                        {
                            if (arrowConfiguration == nullptr)
                            {
                                throw gcnew ArgumentNullException("Arrow configuration option should not be null.");
                            }

                            Native::ArrowParquetOption nativeOption;
                            nativeOption.UseThreads = arrowConfiguration->ReadOptions->UseThreads;
                            nativeOption.BlockSize = arrowConfiguration->ReadOptions->BlockSize;

                            switch (arrowConfiguration->ParseOptions->UnexpectedFieldBehavior)
                            {
                            case UnexpectedFieldBehaviorOptions::Ignore:
                                nativeOption.UnexpectedFieldBehavior = arrow::json::UnexpectedFieldBehavior::Ignore;
                                break;
                            case UnexpectedFieldBehaviorOptions::Error:
                                nativeOption.UnexpectedFieldBehavior = arrow::json::UnexpectedFieldBehavior::Error;
                                break;
                            case UnexpectedFieldBehaviorOptions::InferType:
                                nativeOption.UnexpectedFieldBehavior = arrow::json::UnexpectedFieldBehavior::InferType;
                                break;
                            default:
                                throw gcnew ArgumentException("UnexpectedFieldBehavior '" + arrowConfiguration->ParseOptions->UnexpectedFieldBehavior.ToString() + "' is not supported");
                            }

                            switch (arrowConfiguration->WriteOptions->Compression)
                            {
                            case CompressionOptions::SNAPPY:
                                nativeOption.Compression = arrow::Compression::type::SNAPPY;
                                break;
                            case CompressionOptions::UNCOMPRESSED:
                                nativeOption.Compression = arrow::Compression::type::UNCOMPRESSED;
                                break;
                            default:
                                throw gcnew ArgumentException("Compression type '" + arrowConfiguration->WriteOptions->Compression.ToString() + "' is not supported");
                            }

                            nativeOption.WriteBatchSize = arrowConfiguration->WriteOptions->WriteBatchSize;

                            return nativeOption;
                        }
                    }
                }
            }
        }
    }
}
