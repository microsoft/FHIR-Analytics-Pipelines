#pragma once
#include "ArrowSchemaManager.h"

using namespace Microsoft::Health::Fhir::Synapse::SchemaManagement::Parquet;
using namespace Microsoft::Health::Fhir::Synapse::Common::Configurations::Arrow;

using namespace System;

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
						public ref class ParquetConverterWrapper : public ManagedObject<Native::ParquetConverter>
						{
						public:
							/// <summary>
							/// Initialize a ParquetConverterWrapper with default arrow configuration and generate all arrow schemas.
							/// </summary>
							/// <param name="schemaMap">A map of resource type to FHIR schema node, used to generate arrow schemas.</param>
							ParquetConverterWrapper(Dictionary<String^, FhirParquetSchemaNode^>^ schemaMap);
							
							/// <summary>
							/// Initialize a ParquetConverterWrapper with specified arrow configuration and generate all arrow schemas.
							/// </summary>
							/// <param name="schemaMap">A map of resource type to FHIR schema node, used to generate arrow schemas.</param>
							/// <param name="arrowConfiguration">The specified arrow configuration.</param>
							ParquetConverterWrapper(Dictionary<String^, FhirParquetSchemaNode^>^ schemaMap, ArrowConfiguration^ arrowConfiguration);

							/// <summary>
							/// Convert a stream of specified resource type to parquet stream.
							/// </summary>
							/// <param name="resourceType">The resource type to get the arrow schema used for conversion.</param>
							/// <param name="stream">the input stream.</param>
							/// <returns>The converted stream.</returns>
							IO::MemoryStream^ ConvertToParquetStream(String^ resourceType, IO::MemoryStream^ stream);

						private:
							/// <summary>
							/// Create native ArrowParquetOption from ArrowConfiguration.
							/// </summary>
							/// <param name="arrowConfiguration">The input ArrowConfiguration.</param>
							/// <returns>The native ArrowParquetOption.</returns>
							Native::ArrowParquetOption CreateNativeArrowParquetOption(ArrowConfiguration^ arrowConfiguration);

							ArrowSchemaManager^ _arrowSchemaManager;
						};
					}
				}
			}
		}
	}
}