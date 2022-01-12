#pragma once
// FieldList in arrow lib need to be compiled with unmanaged 
#pragma unmanaged
#include <arrow/api.h>

#pragma managed
#include "ManagedObject.h"

using namespace System;
using namespace Collections::Generic;
using namespace Microsoft::Health::Fhir::Synapse::SchemaManagement::Parquet;

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
						static const std::string _elementNodeName = "element";

						public ref class ArrowSchemaManager : public ManagedObject<std::unordered_map<std::string, std::shared_ptr<arrow::Schema>>>
						{
						public:
							/// <summary>
							/// Initialize an ArrowSchemaManager with empty schema.
							/// </summary>
							ArrowSchemaManager();

							/// <summary>
							/// Initialize an ArrowSchemaManager with specified FHIR schema map.
							/// </summary>
							/// <param name="schemaMap">An map of resource type string to FHIR schema node.</param>
							ArrowSchemaManager(Dictionary<String^, FhirParquetSchemaNode^>^ schemaMap);

							/// <summary>
							/// Get the arrow schema of the specified resource type. An exception will be thrown if the schema is not found.
							/// </summary>
							/// <param name="resourceType">The specified resource type.</param>
							/// <returns>The arrow schema.</returns>
							std::shared_ptr<arrow::Schema> GetArrowSchema(String^ resourceType);

							/// <summary>
							/// Generate arrow schema from FHIR schema node and add it to ArrowSchemaManager. If it already exists, update it.
							/// </summary>
							/// <param name="resourceType">The resource type.</param>
							/// <param name="schemaNode">The FHIR schema node.</param>
							void AddOrUpdateArrowSchema(String^ resourceType, FhirParquetSchemaNode^ schemaNode);

						private:

							/// <summary>
							/// Generate vector of arrow fields from given schema node.
							/// This method is designed to generate fields to initialize arrow::schema(), which takes vector of arrow fields as input.
							/// </summary>
							/// <param name="schemaNode">FhirParquetSchemaNode that we need to generate arrow fields from its subNodes.</param>
							/// <returns>Vector of arrow field.</returns>
							std::vector<std::shared_ptr<arrow::Field>> GenerateArrowFieldsFromSchemaNode(FhirParquetSchemaNode^ schemaNode);

							/// <summary>
							/// Generate struct arrow field from struct FHIR schema node.
							/// </summary>
							/// <param name="schemaNode">Struct FhirParquetSchemaNode.</param>
							/// <param name="fieldName">Field name string.</param>
							/// <returns>Generated struct arrow field.</returns>
							std::shared_ptr<arrow::Field> GenerateStructArrowField(FhirParquetSchemaNode^ schemaNode, std::string fieldName);

							/// <summary>
							/// Generate array arrow field from struct FHIR schema node.
							/// </summary>
							/// <param name="schemaNode">Array FhirParquetSchemaNode.</param>
							/// <param name="fieldName">Field name string.</param>
							/// <returns>Generated array arrow field.</returns>
							std::shared_ptr<arrow::Field> GenerateArrayArrowField(FhirParquetSchemaNode^ schemaNode, std::string fieldName);

							/// <summary>
							/// Generate primitive arrow field from struct FHIR schema node.
							/// </summary>
							/// <param name="schemaNode">Primitive FhirParquetSchemaNode.</param>
							/// <param name="fieldName">Field name string.</param>
							/// <returns>Generated primitive arrow field.</returns>
							std::shared_ptr<arrow::Field> GeneratePrimitiveArrowField(FhirParquetSchemaNode^ schemaNode, std::string fieldName);
						};
					}
				}
			}
		}
	}
}