#include "ArrowSchemaManager.h"
#include "InteropUtility.h"

using namespace Linq;

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
                        ArrowSchemaManager::ArrowSchemaManager()
                            : ManagedObject(new std::unordered_map<std::string, std::shared_ptr<arrow::Schema>>())
                        {
                        }

                        ArrowSchemaManager::ArrowSchemaManager(Dictionary<String^, FhirSchemaNode^>^ schemaMap)
                            : ManagedObject(new std::unordered_map<std::string, std::shared_ptr<arrow::Schema>>())
                        {
                            for each (KeyValuePair<String^, FhirSchemaNode^> schemaPair in schemaMap)
                            {
                                AddOrUpdateArrowSchema(schemaPair.Key, schemaPair.Value);
                            }
                        }

                        std::shared_ptr<arrow::Schema> ArrowSchemaManager::GetArrowSchema(String^ resourceType)
                        {
                            std::string resourceTypeStr;
                            MarshalStringToAnsi(resourceType, resourceTypeStr);
                            
                            auto schemaIterator = _instance->find(resourceTypeStr);
                            if (schemaIterator == _instance->end())
                            {
                                throw gcnew ArgumentException("Resource type '" + resourceType + "' is not supported");
                            }

                            return schemaIterator->second;
                        }

                        void ArrowSchemaManager::AddOrUpdateArrowSchema(String^ resourceType, FhirSchemaNode^ schemaNode)
                        {
                            std::string resourceTypeStr;
                            MarshalStringToAnsi(resourceType, resourceTypeStr);

                            (*_instance)[resourceTypeStr] = arrow::schema(GenerateArrowFieldsFromSchemaNode(schemaNode));
                        }

                        std::vector<std::shared_ptr<arrow::Field>> ArrowSchemaManager::GenerateArrowFieldsFromSchemaNode(FhirSchemaNode^ schemaNode)
                        {
                            std::vector<std::shared_ptr<arrow::Field>> arrowFields;

                            for each (KeyValuePair<String^, FhirSchemaNode^> ^ subNodeItem in schemaNode->SubNodes)
                            {
                                auto subNode = subNodeItem->Value;
                                std::string subFieldName;
                                MarshalStringToAnsi(subNode->Name, subFieldName);

                                // Array
                                if (subNode->IsRepeated)
                                {
                                    arrowFields.push_back(GenerateArrayArrowField(subNode, subFieldName));
                                }
                                // Leaf
                                else if (subNode->IsLeaf)
                                {
                                    arrowFields.push_back(GeneratePrimitiveArrowField(subNode, subFieldName));
                                }
                                // Struct
                                else
                                {
                                    arrowFields.push_back(GenerateStructArrowField(subNode, subFieldName));
                                }
                            }

                            return arrowFields;
                        }

                        std::shared_ptr<arrow::Field> ArrowSchemaManager::GenerateStructArrowField(FhirSchemaNode^ schemaNode, std::string fieldName)
                        {
                            return arrow::field(fieldName, arrow::struct_(GenerateArrowFieldsFromSchemaNode(schemaNode)));
                        }

                        std::shared_ptr<arrow::Field> ArrowSchemaManager::GenerateArrayArrowField(FhirSchemaNode^ schemaNode, std::string fieldName)
                        {
                            if (schemaNode->IsLeaf)
                            {
                                return arrow::field(fieldName, arrow::list(GeneratePrimitiveArrowField(schemaNode, _elementNodeName)));
                            }
                            
                            return arrow::field(fieldName, arrow::list(GenerateStructArrowField(schemaNode, _elementNodeName)));
                        }

                        std::shared_ptr<arrow::Field> ArrowSchemaManager::GeneratePrimitiveArrowField(FhirSchemaNode^ schemaNode, std::string fieldName)
                        {
                            // Pleaser refer to "Types" in https://parquet.apache.org/documentation/latest/ for data types of parquet format
                            if (FhirSchemaNodeConstants::IntTypes->Contains(schemaNode->Type))
                            {
                                return arrow::field(fieldName, arrow::int32());
                            }

                            if (FhirSchemaNodeConstants::DecimalTypes->Contains(schemaNode->Type))
                            {
                                return arrow::field(fieldName, arrow::float64());
                            }

                            if (FhirSchemaNodeConstants::BooleanTypes->Contains(schemaNode->Type))
                            {
                                return arrow::field(fieldName, arrow::boolean());
                            }

                            return arrow::field(fieldName, arrow::utf8());
                        }
                    }
                }
            }
        }
    }
}
