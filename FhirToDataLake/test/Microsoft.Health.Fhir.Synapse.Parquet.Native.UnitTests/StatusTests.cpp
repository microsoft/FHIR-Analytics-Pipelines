#include <iostream>

#include "CppUnitTest.h"
#include <arrow/api.h>
#include "../../src/Microsoft.Health.Fhir.Synapse.Parquet.Native/Status.h"

using namespace Microsoft::VisualStudio::CppUnitTestFramework;
using namespace Microsoft::Health::Fhir::Synapse::Parquet::Native;

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
					namespace Native
					{
						namespace UnitTest
						{
							TEST_CLASS(status_tests)
							{
							public:
								const std::string delimiter = ": ";
								TEST_METHOD(create_default_ok_status_test)
								{
									const Status ok_status(StatusCode::OK);
									Assert::IsTrue(ok_status.IsOk());
									Assert::AreEqual(std::string("OK"), ok_status.ToString());
									Assert::AreEqual(std::string("OK"), ok_status.CodeAsString());
									Assert::IsTrue(ok_status.Code() == StatusCode::OK);
									Assert::IsTrue(ok_status.Message().empty());
								}

								TEST_METHOD(create_ok_status_with_message_test)
								{
									const std::string ok_message("detail message for ok status");
									const Status ok_status(StatusCode::OK, ok_message);
									Assert::IsTrue(ok_status.IsOk());
									Assert::AreEqual(std::string(ok_status.CodeAsString() + delimiter + ok_message), ok_status.ToString());
									Assert::AreEqual(std::string("OK"), ok_status.CodeAsString());
									Assert::IsTrue(ok_status.Code() == StatusCode::OK);
									Assert::AreEqual(ok_message, ok_status.Message());
								}

								TEST_METHOD(create_fail_status_test)
								{
									const std::string error_message("detail error message of creating table reader failed");
									const Status status(StatusCode::CreateTableReaderFailed, error_message);
									Assert::IsFalse(status.IsOk());
									Assert::AreEqual(std::string(status.CodeAsString() + delimiter + error_message), status.ToString());
									Assert::AreEqual(std::string("Create table reader failed"), status.CodeAsString());
									Assert::IsTrue(status.Code() == StatusCode::CreateTableReaderFailed);
									Assert::AreEqual(error_message, status.Message());
								}

								TEST_METHOD(create_fail_status_without_message_test)
								{
									const Status status(StatusCode::ReadToTableFailed, "");
									Assert::IsFalse(status.IsOk());
									Assert::AreEqual(std::string(status.CodeAsString()), status.ToString());
									Assert::AreEqual(std::string("Read to table failed"), status.CodeAsString());
									Assert::IsTrue(status.Code() == StatusCode::ReadToTableFailed);
									Assert::IsTrue(status.Message().empty());
								}

								TEST_METHOD(code_as_string_test)
								{
									constexpr auto default_code = StatusCode();
									Assert::IsTrue(StatusCode::OK == default_code);

									Status ok_status(StatusCode::OK, "");
									Assert::AreEqual(std::string("OK"), ok_status.CodeAsString());

									Status create_table_reader_failed_status(StatusCode::CreateTableReaderFailed, "");
									Assert::AreEqual(std::string("Create table reader failed"), create_table_reader_failed_status.CodeAsString());

									Status invalid_schema_status(StatusCode::InvalidSchema, "");
									Assert::AreEqual(std::string("The input schema is invalid"), invalid_schema_status.CodeAsString());

									Status read_to_table_failed_status(StatusCode::ReadToTableFailed, "");
									Assert::AreEqual(std::string("Read to table failed"), read_to_table_failed_status.CodeAsString());

									Status table_to_parquet_buffer_failed_status(StatusCode::TableToParquetBufferFailed, "");
									Assert::AreEqual(std::string("Table to parquet buffer failed"), table_to_parquet_buffer_failed_status.CodeAsString());

									Status unhandle_exception_status(StatusCode::UnhandledException, "");
									Assert::AreEqual(std::string("Unhandled exception"), unhandle_exception_status.CodeAsString());

									Status unknown_error_status(StatusCode::UnknownError, "");
									Assert::AreEqual(std::string("Unknown error"), unknown_error_status.CodeAsString());

									constexpr auto invalid_code = static_cast<StatusCode>(7);
									Status unknown_status(invalid_code, "");
									Assert::AreEqual(std::string("Unknown code"), unknown_status.CodeAsString());
								}
							};
						}
					}
				}
			}
		}
	}
}