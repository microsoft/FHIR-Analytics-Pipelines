#pragma once
#include <string>

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
                        enum class StatusCode : char {
                            OK = 0,
                            CreateTableReaderFailed = 1,
                            InvalidSchema = 2,
                            ReadToTableFailed = 3,
                            TableToParquetBufferFailed = 4,
                            UnhandledException = 5,
                            UnknownError = 6,
                        };

                        class Status
                        {
                        public:
                            /// <summary>
                            /// Create a status with StatusCode.
                            /// </summary>
                            /// <param name="code">The StatusCode.</param>
                            Status(StatusCode code);

                            /// <summary>
                            /// Create a status with StatusCode and message specified.
                            /// </summary>
                            /// <param name="code">The StatusCode.</param>
                            /// <param name="msg">The message with the StatusCode.</param>
                            Status(StatusCode code, std::string msg);

                            /// \brief Return True if the status is OK, otherwise return False
                            bool IsOk() const { return _code == StatusCode::OK; }

                            /// \brief Return the StatusCode.
                            StatusCode Code() const { return _code; }

                            /// \brief Return the specific message.
                            const std::string& Message() const { return _msg; }

                            /// \brief Return a string representation of this status suitable for printing.
                            /// The string "OK" is returned for success.
                            std::string ToString() const;

                            /// \brief Return a string representation of the status code, without the message
                            /// text or POSIX code information.
                            std::string CodeAsString() const;

                        private:
                            StatusCode _code;
                            std::string _msg;
                            const std::string _codeMessageDelimiter = ": ";
                        };
                    }
                }
            }
        }
    }
}