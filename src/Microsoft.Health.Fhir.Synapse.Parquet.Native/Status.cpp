#include "Status.h"

using namespace Microsoft::Health::Fhir::Synapse::Parquet::Native;

Status::Status(StatusCode code) :
    _code(code)
{
}

Status::Status(StatusCode code, std::string msg) :
    _code(code),
    _msg(std::move(msg))
{
}

std::string Status::CodeAsString() const
{
    const char* codeStr;
    switch (_code) {
    case StatusCode::OK:
        codeStr = "OK";
        break;
    case StatusCode::CreateTableReaderFailed:
        codeStr = "Create table reader failed";
        break;
    case StatusCode::InvalidSchema:
        codeStr = "The input schema is invalid";
        break;
    case StatusCode::ReadToTableFailed:
        codeStr = "Read to table failed";
        break;
    case StatusCode::TableToParquetBufferFailed:
        codeStr = "Table to parquet buffer failed";
        break;
    case StatusCode::UnhandledException:
        codeStr = "Unhandled exception";
        break;
    case StatusCode::UnknownError:
        codeStr = "Unknown error";
        break;
    default:
        codeStr = "Unknown code";
        break;
    }

    return std::string(codeStr);
}

std::string Status::ToString() const
{
    std::string result(CodeAsString());

    if (_msg.empty()) {
        return result;
    }

    result += _codeMessageDelimiter;
    result += _msg;

    return result;
}