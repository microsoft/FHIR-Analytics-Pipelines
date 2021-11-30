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
                    namespace CLR
                    {
                        // Reference: https://docs.microsoft.com/en-us/cpp/dotnet/how-to-convert-system-string-to-standard-string?view=msvc-160
                        inline void MarshalStringToAnsi(System::String^ s, std::string& os)
                        {
                            const auto chars = static_cast<const char*>((System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(s)).ToPointer());
                            os = chars;
                            System::Runtime::InteropServices::Marshal::FreeHGlobal(System::IntPtr((void*)chars));
                        }
                    }
                }
            }
        }
    }
}