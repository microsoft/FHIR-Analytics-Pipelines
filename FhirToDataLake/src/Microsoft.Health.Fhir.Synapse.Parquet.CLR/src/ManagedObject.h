#pragma once
#pragma managed
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
                        template<class T>
                        public ref class ManagedObject
                        {
                        protected:
                            T* _instance;

                        public:
                            ManagedObject(T* instance)
                                : _instance(instance)
                            {
                            }

                            // Reference: https://docs.microsoft.com/en-us/previous-versions/visualstudio/visual-studio-2010/ms177197(v=vs.100)?redirectedfrom=MSDN
                            virtual ~ManagedObject()
                            {
                                this->!ManagedObject();
                            }

                            !ManagedObject()
                            {
                                if (_instance != nullptr)
                                {
                                    delete _instance;
                                    _instance = nullptr;
                                }
                            }

                            T* GetInstance()
                            {
                                return _instance;
                            }
                        };
                    }
                }
            }
        }
    }
}