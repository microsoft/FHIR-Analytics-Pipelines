## Parquet library
This folder contains the parquet library built with the [Arrow](https://github.com/apache/arrow) project.

The `cpp` folder contains the native wrapper for parquet code. And the `csharp` folder contains all managed wrapper on top of `cpp` folder and all dependencies.

This library works for both Windows and Linux platforms and will be published as dotnet nuget package for future usage.

### Building native library
The native parquet library has dependencies on Arrow, Parquet and json-cpp libraries. All dependencies are managed with the vcpkg tool.
By using the vcpkg [manifest mode](https://vcpkg.readthedocs.io/en/latest/users/manifests/), the dependencies can be easily installed by integrating with CMake:

First load submodules using command:

```
git submodule update --init --recursive
```

Build command for **linux**:
```bash
cmake -B build -S . -DVCPKG_TARGET_TRIPLET=x64-linux-dynamic -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

Build command for **windows**:
```powershell
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

### Package NuGet
We define custom targets to pack native dependencies to nuget:
```xml
<?xml version="1.0"?>
<Project>
    <ItemGroup Condition=" '$(OS)' == 'WINDOWS_NT'">
        <Content Include="$(MSBuildThisFileDirectory)..\runtimes\windows-x64\*.dll">
            <CopyToOutputDirectory>PreserveNewest</CopyToOutputDirectory>
            <Link>%(FileName)%(Extension)</Link>
        </Content>
    </ItemGroup>
    <ItemGroup Condition=" '$(OS)' == 'UNIX'">
        <Content Include="$(MSBuildThisFileDirectory)..\runtimes\linux-x64\*">
            <CopyToOutputDirectory>PreserveNewest</CopyToOutputDirectory>
            <Link>%(FileName)%(Extension)</Link>
        </Content>
    </ItemGroup>
</Project>

```

### Interfaces
```csharp
var schemaMap = new Dictionary<string, string> { { schemaKey, schemaContent } };
var parquetConverter = ParquetConverter.CreateWithSchemaSet(schemaMap);

using var stream = parquetConverter.ConvertJsonToParquet(schemaKey, data);
...
```