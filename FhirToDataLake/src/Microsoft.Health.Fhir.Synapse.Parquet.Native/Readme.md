## Build native parquet library
### Install arrow and vcpkg
```sh
apt-get install git g++ gcc libarrow-dev libparquet-dev -y
apt-get install curl zip unzip tar -y
```

### Build and test using CMake
```sh
cmake -B build -S .
cmake --build build
cd build
make test
```

### Install nuget
```sh

```