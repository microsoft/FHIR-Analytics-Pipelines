## Build native parquet library
### Install arrow and vcpkg
```sh
apt-get install git g++ gcc libarrow-dev=8.0.0-1 libparquet-dev=8.0.0-1 -y
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