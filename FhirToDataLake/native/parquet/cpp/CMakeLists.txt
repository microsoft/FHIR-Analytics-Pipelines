cmake_minimum_required(VERSION 3.2.0)
set (CMAKE_TOOLCHAIN_FILE "${CMAKE_CURRENT_SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake"
  CACHE STRING "Vcpkg toolchain file")
project(ArrowParquetNative)

include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")
set(VCPKG_BUILD_TYPE "release")

# This ensures that things like gnu++11 get passed correctly
if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 11)
endif()

# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Look for installed packages the system
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)
find_package(jsoncpp CONFIG REQUIRED)

# Fetch gtest
include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG release-1.12.1
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_subdirectory(src)

enable_testing ()
add_subdirectory(test)
