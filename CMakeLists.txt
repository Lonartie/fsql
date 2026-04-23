# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.12...3.31)

project ("fsql")

enable_testing()

# Include sub-projects.
add_subdirectory (fsqllib)
add_subdirectory (fsql)
add_subdirectory (fsqltests)
