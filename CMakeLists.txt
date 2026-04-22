# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.12...3.31)

project ("sql")

enable_testing()

# Include sub-projects.
add_subdirectory (core)
add_subdirectory ("sql")
add_subdirectory (tests)
