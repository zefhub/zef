# Copyright 2022 Synchronous Technologies Pte Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.13)

# Use this to distinguish the full project from the individual subdirectories.
project(TestWindows CXX)

add_executable(test_windows test_windows.cpp)
set_target_properties(test_windows PROPERTIES
	CXX_STANDARD 17
)

target_link_libraries(test_windows PUBLIC zef::zef)

message(STATUS "In the test_windows file")