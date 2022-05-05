// Copyright 2022 Synchronous Technologies Pte Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <iostream>
#include <array>
#include <variant>
#include <vector>
#include <string>


template<typename T>
T add(const T& arg)
{
  return arg;
}

template<typename T, typename... ARGS>
T add(const T& arg, const ARGS&... args)
{
  return arg + add(args...);
}

void test1(){
	std::cout<< add(1.21, 2u, 3u, 7.34) <<"\n";
}











template<typename... Ts>
auto add2(const Ts&... args)
{
  return (args + ...);
}


void test2(){
	std::cout<< add2(1.21, 2u, 3u, 7.34) <<"\n";
}







int main() {
	test1();
	return 0;
}
