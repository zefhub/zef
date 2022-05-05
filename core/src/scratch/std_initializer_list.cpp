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
#include <initializer_list>


struct A {
	A() { x = 42.1; };
	A(std::initializer_list<double> v) { std::cout << "v.size= " << v.size() << "\n"; }
	int x=3;
	double y = 11.2;
	float z = 7.1;
};

int main() {
	auto a = A{ 1,2.45,3,56,1 };

	std::cout << a.x << "  "<< a.y <<  "  done\n\n";
}
