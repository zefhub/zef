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
#include <string>
#include <random>

auto print = [](auto x) {
	std::cout << x << std::endl;
};

void mod_and_shift() {
	
	int m = 234525516;
	print(m);
	
	int k = m % 16;
	print(k);
	print(m - k);


}

using enum_indx = unsigned int;

void generate_random_num() {
	static std::random_device rd;  //Will be used to obtain a seed for the random number engine
	static std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	static std::uniform_int_distribution<enum_indx> dis(3, 10);
	

	for(int k=0; k<100; k++)
		print(dis(gen));
}



int main() {
	mod_and_shift();
	generate_random_num();
	return 0;
}


