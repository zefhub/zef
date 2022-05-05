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
#include "../zefDB_utils.h"





void from_hex(const std::string& uid, unsigned char* target) {
	// target needs to be a preallocated buffer of the size uid.size()/2 bytes. (1 hex character = 2 bytes of information)
	// taken from https://stackoverflow.com/questions/17261798/converting-a-hex-string-to-a-byte-array

	if(uid.size()!=32)
		throw std::invalid_argument("Invalid uid input size to from_hex");

	auto char2int = [](unsigned char input)->int
	{
		if (input >= '0' && input <= '9')
			return input - '0';
		if (input >= 'A' && input <= 'F')
			return input - 'A' + 10;
		if (input >= 'a' && input <= 'f')
			return input - 'a' + 10;
		throw std::invalid_argument("Invalid input string");
	};

	const char* src = uid.c_str();
	// the snippet below assumes src to be a zero terminated sanitized string with
	// an even number of [0-9a-f] characters, and target to be sufficiently large
	while (*src && src[1])
	{
		*(target++) = char2int(*src) * 16 + char2int(src[1]);
		src += 2;
	}	
}



int main() {

	
	unsigned char* target = new unsigned char[32];
	from_hex("dB123681A6be4f09b45c54bb37d8f174", target);
	

	std::cout << zefDB::to_hex(target, 16) << std::endl;
	return 0;
}