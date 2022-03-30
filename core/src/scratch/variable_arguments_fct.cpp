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
#include <cstdarg>
using namespace std;
 

double f(int x, double y){
    return x*y+x;
}


void g(auto&& ...my_args){   
    std::cout<< f(my_args...);    //forward all the arguments
};


void g2(int num, ...){   
    va_list arguments;                     // A place to store the list of arguments
    va_start ( arguments, num );  
    //std::cout<< f(my_args...);    //forward all the arguments
};

auto h = [](auto ...my_args){
    std::cout<< f(my_args...);
};





int main() {
    h(6, 10);

	std::cout << "\ndone\n";
	return 0;
}
