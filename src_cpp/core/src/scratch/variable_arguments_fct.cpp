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
