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


