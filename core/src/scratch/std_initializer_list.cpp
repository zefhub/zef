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
