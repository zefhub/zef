#include <iostream>
//#include <string>
#include <string.h>
#include <variant>

using str = std::string;

struct A{};

//int operator^ (A a, std::string s) {
int operator^ (A a, const char* s) {

	auto n = strlen(s);
	std::cout << "|" << n << "|\n";
	std::cout << "|" << s << "|\n\n";
}




struct Sentinel {};

using ZefVariant = std::variant <
	Sentinel,
	int,
	double,
	str,
	bool
>;


// overload operator that we can directly compare variants: bcheck type and value
bool operator== (const ZefVariant& v1, const ZefVariant& v2) {
	if (v1.index() != v2.index()) return false;	
	return std::visit(
		[&v1, &v2](auto x) {return std::get<decltype(x)>(v1) == std::get<decltype(x)>(v2); },  // once the types agree, check whether the values agree.
		v1
	);
}

bool operator!= (const ZefVariant& v1, const ZefVariant& v2) { return !(v1 == v2); }


int main() {




	ZefVariant a1 = 45.6;
	ZefVariant a2 = 45.6;

	std::cout << (a1 == a2) << "\n\n";
	//std::cout << (std::get<int>(a1) == std::get<int>(a2)) << "\n\n";





	auto a = A();
	a ^ "hello zef world!!";


	std::cout << "done\n";
	return 0;
}
