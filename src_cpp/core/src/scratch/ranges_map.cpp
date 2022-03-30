#include <iostream>

#include "range/v3/all.hpp"
#include <vector>
#include <string>
#include <unordered_map>


int main() {

	//using namespace ranges;
	using namespace ranges;
	auto my_range = views::iota(42, 1000);

	for (auto el : my_range | views::take(20))
		std::cout << el << ", ";

	std::unordered_map<int, std::string> m = {
		{12, "Luna"},
		{5, "Naomi"},
		{7, "Frik"}
	};

	//view::all(m) | view::for_each([](auto x) {std::cout << " -> " << x << "\n"; });


	auto v = std::vector<int>{ 5,6,7,8,2,15,3,12 };
	std::cout << "\n" << ranges::back(views::all(v)) << "\n\n\n";


	//views::all(m) | view::keys | view::for_each([](auto x) {std::cout << " -> " << "." << "\n"; });
	
	for (auto el : views::all(m) 
		| view::transform([](auto x)->std::tuple<int, std::string> { return { x.first, x.second }; })
		| ranges::to<std::vector>
		) std::cout << "*" << "\n";
	
	std::cout << "\n----- ";
		
	for (auto el : views::all(m) | view::keys )
		std::cout << el << "\n";



	
	auto sorted = views::all(v) | actions::sort([](auto x, auto y) {return y < x; });
	for (auto y : sorted) std::cout << y << ", ";

	std::cout << "\n----- done\n";
	return 0;
}
