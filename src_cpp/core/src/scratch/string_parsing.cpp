#include <iostream>
#include <string>
#include <string_view>
#include <vector>

using str = std::string;
using strv = std::string_view;

constexpr auto print = [](const auto& x)->void { std::cout << x << std::endl; };
constexpr auto printt = [](const auto& v)->void { for (auto& el : v) std::cout << el << ", "; std::cout << std::endl; };








std::vector<str> split(const strv& string_to_split, const str& to_split_on, std::vector<str>&& to_push_onto = {}) {	
	print(string_to_split);
	auto found = string_to_split.find(to_split_on);
	print(found);
	if (found==std::string::npos){
		if(!string_to_split.empty() )
			to_push_onto.emplace_back(string_to_split);
		return to_push_onto;
	}
	to_push_onto.emplace_back(string_to_split.substr(0, found));
	return split(string_to_split.substr(found+1, string_to_split.size()), to_split_on, std::move(to_push_onto));
}


void run_split(str s, str k) {	 
	auto v = split(s, k);
	print(v.size());
	for(auto el : v)
		print(el);
}


void run_split_and_convert(){
	for(auto s : split("2.1.5", ".")){
		int x = stoi(s);
		print(x);
	}

}


int main() {
	run_split("Dit is maar baie zef, ne? Wat sê jy? ", " ");
	
	print("********");
	run_split_and_convert();
	print("done");
	return 0;
}
