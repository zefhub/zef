#include <iostream>
#include <variant>
#include <optional>


using str = std::string;

constexpr auto print = [](const auto& x)->void { std::cout << x << std::endl; };
constexpr auto printt = [](const auto& v)->void { for (auto& el : v) std::cout << el << ", "; std::cout << std::endl; };




struct Sentinel2 {};

using ZefVariant = std::variant <
	Sentinel2,
	int,
	double,
	str
>;



bool operator== (const Sentinel2& v1, const Sentinel2& v2) { return false; }

std::optional<bool> operator== (const ZefVariant& v1, const ZefVariant& v2) {
	if (v1.index() != v2.index()) return false;
	return (std::visit(
		[&v1, &v2](auto x) {return std::get<decltype(x)>(v1) == std::get<decltype(x)>(v2); },  // once the types agree, check whether the values agree.
		v1
	));
}

//std::optional<bool> operator== (const ZefVariant& v1, const ZefVariant& v2) {
//	return false;
//}

int main() {
	print("done");

	print(*(ZefVariant("asd") == ZefVariant(5)));
	print(*(ZefVariant("asd") == ZefVariant("asd")));
	print(*(ZefVariant("asd") == ZefVariant("asds")));
	//print(ZefVariant("asd") == ZefVariant("asd"));

	auto c = ZefVariant("asd") == ZefVariant(5);

	return 0;
}