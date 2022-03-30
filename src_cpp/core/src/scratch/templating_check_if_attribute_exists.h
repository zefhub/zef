#pragma once
#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest.h>
#include <type_traits>
#include <cstddef>


struct Hello
{
	int helloworld() { return 0; }
	double y;
};

struct Generic {
	double z;
};

// SFINAE test
template <typename T>
class has_y
{
	typedef char one;
	struct two { char x[2]; };
	template <typename C> static one test(decltype(&C::y));
	template <typename C> static two test(...);
public:
	enum { value = sizeof(test<T>(0)) == sizeof(char) };
};


template <typename T>
int function_that_returns_y_if_present(T s) {

	if constexpr (has_y<T>::value) return s.y;
	else return -1;
}



TEST_CASE("check for member") {
	std::cout << has_y<Hello>::value << std::endl;
	std::cout << has_y<Generic>::value << std::endl;
	std::cout << function_that_returns_y_if_present(Hello({ 42 }));
	std::cout << function_that_returns_y_if_present(Generic({ 41 }));
}
