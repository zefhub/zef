#include <iostream>
#include <array>
#include <variant>
#include <vector>
#include <string>


template<typename T>
T add(const T& arg)
{
  return arg;
}

template<typename T, typename... ARGS>
T add(const T& arg, const ARGS&... args)
{
  return arg + add(args...);
}

void test1(){
	std::cout<< add(1.21, 2u, 3u, 7.34) <<"\n";
}











template<typename... Ts>
auto add2(const Ts&... args)
{
  return (args + ...);
}


void test2(){
	std::cout<< add2(1.21, 2u, 3u, 7.34) <<"\n";
}







int main() {
	test1();
	return 0;
}
