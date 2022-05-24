

#include "zefDB.h"
#include <iostream>
#include <stdlib.h>

using namespace zefDB;

int main(void) {
	std::cerr << "About  to initialise butler" << std::endl;
	auto butler = Butler::get_butler();

	std::cerr << "About  to wait for auth" << std::endl;
	butler->wait_for_auth();

	try {
		std::cerr << "About  to zearch" << std::endl;
		auto out = zearch("");
	}
	catch (const std::exception& exc) {
		std::cerr << "Got some kind of exception: " << exc.what() << std::endl;
		throw;
	}

	std::cerr << "About  to get graph" << std::endl;
	Graph g("blog/northwind", MMap::MMAP_STYLE_ANONYMOUS);
}