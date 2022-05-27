

#include "zefDB.h"
#include <iostream>
#include <stdlib.h>

using namespace zefDB;

int main(void) {
	_putenv("LIBZEF_AUTH_HTML_PATH=C:/Users/danie/zef/core/");

	std::cerr << "About  to initialise butler" << std::endl;
	auto butler = Butler::get_butler();

	butler->user_login();

	std::cerr << "About  to wait for auth" << std::endl;
	butler->wait_for_auth();

	std::cerr << "About  to zearch" << std::endl;
	auto out = zearch("");

	//Butler::stop_butler();

	std::cerr << "About  to get graph" << std::endl;
	Graph g("blog/northwind", MMap::MMAP_STYLE_ANONYMOUS);

	//Graph g2(false);
	//auto z = instantiate(ET("Danny"), g2);

	//sync(g2);

}