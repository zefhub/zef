// Copyright 2022 Synchronous Technologies Pte Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.



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