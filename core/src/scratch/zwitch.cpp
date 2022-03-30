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
















#include <iostream>
#include <string>
#include <unordered_map>
#include <variant>

using str = std::string;
constexpr auto print = [](auto&& x)->void { std::cout << x << std::endl; };



using str = std::string;
using umap = std::unordered_map<str, str>;


struct Zwitch {
	umap display_zefhub_communication_msgs(bool val) const {};
	umap allow_dynamic_entity_type_definitions(bool val) const {};
	umap allow_dynamic_relation_type_definitions(bool val) const {};
	umap allow_dynamic_enum_type_definitions(bool val) const {};
};

constexpr Zwitch zwitch;

void f() {
	zwitch.display_zefhub_communication_msgs(false);
	zwitch.allow_dynamic_entity_type_definitions(true);
	
}







using any = std::variant<
	int,
	double,
	str,
	bool
>;
using rec = std::unordered_map<str, any>;

rec operator| (const rec& r1, const rec& r2) {
	auto res = r1;
	for (const auto& p : r2)
		res[p.first] = p.second;
	return res;
}


void ff() {
	rec Luna = { {"age", 13}, {"weight", 6.1} };
	rec Naomi = { {"age", 11}, {"color", str("brown")} };

	//auto older_Luna = Luna | age(14);
	auto older_Luna = Luna | rec{ {"age", 14}, {"name", str("Luna")} } | rec{};
	print(std::get<int>(older_Luna["age"]));
	print(std::get<str>(older_Luna["name"]));
	print(std::get<str>(Naomi["color"]));
	
}



int main() {
	ff();
	return 0;
}



