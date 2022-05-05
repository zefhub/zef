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
#include <variant>


struct EntityType { int x; };
struct AtomicEntityType { int x; };
struct RelationType { int x; };

struct ET_ {
	static constexpr auto CNCMachine = EntityType{ 5 };
	static constexpr auto SalesOrder = EntityType{ 51 };
	static constexpr auto Material = EntityType{ 34 };
};
auto ET = ET_{};


struct AET_ {
	static constexpr auto Float = AtomicEntityType{ 5 };
	static constexpr auto String = AtomicEntityType{ 51 };
};
auto AET = AET_{};


struct RT_ {
	static constexpr auto UsedBy = RelationType{ 5 };
	static constexpr auto Ordered = EntityType{ 51 };	
	static constexpr auto SubscribedTo = EntityType{ 517 };
};
auto RT = RT_{};


struct Any {};
struct Sentinel {};

using SomeZefType = std::variant<
	EntityType,
	AtomicEntityType,
	RelationType,
	Any,
	Sentinel
>;


struct Zuple {
	const SomeZefType left;
	const SomeZefType center;
	const SomeZefType right;

	static const Sentinel nothing;
};
constexpr Any any;
constexpr Sentinel sentinel;


Zuple operator> (EntityType e, RelationType r) { return Zuple{ SomeZefType{e}, SomeZefType{r}, SomeZefType{sentinel} }; }
Zuple operator> (AtomicEntityType ae, RelationType r) { return Zuple{ SomeZefType{ae}, SomeZefType{r}, SomeZefType{sentinel} }; }
Zuple operator> (RelationType r1, RelationType r) { return Zuple{ SomeZefType{r1}, SomeZefType{r}, SomeZefType{sentinel} }; }

Zuple operator> (Zuple zu, EntityType e) { return Zuple{ SomeZefType{zu.left}, SomeZefType{zu.center}, SomeZefType{e} }; }
Zuple operator> (Zuple zu, AtomicEntityType ae) { return Zuple{ SomeZefType{zu.left}, SomeZefType{zu.center}, SomeZefType{ae} }; }
Zuple operator> (Zuple zu, RelationType r2) { return Zuple{ SomeZefType{zu.left}, SomeZefType{zu.center}, SomeZefType{r2} }; }

Zuple operator> (Any a, RelationType r) { return Zuple{ SomeZefType{a}, SomeZefType{r}, SomeZefType{sentinel} }; }
Zuple operator> (EntityType e, Any a) { return Zuple{ SomeZefType{e}, SomeZefType{a}, SomeZefType{sentinel} }; }
Zuple operator> (Any a1, Any a2) { return Zuple{ SomeZefType{a1}, SomeZefType{a2}, SomeZefType{sentinel} }; }

// what should ET.CNCMachine > any > ET.SalesOrder   mean? Does 'any' have to be an RT? Contextually, yes!

Zuple operator> (Zuple zu, Any a) { return Zuple{ SomeZefType{zu.left}, SomeZefType{zu.center}, SomeZefType{a} }; }

struct Delegate{
	int operator[] (const Zuple& zu) const  { return 42; }
};
constexpr Delegate delegate;





int main() {

	auto x1 = ET.CNCMachine > RT.UsedBy;  // incomplete zuple
	auto x2 = ET.CNCMachine > RT.UsedBy > ET.SalesOrder;
	auto x3 = ET.CNCMachine > RT.UsedBy > RT.SubscribedTo;
	auto x4 = any > RT.UsedBy > RT.SubscribedTo;
	auto x5 = any > RT.UsedBy > any;
	auto x6 = any > RT.UsedBy > AET.String;

	std::cout << "done\n";
	return 0;
}
