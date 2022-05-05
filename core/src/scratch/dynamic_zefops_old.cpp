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
#include <vector>



template <typename T>
auto vec(const std::vector<T>& v, T new_el)->std::vector<T> {
	std::vector<T> new_vec = v;
	new_vec.push_back(new_el);
	return new_vec;
};


template <typename T>
auto vec(T new_el, const std::vector<T>& v)->std::vector<T> {
	std::vector<T> new_vec;
	new_vec.reserve(v.size() + 1);
	new_vec.push_back(new_el);
	for(auto el : v)
		new_vec.push_back(el);	
	return new_vec;
};

template <typename T>
auto vec(const std::vector<T>& v1, const std::vector<T>& v2)->std::vector<T> {
	std::vector<T> new_vec;
	new_vec.reserve(v1.size() + v2.size());	
	for(auto el : v1)
		new_vec.push_back(el);	
	for(auto el : v2)
		new_vec.push_back(el);	
	return new_vec;
};



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
	static constexpr auto _undefined = RelationType{ 5 };
	static constexpr auto UsedBy = RelationType{ 5 };
	static constexpr auto Ordered = RelationType{ 51 };
	static constexpr auto SubscribedTo = RelationType{ 517 };
};
auto RT = RT_{};










struct EZefRef { void* p = nullptr; };
struct ZefRef {	EZefRef z1; EZefRef z2; };
struct ZefRefs {};


struct Greater{
	const RelationType rt = RT._undefined;
};
struct Lesser{
	const RelationType rt = RT._undefined;
};
struct GreaterGreater{
	const RelationType rt = RT._undefined;
};
struct LesserLesser{
	const RelationType rt = RT._undefined;
};


// traversals on the graph can be broken down into the steps: the comnputational atomic operations are dual
// to the knowledge graph structure: computational nodes correspond to edge traversals 
using SomeOp = std::variant<
	Greater,
	Lesser,
	GreaterGreater,
	LesserLesser
>;

using InitialState = std::variant<
	ZefRef,
	ZefRefs
>;



// Lazy ZefRef
struct LZefRef {
	const InitialState initial_state;
	std::vector<SomeOp> ops;
};

// Lazy ZefRefs
struct LZefRefs {
	const InitialState initial_state;
	std::vector<SomeOp> ops;
};






struct PureZefOp2 {
	std::vector<SomeOp> ops;
};



LZefRef operator>> (ZefRef z, RelationType rt) {
	return LZefRef{ z, {SomeOp{GreaterGreater{rt}}} };
}


LZefRef operator>> (LZefRef z, RelationType rt) {
	return LZefRef{ z.initial_state, vec(z.ops, SomeOp{GreaterGreater{rt}}) }; //apend new op
}











// DeltaL is the lifting number for this operator
template <int DeltaL, typename src_type, typename trg_type>
struct PureZefOp {
	std::vector<SomeOp> ops;
};



PureZefOp<0, ZefRef, ZefRef> operator>> (RelationType rt1, RelationType rt2) {
	return PureZefOp<0, ZefRef, ZefRef>{ { SomeOp{GreaterGreater{rt1}},  SomeOp{GreaterGreater{rt2}} } };
}

template<int DeltaL, typename src_type>
PureZefOp<DeltaL, src_type, ZefRef> operator>> (PureZefOp<DeltaL, src_type, ZefRef> pzo, RelationType rt) {
	return PureZefOp<DeltaL, src_type, ZefRef>{ vec(pzo.ops, SomeOp{GreaterGreater{rt}}) };
}

template<int DeltaL, typename trg_type>
PureZefOp<DeltaL, ZefRef, trg_type> operator>> (RelationType rt, PureZefOp<DeltaL, ZefRef, trg_type> pzo) {
	return PureZefOp<DeltaL, ZefRef, trg_type>{ vec(SomeOp{ GreaterGreater{rt} }, pzo.ops) };
}


template <typename trg_type>
trg_type operator>> (LZefRef z, PureZefOp<0, ZefRef, trg_type> pzo) {
	return LZefRef{ z.initial_state, vec(z.ops, pzo.ops) };
}

//LZefRefs operator>> (LZefRef z, PureZefOp<1> pzo) {
//	return LZefRefs{ z.initial_state, vec(z.ops, pzo.ops) };
//}
//
//LZefRefs operator>> (LZefRefs z, PureZefOp<0> pzo) {
//	return LZefRefs{ z.initial_state, vec(z.ops, pzo.ops) };
//}


template<int DeltaL1, typename src_type1, typename contracted_type, int DeltaL2, typename trg_type2>
PureZefOp<DeltaL1 + DeltaL2, src_type1, trg_type2> operator>> (
	PureZefOp<DeltaL1, src_type1, contracted_type> pzo1,
	PureZefOp<DeltaL2, contracted_type, trg_type2> pzo2) {
	return PureZefOp<DeltaL1 + DeltaL2, src_type1, trg_type2>{ vec(pzo1.ops, pzo2.ops) };
}


//LZefRef operator>> (ZefRef z, PureZefOp<0> pzo) {
//	return LZefRef{ z, pzo.ops };
//}
//
//




struct Ins{};
constexpr Ins ins;

struct Filter{};
constexpr Filter filter;





template <int DeltaL>
double f(double x) {	
	return DeltaL*x;
}









// normal fcts are not constexpr by default
constexpr int times_two(const int m) {
	return 2 * m;
}


// lambda fcts are constexpr by default
auto times_three = [](int m)->int { return 3 * m; };



int ff(const int& m) {
	return m;
}



int main() {
	constexpr int d = 3;


	std::cout << f<times_three(d)>(5.1)<<"\n";

	ZefRef z;

	auto A = RT.SubscribedTo >> RT.SubscribedTo >> RT.UsedBy;
	auto B = RT.SubscribedTo >> RT.UsedBy;
	auto C = A >> B;
	auto my_lazy_z = z >> RT.SubscribedTo >> RT.UsedBy;
	//auto my_lazy_z2 = z >> C;
	





	std::cout << "...done\n";
	return 0;
}
