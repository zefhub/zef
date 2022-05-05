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
#include <array>
#include <variant>
#include <vector>
#include <string>





struct Sentinel{
	size_t size(){return 0;}
};

struct RT {
	int m;
	RT(int m_ = 0) : m(m_) {};
};

struct BT {
	int m=0;
};


struct AnyVal{
	std::variant<
		double,
		int,
		bool,
		std::string,
		BT,
		RT> val;
};





template <typename ValType, int ListOrder>
struct ZefTensor{};




// ------- use variadic templates: see  https://www.bfilipek.com/2018/09/visit-variants.html--------------------------
// Is C++ really making it that difficult to do a bit of pattern matching below? 
template<class... Ts> struct overload : Ts... { using Ts::operator()...; };
template<class... Ts> overload(Ts...)->overload<Ts...>;




//                                      _                 ___        _                                                     
//                         ___  _ __ __| | ___ _ __      / _ \      | |_ ___ _ __  ___  ___  _ __ ___                      
//    _____ _____ _____   / _ \| '__/ _` |/ _ \ '__|    | | | |     | __/ _ \ '_ \/ __|/ _ \| '__/ __|   _____ _____ _____ 
//   |_____|_____|_____| | (_) | | | (_| |  __/ |       | |_| |     | ||  __/ | | \__ \ (_) | |  \__ \  |_____|_____|_____|
//                        \___/|_|  \__,_|\___|_|        \___/       \__\___|_| |_|___/\___/|_|  |___/                     
//                                                                                                           

template <typename ValType>
struct ZefTensor<ValType, 0>{
	ValType val;
};









//                                      _                _       _                                                     
//                         ___  _ __ __| | ___ _ __     / |     | |_ ___ _ __  ___  ___  _ __ ___                      
//    _____ _____ _____   / _ \| '__/ _` |/ _ \ '__|    | |     | __/ _ \ '_ \/ __|/ _ \| '__/ __|   _____ _____ _____ 
//   |_____|_____|_____| | (_) | | | (_| |  __/ |       | |     | ||  __/ | | \__ \ (_) | |  \__ \  |_____|_____|_____|
//                        \___/|_|  \__,_|\___|_|       |_|      \__\___|_| |_|___/\___/|_|  |___/                     
//                                                                                                                   


// partial template specialization
template <typename ValType>
struct ZefTensor<ValType, 1>{
	struct Iterator;
	//struct cIterator;

	using var = std::variant<
		std::array<ValType, 0>,   // SSO
		std::array<ValType, 1>,
		std::array<ValType, 2>,
		std::array<ValType, 3>,
		std::vector<ValType>
	>;
	var val;

	ZefTensor<ValType, 1>(var init_val): val(init_val) {}

	ZefTensor<ValType, 1>(int init_size){ 
		if     (init_size == 0) val = std::array<ValType, 0>(); 
		else if(init_size == 1) val = std::array<ValType, 1>(); 
		else if(init_size == 2) val = std::array<ValType, 2>(); 
		else if(init_size == 3) val = std::array<ValType, 3>(); 
		else                    val = std::vector<ValType>(init_size);
	}  // ctor only specifying the size

	ValType& operator[] (size_t m) { return *std::visit([m](auto& v)->ValType* { return &v[m]; }, val );}
	size_t size() {return std::visit([](auto& x){return x.size();}, val);}
	Iterator begin();
	Iterator end();
};





template <typename ValType>
struct ZefTensor<ValType, 1>::Iterator {
	// we need to specify these: pre-C++17 this was done by inheriting from std::Iterator
	using value_type = ValType;
	using reference = ValType&;
	using pointer = ValType*;
	using iterator_category = std::random_access_iterator_tag;
	using difference_type = ptrdiff_t;

	ValType* ptr_to_current_el = nullptr;		
	Iterator& operator++() { ++ptr_to_current_el; return *this; }		// pre-increment op: this one is used mostly
	Iterator operator++(int) { return Iterator{ ptr_to_current_el++ }; }		// post incremenet
	reference operator*() { return *ptr_to_current_el; }
	std::add_const_t<reference> operator*() const { return *ptr_to_current_el; }		
	value_type operator[](const difference_type& n) const { return *(ptr_to_current_el + n); }
	bool operator!=(const Iterator& other) const { return ptr_to_current_el != other.ptr_to_current_el; }
	bool operator==(const Iterator& other) const { return ptr_to_current_el == other.ptr_to_current_el; }
};


template <typename ValType>
typename ZefTensor<ValType, 1>::Iterator ZefTensor<ValType, 1>::begin(){
	return ZefTensor<ValType, 1>::Iterator{
	std::visit([](auto& v){ return &(*v.begin()); }, val )};
}


template <typename ValType>
typename ZefTensor<ValType, 1>::Iterator ZefTensor<ValType, 1>::end(){
	return ZefTensor<ValType, 1>::Iterator{
	std::visit([](auto& v){ return &(*v.end()); }, val )};
}





// we can specialize further if we want any specific type to be handled separately: e.g. for allocating a list of string contiguously together somewhere.
template <>
struct ZefTensor<std::string, 1>{
	std::variant<
		Sentinel,		
		std::vector<std::string>
	> val = Sentinel{};
};







//                                      _                ____        _                                                     
//                         ___  _ __ __| | ___ _ __     |___ \      | |_ ___ _ __  ___  ___  _ __ ___                      
//    _____ _____ _____   / _ \| '__/ _` |/ _ \ '__|      __) |     | __/ _ \ '_ \/ __|/ _ \| '__/ __|   _____ _____ _____ 
//   |_____|_____|_____| | (_) | | | (_| |  __/ |        / __/      | ||  __/ | | \__ \ (_) | |  \__ \  |_____|_____|_____|
//                        \___/|_|  \__,_|\___|_|       |_____|      \__\___|_| |_|___/\___/|_|  |___/                     
//                                                                                                                      

// partial template specialization
template <typename ValType>
struct ZefTensor<ValType, 2>{
	std::variant<
		Sentinel,				  // automatically could serve as an options monad
		std::array<ValType, 0>,   // SSO
		std::array<ValType, 1>,
		std::array<ValType, 2>,
		std::array<ValType, 3>,
		std::vector<ValType>
	> val = Sentinel{};


};


















// ----------------------------------- generally templated -------------------------------
// This is only here to explore the most evil form of overloading "operator," and see how much it can break. 
// Turns out it is not too bad: this op only binds at the very end if all esle fails. Functions with multiple args are still bound first.
// Nevertheless, we definitely don't wanna use this anywhere in production.
//template <typename T>
//ZefTensor<T, 1> operator, (T x1, T x2){
//	std::cout<<"templated operator, called for T, T\n";
//	return ZefTensor<T, 1> {std::array{x1, x2}};
//}





//
//
//template <typename T>
//ZefTensor<T, 1> operator, (ZefTensor<T, 1> exisiting_rel_list, T rt2){
//
//	std::cout << "ZefTensor (ZefTensor<T, 1>, T) called\n";
//
//	ZefTensor<T, 1> res(exisiting_rel_list.size()+1);
//	for(int c=0; c < exisiting_rel_list.size(); c++) res[c] = exisiting_rel_list[c];
//	res[exisiting_rel_list.size()] = rt2;
//	return res;
//}


// --------------------------------- specialized ------------------------------------------


// Use this version very sparingly in production to catch the case " L[RT.IfPreviousIs, RT.IfNextIs]  "

//template <>  // no longer using template specialization
ZefTensor<RT, 1> operator, (RT rt1, RT rt2){
	std::cout<<"specialized ZT operator , called for RT, RT\n";
	return ZefTensor<RT, 1> {std::array{rt1, rt2}};
}






ZefTensor<RT, 1> operator, (ZefTensor<RT, 1> exisiting_rel_list, RT rt2) {

	std::cout << "ZefTensor (ZefTensor<RT, RT>, RT) called\n";

	ZefTensor<RT, 1> res(exisiting_rel_list.size() + 1);
	for (int c = 0; c < exisiting_rel_list.size(); c++) res[c] = exisiting_rel_list[c];
	res[exisiting_rel_list.size()] = rt2;
	return res;
}








//
//
//using str = std::string;
//
//void test4(){
//	using namespace std;
//
//	auto rts = (RT{ 5 }, RT{ 8 }, RT{9}, RT{ 81 }, RT{119});
//	cout << "v0: " << rts[0].m << "\n";
//	cout << "v1: " << rts[1].m << "\n";
//
//	for(const auto &el: rts){
//		cout << el.m<< ", ";
//	}
//	cout << "\n\n"<<rts.size()<<"\n";
//
//
//	//auto ms = ( AnyVal{5}, AnyVal{42.1}, AnyVal{str{"hello"}} );
//	// for(auto &el: ms){
//	// 	cout << el<< ", ";
//	// }
//	//cout << "\n\n"<< std::get<str>(ms[2].val) <<ms.size()<<"\n";
//
//	auto ww=std::array{9,4,1};
//	ZefTensor<int, 1> w = ZefTensor<int, 1>(ww);
//	for(auto &el: w){
//		cout << el<< ", ";
//	}
//
//
//
//
//	// althought the comma operator is overloaded, RT still binds to to binary functions first
//	[](auto rt1, auto rt2){
//		cout<< "\n\n    " << rt1.m <<"\n";
//		cout<< "    " << rt2.m <<"\n";
//	}(RT{4}, RT{85});
//
//
//
//
//}










// ----------- do functions still bind with multiple arguments first when the operator, has been overloaded? Yes :) ---------------------------------


void ff(RT r1, RT r2){
	std::cout<< "ff called!!\n"; 
}

// ---------------- the generic function below still binds first ---------------------------
void ff2(auto r1, auto r2){
	std::cout<< "ff2 called!!\n"; 
}







struct L_class {
	using var = std::variant<
		RT,
		std::array<RT,1>,
		std::array<RT,2>,
		std::array<RT,3>,
		std::vector<RT>
		>;
	var v;

	//L_class operator[] (return L_class{ RT{111} }; ) const;
	
	L_class operator[] (ZefTensor<RT, 1> zt) const{  
		std::cout<< "L operator[ZefTensort<RT, 1> called!!]"; 
		std::cout<< "\n\n----------\n";
		for(auto el: zt)
			std::cout << "  --> " << el.m <<"\n";

		return L_class{RT{111}}; 
		
		}

};
const L_class L;


void test3(){
	using namespace std;
    //auto something = (RT{5}, RT{8}, RT{117});

	auto xx = L[RT{5}, RT{8}, RT{117}, 63, RT{100001}, RT{1}];
	//ff(RT{8}, RT{117});   // binds to the fct taking two RTs, does not bind to operator, first
	//ff2(RT{8}, RT{117});
	//ff2((RT{8}, RT{117}), 5);

}





int main() {
	test3();
	
	std::cout << "done\n";
	return 0;
}
