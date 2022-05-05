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




constexpr auto print = [](const auto& x)->void { std::cout << x << std::endl; };
constexpr auto printt = [](const auto& v)->void { for (auto& el : v) std::cout << el << ", "; std::cout << std::endl; };




 // ------------------------ dummy ---------------------------
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
	static constexpr auto OrderedMaterial = RelationType{ 5127 };
	static constexpr auto Amount = RelationType{ 513 };
};
auto RT = RT_{};

//----------------------------------------------------------------------------



struct Any{};
struct ZRType{};   // placeholder that can deal with both ZefRef/EZefRef
struct ZefRef{};
struct EZefRef{};



using atomic_zefop_type_indx = int;

// acts like an enum, saves data like an enum (as a et_enum_num_data=ushort), but is a struct
struct AtomicZefopType {		
	constexpr AtomicZefopType(atomic_zefop_type_indx n = 0) : index_val(n) {};
	atomic_zefop_type_indx index_val;
	bool operator== (const AtomicZefopType& rhs) const { return index_val == rhs.index_val; }
	bool operator!= (const AtomicZefopType& rhs) const { return index_val != rhs.index_val; }
};


struct AtomicZefopTypeStruct {		
	static constexpr AtomicZefopType GreaterThan{1};
	static constexpr AtomicZefopType LesserThan{2};
	static constexpr AtomicZefopType GreaterGreaterThan{3};
	static constexpr AtomicZefopType LesserLesserThan{4};
};

constexpr AtomicZefopTypeStruct AZT;








struct GreaterThan_{
	const AtomicZefopType zo_type = AZT.GreaterThan;
	const RelationType rt = RT._undefined;   // TODO: this should be a ZefTensor<1, BT> or ZefTensor<1, RT>
};
struct LesserThan_{
	const AtomicZefopType zo_type = AZT.LesserThan;
	const RelationType rt = RT._undefined;   // TODO: ZefTensor
};
struct GreaterGreaterThan_{
	const AtomicZefopType zo_type = AZT.GreaterGreaterThan;
	const RelationType rt = RT._undefined;   // TODO: ZefTensor
};
struct LesserLesserThan_{
	const AtomicZefopType zo_type = AZT.LesserLesserThan;
	const RelationType rt = RT._undefined;   // TODO: ZefTensor
};
struct Pipe_{
	const AtomicZefopType zo_type = AZT.LesserLesserThan;
	const RelationType rt = RT._undefined;   // TODO: ZefTensor
};


struct Ins_{
	const AtomicZefopType zo_type;
};
struct Outs_{
	const AtomicZefopType zo_type;
};








namespace constants{
	constexpr int zefop_default_buffer_size = 64;
}

// DeltaL is the lifting number for this operator



template <int DeltaL, typename src_type, typename trg_type>
struct ZefOp;






struct Now{};
constexpr Now now;





template<int DeltaL, typename src_type, typename trg_type>
struct ZefOp {
	constexpr ZefOp(){};
	constexpr ZefOp(Now n){};

	//char buffer[constants::zefop_default_buffer_size] = {'b'};
};


template<>   // this is required to indicate template specialization
struct ZefOp<0, ZRType, Any> {
	constexpr ZefOp(){};

	//char buffer[constants::zefop_default_buffer_size] = {'a'};
};



template <>   // this is required to indicate template specialization
struct ZefOp<0, ZRType, ZRType> {
	constexpr ZefOp(){};
	constexpr ZefOp(RelationType rt){};

	//char buffer[constants::zefop_default_buffer_size] = {'a'};
};







template<int DeltaL1, typename src_type1, typename contracted_type, int DeltaL2, typename trg_type2>
constexpr ZefOp<DeltaL1+DeltaL2, src_type1, trg_type2> operator| (ZefOp<DeltaL1, src_type1, contracted_type> op1, ZefOp<DeltaL2, contracted_type, trg_type2> op2){
	ZefOp<DeltaL1+DeltaL2, src_type1, trg_type2> res;
	return res;
}



template<int DeltaL1, typename src_type1, int DeltaL2>
constexpr ZefOp<DeltaL1+DeltaL2, src_type1, ZefRef> operator| (ZefOp<DeltaL1, src_type1, ZefRef> op1, ZefOp<DeltaL2, ZRType, ZRType> op2){
	ZefOp<DeltaL1+DeltaL2, src_type1, ZefRef> res;
	return res;
}










// ZefOp<2, int, int> operator<< (ZefOp<0, int, int> op1, RelationType my_rt){
// 	ZefOp<2, int, int> res;
// 	return res;
// }


// constexpr ZefOp<0, int, int> operator| (ZefOp<0, int, int> op1, ZefOp<0, int, int> op2){
// 	ZefOp<0, int, int> res;
// 	return res;
// }



// constexpr ZefOp<0, int, int> operator> (ZefOp<0, int, int> op1, ZefOp<0, int, int> op2){
// 	ZefOp<0, int, int> res;
// 	return res;
// }


// constexpr ZefOp<0, int, int> operator< (ZefOp<0, int, int> op1, ZefOp<0, int, int> op2){
// 	ZefOp<0, int, int> res;
// 	return res;
// }


// constexpr ZefOp<0, int, int> operator<< (ZefOp<0, int, int> op1, ZefOp<0, int, int> op2){
// 	ZefOp<0, int, int> res;
// 	return res;
// }

template<int DeltaL1, typename src_type1, typename contracted_type, int DeltaL2, typename trg_type2>
auto operator>> (ZefOp<DeltaL1, src_type1, contracted_type> op1, ZefOp<DeltaL2, contracted_type, trg_type2> op2) -> ZefOp<DeltaL1+DeltaL2, src_type1, trg_type2> {
	ZefOp<DeltaL1+DeltaL2, src_type1, trg_type2> res;
	return res;
}




// TODO: how to get this explicit template specialization to work?????????????????????????????????????????????????????????????????????????????????????????
// template<> auto operator>> <0, ZRType, ZRType, 0, ZRType> (ZefOp<0, ZRType, ZRType> op1, ZefOp<0, ZRType, ZRType> op2) -> ZefOp<0, ZRType, ZRType>;

// ----------------- work around -----------------: overload explicit operator function. We need this to allow expressions of the for "RT.OrderedMaterial >> RT.UsedBy;" to be automatically cast
auto operator>> (ZefOp<0, ZRType, ZRType> op1, ZefOp<0, ZRType, ZRType> op2) -> ZefOp<0, ZRType, ZRType> {
	ZefOp<0, ZRType, ZRType> res;
	return res;
}


auto operator< (ZefOp<0, ZRType, ZRType> op1, ZefOp<0, ZRType, ZRType> op2) -> ZefOp<0, ZRType, ZRType> {
	ZefOp<0, ZRType, ZRType> res;
	return res;
}


template<int DeltaL1, typename src_type1, typename contracting_type, int DeltaL2, typename src_type2, typename trg_type2>
constexpr ZefOp<DeltaL1+DeltaL2, src_type1, trg_type2> operator| (ZefOp<DeltaL1, src_type1, contracting_type> op1, ZefOp<DeltaL2, contracting_type, trg_type2> op2){
	ZefOp<DeltaL1+DeltaL2, src_type1, trg_type2> res;
	return res;
}



template<int DeltaL1, typename src_type1, typename contracting_type, int DeltaL2, typename trg_type2>
constexpr ZefOp<DeltaL1+DeltaL2, src_type1, trg_type2> operator| (ZefOp<DeltaL1, src_type1, contracting_type> op1, ZefOp<DeltaL2, Any, trg_type2> op2){
	ZefOp<DeltaL1+DeltaL2, src_type1, trg_type2> res;
	return res;
}













struct String{};

const ZefOp<0, Any, Any> value;     				
const ZefOp<0, Any, String> value_dot_String;			// do we also want to allow "3 | value" -> "3"  to be called?
const ZefOp<1, ZRType, ZRType> L;
const ZefOp<-1, ZRType, ZRType> flatten = [](){ return ZefOp<-1, ZRType, ZRType>(); }();







void test_composition(){
	//g[3612] | now > RT.OrderedMaterial >> RT.Amount | value.String;

	

	//only zefop
	//auto my_op = now > RT.OrderedMaterial >> RT.Amount | value.String;
	// will be translated to a zefop data struct
	// [now_, Greater_, RT.OrderedMaterial, GreaterGreater_, RT.Amount, Pipe_, value.String]
	// we need to wrap all the values with their type: use a zef_type_wrapper

	//auto my_op =  now > now << RT.OrderedMaterial | now;

	auto opA = ZefOp<0, ZRType, ZRType>(RT.OrderedMaterial);
	auto opB = ZefOp<0, ZRType, ZRType>(RT.OrderedMaterial);
	
	auto opC = opA >> opB;

	//auto opD =  ZefOp<0, ZRType, ZRType>(RT.OrderedMaterial) >>  ZefOp<0, ZRType, ZRType>(RT.UsedBy);

	auto op3 = RT.OrderedMaterial >> RT.UsedBy < RT.Ordered >> RT.OrderedMaterial >> RT.UsedBy < RT.Ordered | value;
	auto op4 = RT.OrderedMaterial >> RT.UsedBy;

	//auto x = op3 | ZefOp<0, ZRType, int>(value);
	
}












template <int ListOrder, typename ScalarType>
struct ZefTensor{};





template<int L, typename contracting_type, int DeltaL, typename trg_type2>
constexpr ZefTensor<L+DeltaL, trg_type2> operator| (ZefTensor<L, contracting_type> tens, ZefOp<DeltaL, contracting_type, trg_type2> op2){
	ZefTensor<L+DeltaL, trg_type2> res;
	return res;
}




template<int L, typename ZRType_case, int DeltaL>
constexpr ZefTensor<L+DeltaL, ZRType_case> operator| (ZefTensor<L, ZRType_case> tens, ZefOp<DeltaL, ZRType, ZRType> op2){
	ZefTensor<L+DeltaL, ZRType_case> res;
	return res;
}








void test_composition_with_tnesor(){
	auto zz = ZefTensor<1, ZRType>();
	auto zz2 = ZefTensor<2, ZefRef>();
	auto zz3 = ZefTensor<1, EZefRef>();

	auto op3 = RT.OrderedMaterial >> RT.UsedBy < RT.Ordered >> RT.OrderedMaterial >> RT.UsedBy < RT.Ordered | value_dot_String;
	
	auto op4 = RT.OrderedMaterial >> RT.UsedBy < RT.Ordered;

	auto t2 = zz2 | op4;
	auto t3 = zz3 | op4;
}





int main() {
	test_composition();

	std::cout << "...done\n";
	return 0;
}
