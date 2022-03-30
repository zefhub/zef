#include <iostream>
#include <vector>
#include <type_traits>
#include <typeinfo>


constexpr auto print = [](const auto& x)->void { std::cout << x << std::endl; };
constexpr auto printt = [](const auto& v)->void { for (auto& el : v) std::cout << el << ", "; std::cout << std::endl; };








template <int ListOrder>
struct ZefTensor{
    std::vector<ZefTensor<ListOrder-1>> v;
    ZefTensor<ListOrder-1>& operator[](int ind) {return v[ind]; }
};

// ---------- specialization ------------                                                                                                   

template <>
struct ZefTensor<0>{
	double val;
};



template <int ListOrder>
auto reduce_list_order_by_1(ZefTensor<ListOrder> L){
    return ZefTensor<ListOrder-1>{};
}

template <>
auto reduce_list_order_by_1(ZefTensor<1> L){
    return long(42);
}






template <int ListOrder>
struct ZefTensor2{
    std::vector<ZefTensor2<ListOrder-1>> v;
    ZefTensor2<ListOrder/2>& operator[](int ind) {return v[ind]; }
};

// ---------- specialization ------------                                                                                                   

template <>
struct ZefTensor2<1>{
	double val;
};

template <int n, int m>
struct S{
    auto operator()() {return S<n-1, m*n>{}; }
};




int main() {

	
    ZefTensor<3> L3;
    auto L2 = reduce_list_order_by_1(L3);
    auto L1 = reduce_list_order_by_1(L2);
    auto L0 = reduce_list_order_by_1(L1);


    //auto x = L[4][2][7][1][1].val;

    ZefTensor2<16> L2;
    //auto x2 = L2[1][1][1][1].val;
    
    
    auto y = S<5,1>{}()()()();

	std::cout << "done\n";


	return 0;
}
