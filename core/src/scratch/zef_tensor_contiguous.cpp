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
#include <string.h>
#include <string_view>
#include <algorithm>


constexpr auto print = [](const auto& x)->void { std::cout << x << std::endl; };
constexpr auto printt = [](const auto& v)->void { for (auto& el : v) std::cout << el << ", "; std::cout << std::endl; };


using Str = std::string;
using Strs = std::vector<Str>;
using Strss = std::vector<Strs>;

using Doubles = std::vector<double>;
using Doubless = std::vector<Doubles>;

using Ints = std::vector<int>;
using Intss = std::vector<Ints>;



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


struct Any{
	std::variant<
		double,
		int,
		bool,
		std::string,
		BT,
		RT> val;
};


// store the type as a value to access at runtime
struct Zignature{
    const int zignature_code = 0;
};






namespace constants{
    constexpr int string_buffer_length = 20;
}




// auto my_str = String("hello");   // this will allocate the 'my_str' object on the stack and the size needs to be known at compile time. 
// If the assigned value is too large for the sso buffer, we can decide what to do. Use a custom allocator that preallocated a memory pool 
// and assigns the required amount when the String::new operator is called. This needs to be a custom allocator, since the String may overflow
// and the required size is passed to the allocator at time of construction.

// The objects allocated in the pool are fully valid String objects by themselves (with the appropriate) struct header layout


struct String {
    String* referenced_struct_ptr = nullptr;  // if null: data held locally
    const int array_length = 0;
    char buffer_[constants::string_buffer_length+1];  // also contains one character for null termination (to give c_str() functionality without copy)


    String(const char* val);  // requires the char array to be \0 termianted
    String(std::string val);
    char* char_ptr_start() { return (referenced_struct_ptr == (String*)nullptr) ? buffer_ : referenced_struct_ptr->buffer_; }     
    int size() { return array_length; }
    std::string_view operator* () { return std::string_view(char_ptr_start(), array_length ); }
};

String::String(const char* val) : array_length(strlen(val)){    
    if(array_length<constants::string_buffer_length){
        memcpy(buffer_, val, array_length+1);  // also copy the '\0'
    }else{
        throw std::runtime_error("allocation of String not implemented yet");
    }
}

String::String(std::string val) : array_length(val.size()){    
    if(array_length<constants::string_buffer_length){
        memcpy(buffer_, val.c_str(), array_length+1);  //copy the '\0'        
    }else{
        throw std::runtime_error("allocation of String not implemented yet");
    }
}













template <typename ScalarType, int ListOrder>
struct ZefTensor;


// template this to be used both for const and non-const iterators. Define this before the actual ZefTensor, then we can define 'begin' etc. within
// TODO: add all required interface functions to make this comply with random_access_iterator_tag: see http://www.cplusplus.com/reference/iterator/RandomAccessIterator/ !!!!!!!!!!!!!!!!

template <bool IsConstantIterator, typename ScalarType, int ListOrder>
struct MyIterator {
    // we need to specify these: pre-C++17 this was done by inheriting from std::Iterator
    using value_type = ZefTensor<ScalarType, ListOrder-1>;
    // using reference = ZefTensor<ScalarType, ListOrder-1>&;
    using reference = typename std::conditional_t<IsConstantIterator, const ZefTensor<ScalarType, ListOrder-1>&, ZefTensor<ScalarType, ListOrder-1>& >;
    using pointer = typename std::conditional_t<IsConstantIterator, const ZefTensor<ScalarType, ListOrder-1>*, ZefTensor<ScalarType, ListOrder-1>* >;
    using iterator_category = std::random_access_iterator_tag;   // see http://www.cplusplus.com/reference/iterator/RandomAccessIterator/ for requirements
    using difference_type = int;

    // each iterator consists of a pointer to the actual data struct, as well as an index saved as integer
    const ZefTensor<ScalarType, ListOrder>* referenced_struct_ptr = nullptr;
    int array_pos = 0;

    MyIterator& operator++() { ++array_pos; return *this; }		// pre-increment op: this one is used mostly
    MyIterator operator++(int) { return MyIterator{ referenced_struct_ptr, array_pos++ }; }		// post incremenet
    reference operator*() { return referenced_struct_ptr->operator[](array_pos); }
    std::add_const_t<reference> operator*() const { return referenced_struct_ptr->operator[](array_pos); }
    value_type operator[](const difference_type& n) const { return referenced_struct_ptr->operator[](array_pos+n); }
    bool operator==(const MyIterator& other) const { return referenced_struct_ptr == other.referenced_struct_ptr && array_pos == other.array_pos; }
    bool operator!=(const MyIterator& other) const { return !(*this == other); }
};







template <bool IsConstantIterator, typename ScalarType>
struct MyIteratorPtr {
    // we need to specify these: pre-C++17 this was done by inheriting from std::Iterator
    using value_type = ScalarType;
    using reference = typename std::conditional_t<IsConstantIterator, const ScalarType&, ScalarType& >;
    using pointer = typename std::conditional_t<IsConstantIterator, const ScalarType*, ScalarType* >;
    using iterator_category = std::random_access_iterator_tag;   // see http://www.cplusplus.com/reference/iterator/RandomAccessIterator/ for requirements    
    using difference_type = ptrdiff_t;

    ScalarType* ptr_to_current_el = nullptr;

    MyIteratorPtr& operator++() { ++ptr_to_current_el; return *this; }		// pre-increment op: this one is used mostly
    MyIteratorPtr operator++(int) { return MyIteratorPtr{ ptr_to_current_el++ }; }		// post incremenet
    reference operator*() { return *ptr_to_current_el; }
    std::add_const_t<reference> operator*() const { return *ptr_to_current_el; }
    value_type operator[](const difference_type& n) const { return *(ptr_to_current_el + n); }
    bool operator!=(const MyIteratorPtr& other) const { return ptr_to_current_el != other.ptr_to_current_el; }
    bool operator==(const MyIteratorPtr& other) const { return ptr_to_current_el == other.ptr_to_current_el; }
};
















template <typename ScalarType, int ListOrder>
struct ZefTensor{    
    using ListElementType = ZefTensor<ScalarType, ListOrder-1>;

    const ZefTensor<ScalarType, ListOrder>* referenced_struct_ptr = nullptr;       // nullptr means the data is actually stored_locally
    const Zignature zignature {42};
    int ref_count = 0;  // need this???
    const int true_local_size_in_bytes = 0;                 // this + true_local_size_in_bytes indicates the true end of this object
    const int array_length = 0;                             // the buffer is used both for the array and blobs. The array runs up to this + array_length
    char buffer_[111];   //contains both the array and subsequent blobs

    // call this to get a ptr to the struct that actually contains the data. Could be this or referenced
    const ZefTensor<ScalarType, ListOrder>* true_data_struct() const { return referenced_struct_ptr==0 ? this : referenced_struct_ptr; }  
    int size() const { return true_data_struct()->array_length; }   
    ListElementType& operator[] (int ind) {
        if( ind<0 || ind >= array_length )
            throw std::runtime_error("index error calling operator[] called for ZefTensor");
        const ZefTensor<ScalarType, ListOrder>* data_ptr = true_data_struct();        
        const int blob_positon_indx = ((int*)data_ptr->buffer_)[ind];   // the array element contains the offset from the beginning of the actual object in bytes
        return *(ListElementType*)(std::uintptr_t(data_ptr) + blob_positon_indx);
    }
    const ListElementType& operator[] (int ind) const {
        if( ind<0 || ind >= array_length )
            throw std::runtime_error("index error calling operator[] called for ZefTensor");
        const ZefTensor<ScalarType, ListOrder>* data_ptr = true_data_struct();        
        const int blob_positon_indx = ((int*)data_ptr->buffer_)[ind];   // the array element contains the offset from the beginning of the actual object in bytes
        return *(ListElementType*)(std::uintptr_t(data_ptr) + blob_positon_indx);
    }

    using Iterator = MyIterator<false, ScalarType, ListOrder>;
    using ConstIterator = MyIterator<true, ScalarType, ListOrder>;
    
    Iterator begin()             { return Iterator{ referenced_struct_ptr==0 ? this : referenced_struct_ptr, 0 }; }
    Iterator end()               { return Iterator{ referenced_struct_ptr==0 ? this : referenced_struct_ptr, array_length }; }
    ConstIterator begin() const  { return ConstIterator{ referenced_struct_ptr==0 ? this : referenced_struct_ptr, 0 }; }
    ConstIterator end() const    { return ConstIterator{ referenced_struct_ptr==0 ? this : referenced_struct_ptr, array_length }; }
};



//                                      _                _       _
//                         ___  _ __ __| | ___ _ __     / |     | |_ ___ _ __  ___  ___  _ __ ___
//    _____ _____ _____   / _ \| '__/ _` |/ _ \ '__|    | |     | __/ _ \ '_ \/ __|/ _ \| '__/ __|   _____ _____ _____
//   |_____|_____|_____| | (_) | | | (_| |  __/ |       | |     | ||  __/ | | \__ \ (_) | |  \__ \  |_____|_____|_____|
//                        \___/|_|  \__,_|\___|_|       |_|      \__\___|_| |_|___/\___/|_|  |___/
//

// ---------------------- first template specialization for all standard scalar contiguous types -----------------------------



template <typename ScalarType>
struct ZefTensor<ScalarType, 1>{
    using ListElementType = ScalarType;

    const ZefTensor<ScalarType, 1>* referenced_struct_ptr = nullptr;       // nullptr means the data is actually stored_locally
    const Zignature zignature {42};
    int ref_count = 0;  // need this???
    const int true_local_size_in_bytes = sizeof(*this);                 // this + true_local_size_in_bytes indicates the true end of this object. Designed to overflow if on heap and managed
    const int array_length = 0;                                         // the buffer is used both for the array and blobs. The array runs up to this + array_length
    char buffer_[111];   //contains both the array and subsequent blobs

    // call this to get a ptr to the struct that actually contains the data. Could be this or referenced
    const ZefTensor<ScalarType, 1>* true_data_struct() const { return referenced_struct_ptr==0 ? this : referenced_struct_ptr; }    
    int size() const { return true_data_struct()->array_length; }   

    ListElementType& operator[] (int ind) {
        if( ind<0 || ind >= array_length )
            throw std::runtime_error("index error calling operator[] called for ZefTensor");              
        return ((ListElementType*)true_data_struct()->buffer_)[ind];   // in contrast to higher order tensors, we don't need an intermediate array of of integers to point to the blobs of elements. The first array contains these.     
    }

    const ListElementType& operator[] (int ind) const {
        if( ind<0 || ind >= array_length )
            throw std::runtime_error("index error calling operator[] called for ZefTensor");              
        return ((ListElementType*)true_data_struct()->buffer_)[ind];   // in contrast to higher order tensors, we don't need an intermediate array of of integers to point to the blobs of elements. The first array contains these.     
    }

    using Iterator = MyIteratorPtr<false, ScalarType>;
    using ConstIterator = MyIteratorPtr<true, ScalarType>;
    
    Iterator begin()             { return Iterator{ (ScalarType*)(true_data_struct()->buffer_) }; }
    Iterator end()               { return Iterator{ ((ScalarType*)(true_data_struct()->buffer_))+(true_data_struct()->array_length) }; }
    ConstIterator begin() const  { return ConstIterator{ (ScalarType*)(true_data_struct()->buffer_) }; }
    ConstIterator end() const    { return ConstIterator{ ((ScalarType*)(true_data_struct()->buffer_))+(true_data_struct()->array_length) }; }
};











































template <typename ScalarType, int ListOrder>
constexpr bool is_element_size_compile_time_known(ZefTensor<ScalarType, ListOrder> x){ return false; }

template <typename ScalarType>
constexpr bool is_element_size_compile_time_known(ScalarType x){ return true; }

// template <>
// constexpr bool is_element_size_compile_time_known(Str x){ return false; }

// template <>
// constexpr bool is_element_size_compile_time_known(Any x){ return false; }











template <typename ScalarType, int ListOrder>
auto make_tensor(const std::vector<ZefTensor<ScalarType, ListOrder>>& v_init)->ZefTensor<ScalarType, ListOrder+1>{
    ZefTensor<ScalarType, ListOrder+1> res = {
        nullptr,
        Zignature {21},
        0,
        60,   // TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        int(v_init.size())
    };
    return res;
}











template <typename T>
auto make_tensor(const std::vector<T>& v_init)->ZefTensor<T, 1>{
    print(v_init.size());
    
    print(std::is_trivially_copy_constructible_v<ZefTensor<T, 1>>);
    print(std::is_copy_constructible_v<ZefTensor<T, 1>>);

    print("-----");
    ZefTensor<T, 1> res = {
        nullptr,
        Zignature {21},
        0,
        60,   // TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        int(v_init.size())
    };
    //if constexpr(is_element_size_compile_time_known(std::declval<T>())){
    if constexpr(true){
        std::transform(
            v_init.begin(),
            v_init.end(),
            (T*)res.buffer_,
            [](T x) { return x; }
        );
    } else{

    }
    return res;
}





// L[
//     L["The", "bag", "cat"],
//     L["is", "out of the"]
// ] // Tensor<String,2>

// L[8,3,1,9] 
//     | lift[ [](int m)->double { return 2.5*m; } ]
//     | filter[ [](double x)->bool { return x>6.2; } ]
//     | reduce[ [](double x, double s){ return x+s; }, 0]


// L[RT.UsedBy, RT.FriendOf]

// auto my_tensor = L[EN.MachineStatus.idle, EN.Unit.centimeters, QuantityFloat(42.1, EN.Unit.kilometers)]  // Tensor<Any,1>

// my_tensor_ae <= my_tensor   // treats entire tensor as atomic
// my_e <= unpack[my_tensor, as_entities]   // unpacks/splats tensor: each element becomes a new atomic entity

// my_tensor_ae <= zef_set[compress_values[true]]

// make_api(my_gql_ae, host["api.zefdb.com/ninjas_api/"])
// make_api(my_gql_ae, host["localhost:6666", run_in_background[true]])


int main() {

    // constexpr auto x = double(4.3);
    // constexpr auto rr = is_element_size_compile_time_known(x);

    const auto t1 = make_tensor( Doubles({1.0,2.1,0.4,1234.5}) );
    auto t2 = make_tensor( Ints({99, 101,108,109}) );
	
    for(auto el: t2)
        print(el);

    t2[2] = 42;
    print(t2[2]);


    auto t3 = make_tensor(std::vector<ZefTensor<double, 1>>{t1, t1, t1});
    auto t4 = t3[0];


    // //print(x.val);

    ZefTensor<long, 3> v{};
    for(auto el1 : v)
    for(auto el2 : el1)
    for(auto el3 : el2)
        el3;

    // auto it1 = v.begin();
    // auto it2 = v.end();

    // //auto d = it2-it1;

    // //auto x = v[0][0][0][0];


    auto s = String("Hello Ninja!");
    print(*s);

    auto s2 = String(std::string("Hello Yolandi!")); 
    print(*s2);
	std::cout << "done\n";
	return 0;
}
