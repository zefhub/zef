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
#include <tuple>
#include <string>
#include <variant>
#include <vector>
#include <cstring>
#include <string_view>

using str = std::string;


constexpr auto print = [](const auto& x)->void { std::cout << x << std::endl; };
constexpr auto printt = [](const auto& v)->void { for (auto& el : v) std::cout << el << ", "; std::cout << std::endl; };





// ------------------------------------ zef memory allocation system: ----------------------------
// 1) on new thread starting: reserve arena on very first request.
// 2) in CTOR: call auto [mem_pool_buffer_start, actual_max_size] = request_memory(min_size_requirement, approximate_size)
// 3) in CTOR: if we notice that we need more than actual_max_size:
//       a) call auto [mem_pool_buffer_start2, actual_max_size2] = request_memory(min_size_requirement2, approximate_size2)
//       b) memcpy contents from mem_pool_buffer_start to mem_pool_buffer_start2 if wanted
//       c) call release_mem_buffer(mem_pool_buffer_start) (for first buffer that we're abandoning)
// 4) in DTOR: release_mem_buffer(mem_pool_buffer_start)
//




// The produced function may be called a lot. Do it this way that the pointer to the mem arena 
// itself is put on the stack. Using a static variable would put in into the bss section of process memory
// which is not as likely to be hot in the cache as the stack.
struct ZefMemAllocator{

    //  |(M)----------(M)---(M)------------------(M)--------               
    //                                                      ^ head
    //   each M = (indx of prev. start, indx of next start)

    void* mem_arena_ptr;
    size_t head;    // offset from mem_arena_ptr in bytes 
    size_t arena_size;    // how much is allocated? This is fixed for now - we may wanna move this to mmap soon.
    bool unconfirmed_write_process_is_open;

    struct InfoBlock{
        size_t previous_block_start;
        size_t next_block_start;
    };

     ZefMemAllocator(size_t arena_size=1024*1024*256) :
        mem_arena_ptr([](size_t arena_size){return ::operator new(arena_size);}(arena_size)), 
        head(0),
        arena_size(arena_size),
        unconfirmed_write_process_is_open(false)
    {}

    // min_size_requirement: the abolute minimum size we know that we want returned (in bytes).
    // approximate_size: If we don't know how much space may be needed, but we have a guesstimate. 
    // This is used to decide whether which segment to return, in case there are available fragments.
    auto request_memory(size_t min_size_requirement, size_t approximate_size = 0){ 
        void* mem_pool_buffer = mem_arena_ptr;   // TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        size_t actual_max_size = 42;
        return std::make_tuple(mem_pool_buffer, actual_max_size);
    };


    // this has to be called by the fct that requested memory before the next memory request can be processed.
    // Calling this function moves th write head forward and coompletes the write to buffer "transaction". 
    // No more overflowing hereafter.
    auto confirm_write_completion(void* mem_pool_buffer, size_t actual_written_size){
        
        return;
    }


    void release_memory(void* mem_pool_buffer) { 
        print("mem releasing!");        
        // check whether the ptr passed actually referes to something in our arena
    };

};
auto zef_mem_allocator1 = ZefMemAllocator();

















struct String{
    constexpr static int local_buffer_size = 9;//25;
    const int len;                              // if len=-1: magic value to indicate that it is not stored locally and the first 8 bytes of buffer contain the ptr
    const char buffer[local_buffer_size];       // contains the pointer to the actual object in the ZefMemAllocator's arena iff len == -1

    String(const char* s, bool allow_overflow=false);       // binds to char arrays e.g. s1 = String("hello!");
    ~String();
};



// Warning: this constructor manually overwrites the contents of the const buffer! 
// This is only done in the constructor and is the best of a bunch of bad options afaik!
String::String(const char* s, bool allow_overflow) : 
    len(
        [](const char* s, bool allow_overflow)->int{
            int m = strlen(s);
            if (allow_overflow) return m;
            return m < String::local_buffer_size ? m : -1;
        }(s, allow_overflow)
     ),
    buffer()    // we can't call memcpy from here, overwrite this below
    {    
        int m = strlen(s);
        if(len == -1){
            auto [mem_pool_buffer, actual_max_size] = zef_mem_allocator1.request_memory(m+1, m+1);       // FIXME:  ask for the correct amount of mem!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!     
            // set the first 8 bytes in buffer to the address where the remote object is stored
            String** p = (String**)buffer;   // a pointer that contains a String pointer
            *p = (String*)mem_pool_buffer;
            // TODO: try     String* p = (String*)mem_pool_buffer;
            new(mem_pool_buffer) String(s, true);   // placement new on the mem area returned by the allocator. We reserved sufficient space to allow overflowing the buffer.                        
        } 
        else memcpy((void*)buffer, s, m+1);   // dangerous part!!! Overwrite const buffer. Copy the null character at the end along

        return;
    }


String::~String(){
    print("string destructor called!");
    if(len == -1) (*(String**)buffer)->~String();   // explicitly call the dtor for the remotely allocated object (after placement new)
    if(len == -1) zef_mem_allocator1.release_memory(*(void**)buffer);
    
}



int length(const String& s){
    return s.len >= 0 ? s.len : length(**(String**)s.buffer);    // a bit of mental pointer gymnastics :). We must use a pointer to a pointer here.
}

std::ostream& operator<< (std::ostream& os, const String& s){
    if(s.len >= 0) {os << std::string_view(s.buffer, s.len);}
    else {
        auto str_ptr = *(String**)s.buffer;
        os << std::string_view(str_ptr->buffer, str_ptr->len);
        }        
    return os;
}


void test_zef_string(){ 
    //auto s = String("hello@");
    auto s = String("hello there!");
    auto s2 = String("Good night");
    print("str alloc done");
    print(length(s));
    print(s);
    print(s2);
    // print(sizeof(String));


    //s | append("hello")

    // auto s2 = append(s, "abc", "def");  // s2 is a new String and s is not affected
    // auto s2 = append(std::move(s), "abc", "def");  // more performant since s2 can reuse the remotely allocated object if this is large


}







namespace flat_graph{


    // should we just use the zefDB BT. ... blob types and extend these?

    // The buffer for the following structs are designed to overflow. 
    // They are only designed to be used *within* a FGraph.
    struct Entity{    
        const int blob_type;
        const int entity_type;
        const int allocated_edge_list_length;         
        const int edge_list[0];
    };

    struct ExistingEntity{ 
        const int blob_type;
        const int entity_type;
        const int allocated_edge_list_length;
        const char[16] existing_uid;
        const int edge_list[0];
    }

    struct Relation{    
        const int blob_type;
        const int relation_type;
        const int allocated_edge_list_length;
        const int src_indx;
        const int trg_indx;
        const int edge_list[0];
    };

    struct ExistingRelation{    
        const int blob_type;
        const int relation_type;
        const int allocated_edge_list_length;
        const int src_indx;
        const int trg_indx;
        const char[16] existing_uid;
        const int edge_list[0];
    };

    struct AtomicEntity{    
        const int blob_type;
        const int entity_type;
        const int allocated_edge_list_length;
        const int value_buffer_length;
        const int edge_list[0];
        const char value_buffer[0];
    };

    struct ExistingAtomicEntity{    
        const int blob_type;
        const int entity_type;
        const int allocated_edge_list_length;
        const int value_buffer_length;
        const char[16] existing_uid;        
        const char edge_list_and_buffer_combined[0];
    };

    void* get_value_buffer(const ExistingAtomicEntity& ent){
        return void*(&ent.edge_list_and_buffer_combined) + ... // determine offset from allocated_edge_list_length
    }









    // ------ overload custom size functions which respect the overflow of the edge list and the data buffer -------

    int size(const Entity& ent){
        // auto p1 = static_cast<const void*>(&ent.edge_list[0]);
        // auto p2 = static_cast<const void*>(&ent);
        // return ent.allocated_edge_list_length * sizeof((ent.edge_list[0])) + (size_t(p1)-size_t(p2));
        return ent.allocated_edge_list_length * sizeof((ent.edge_list[0])) + sizeof(ent);
    }

    int size(const Relation& rel){
        return rel.allocated_edge_list_length * sizeof((rel.edge_list[0])) + sizeof(rel);
    }

    int size(const AtomicEntity& ae){
        return ae.allocated_edge_list_length * sizeof((ae.edge_list[0])) + ae.value_buffer_length + sizeof(ae);
    }


}


struct FGraph{
    constexpr static int local_buffer_size = 15;    // at which size do we start allocating remotely?
    const FGraph* foreign_buffer_ptr;        // if this is nullptr, it means that all data is stored in this very buffer
    const int fgraph_size_in_16b;    // since the offsets are also measured in units of 16 bytes for each blob, we can use the same unit for the total grpah size.
    const int fgraph_id;             // a FRef may contain this id as well if the safety switch is on for FGraphs: allow checking that the graph is still there when accessed
    const int hashmap_size;          // if this is non-zero, this implies that
    const bool all_edgelists_are_sorted;
    const bool is_compacted;
    // if this struct happens to be on the stack and the graph is too large, 
    // then have a pointer forwarding to an object of this very type
    const int blobs_start_index;   // there may be a hash table or other meta info after this: where do the data blobs start? Measured as a distance from this
    const char blobs_buffer[local_buffer_size];
};

    size_t size(const FGraph& g){
        return g.foreign_buffer_ptr == nullptr ? g.graph_size : size(*g.foreign_buffer_ptr);
    }


    struct FRef{
        const FGraph* graph_ptr;
        const int offset;           // offset from graph_ptr in units of f_chunk_size (typically 16 bytes). This is a compromise to allow for graphs with 32GB
        const int graph_id;         // optional and functionality may be turned on via a compiler switch: allows checking whether the graph is still alive and help debug bugs involving lifetime management of the fgraph
    };


    // A weak flat ref: does not own the graph, i.e. not register and unregister with the ref counter on the graph to hook into the garbage collection.    
    struct WFRef{
        const FGraph* graph_ptr;
        const int offset;           // offset from graph_ptr in units of f_chunk_size (typically 16 bytes). This is a compromise to allow for graphs with 32GB
        const int graph_id;         // optional and functionality may be turned on via a compiler switch: allows checking whether the graph is still alive and help debug bugs involving lifetime management of the fgraph
    };







struct ZefOp{
    // do we want input output types here separately or just put them on the graph?
    FGraph g;
};

struct LZefTensor{
    // resulting type of my_zeftensor | lazy_zefop before eval is called
    // also for any my_scalar | my_zefop 
};


// ???????????????? unsure about this ????????????????
struct ValueOp{

};



// ??????????????? do we need this or can 'value' be just another ZefOp and the Zef Runtime chooses to evaluate?
struct EvaluatingZefOp{

};



const auto only = ZefOp();  // todo: init with data
const auto outs = ZefOp();  // todo: init with data
const auto first = ZefOp();  // todo: init with data
const auto flatten = ZefOp();  // todo: init with data

auto my_zefop = flatten | outs | first;

ZefOp operator| (const ZefOp& pre, const ZefOp& post) {
    // check composition rules
    // combine the graphs, create new type signature
}


ZefOp operator>> (const ZefOp& pre, RelationType rt) {
    // check composition rules
    // combine the graphs, create new type signature
}


ZefOp operator> (const ZefOp& pre, RelationType rt) {
    // check composition rules
    // combine the graphs, create new type signature
}


// required for my_op >> L[RT.FriendOf]
ZefOp operator>> (const ZefOp& pre, const ZefOp& post) {
    // check composition rules
    // combine the graphs, create new type signature
}


// required for my_op > L[RT.FriendOf]
ZefOp operator> (const ZefOp& pre, const ZefOp& post) {
    // check composition rules
    // combine the graphs, create new type signature
}

// ... <, << ...


LZefTensor operator| (ZefRef z, ZefOp){
    
}

LZefTensor operator| (double x, ZefOp){

}





// force evaluation: these can be cast to the elementary types as well in case it is e.g. an int
ZefTensor operator| (LZefTensor, Value);        // TODO: first try this without introducing a special Value type. value is just another zefop

EvaluatingZefOp operator| (const ZefOp& op, const Value& val);

EvaluatingZefOp operator| (const ZefOp& pre, const EvaluatingZefOp& post);

ZefTensor operator| (const LZefTensor& pre, const EvaluatingZefOp& post);





//can we write?
// use cast operator https://en.cppreference.com/w/cpp/language/cast_operator
// defined for various types 
Ints squared_vec =
    {1,3,6,8,11,14} 
    | map[ [](int x)->int{return x*x;} ] 
    | value;







// // how do we want to initialize a FGraph?
// A simple linear list of declarations: order does not play a role. Introduce temporary tags / names
// void test_allocated_FGraph(
//     FGraph g = {
//         {ET.Person, "bob"},              // the second is just a key for intermediate construction?
//         {ET.Person, "fred"},

//         {RT.FriendOf, "bob", "friend rel1", "fred"},
//         {RT.FriendOf, "fred", "name r1", "fred name"},

//         {AET.String, "Fred", "fred name"},
//     };
// )

//auto fg = FGraph() | attach[ET.Dog]


const auto instances = ZefOp(FGraph({ {ET.Instances, }, }));
// instances[ET.Dog] will create a new zefop, where the additional info "ET.Dog" is attached to the new graph by e.g. RT.Type

const auto first = ZefOp(FGraph({ {ET.First, }, }));
const auto last = ZefOp(FGraph({ {ET.Last, }, }));
const auto only = ZefOp(FGraph({ {ET.Only, }, }));

// in addition: we can add the zefop spec to the very same graph. Develop a consistent language as we build this out. 
// e.g. only:   List[T] -> T;   what are the restrictions on T?

// e.g. map zefop: curry in a zef function. Should the actual function be copied onto the actual zefop graph? Is it always a reference to a zef graph? Could it be either?



FRef rufus = fg | instances[ET.Dog] | first;
String dog_name = fg | instances[ET.Dog] | first >> RT.Name | value;        // calls the cast operator at the very end: attempt conversion to actual string. While running that op, we can check that this is indeed a string.

// allow alternative syntax to key lookup if graph acts as dictionary?
fg >> RT.Name is equivalent to fg[RT.Name]


















struct A1{
    int x;
    str s;
};

struct A2{
    double x;
    str s;
};


struct A3{
    str x;
    bool b;
    str s;    
};

using V = std::variant<A1,A2,A3>;


void test_mixed_init_list(){ 
    std::vector<V> my_list = {
        A1{42, "bob"},        
        A2{42.5, "bob"},
        A3{"hello", true, "bob"},
    };
    print("tested list init!");
}






template <typename DeltaM>
struct ZefOp{

    FGraph g;
};


template <typename ScalarType, int TensorOrder>
struct ZefTensor{

};

struct ZefTensorAndZefOp{}
struct Evaluate{};
struct EvaluatingZefOp{};








int main(){

    //test_mixed_init_list();

    test_zef_string();



    // auto [ptr, actual_size] = internals::request_memory(1024, 4048);
    // internals::release_memory(nullptr);
    // print(actual_size);
    

    // auto ent = flat_graph::Entity{1,1,0};

    // print(sizeof(ent));
    // print(flat_graph::size(ent));

   

    return 0;
}





 //new((void*)(&ent.blob_type)) int(3);
    
    // how do we want to construct a FGraph if all fields are constant?
    //auto g2 = FGraph( {  {ET.Node, 34}, {AET.String, 8}, {RT.Name, 42, 34, 8}  } )

    //given 
    // FGraph g;
    // FGraph g2;
    // {
    //     auto hold_open = Transaction(std::move(g2));


    // }

    // mod_fct = [](){
    //     f = g >> attach[ET.Filter];
    // };
    // g4 = g2 | add(mod_fct);





// z >> L[RT.FriendOf]
//  | only 
//  >> RT.Name
//  | filter[[](auto s){return s=="abc";}]
//  | value;


// auto operator>>(RelationType rt1, RelationType rt2) -> ZefOp<0> {}

// auto operator>>(ZefOp<M> op, RelationType rt) -> ZefOp<M> {}







//  // **************************************** using initializer_list **********************************************
// #include <cassert> // for assert()
// #include <initializer_list> // for std::initializer_list
// #include <iostream>
 
// class IntArray
// {
// private:
// 	int m_length{};
// 	int *m_data{};
 
// public:
// 	IntArray() = default;
 
// 	IntArray(int length) :
// 		m_length{ length },
// 		m_data{ new int[length]{} }
// 	{
 
// 	}
 
// 	IntArray(std::initializer_list<int> list) : // allow IntArray to be initialized via list initialization
// 		IntArray(static_cast<int>(list.size())) // use delegating constructor to set up initial array
// 	{
//         print("using ii");
// 		// Now initialize our array from the list
// 		int count{ 0 };
// 		for (auto element : list)
// 		{
// 			m_data[count] = element;
// 			++count;
// 		}
// 	}
 
// 	~IntArray()
// 	{
// 		delete[] m_data;
// 		// we don't need to set m_data to null or m_length to 0 here, since the object will be destroyed immediately after this function anyway
// 	}
 
// 	IntArray(const IntArray&) = delete; // to avoid shallow copies
// 	IntArray& operator=(const IntArray& list) = delete; // to avoid shallow copies
 
// 	int& operator[](int index)
// 	{
// 		assert(index >= 0 && index < m_length);
// 		return m_data[index];
// 	}
 
// 	int getLength() const { return m_length; }
// };
 
// int main()
// {
// 	IntArray array{ 76, 5, 4, 3, 2, 1 }; // initializer list
// 	for (int count{ 0 }; count < array.getLength(); ++count)
// 		std::cout << array[count] << ' ';
 
// 	return 0;
// }
