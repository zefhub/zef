#include <iostream>
#include <unordered_map>

// for a umap with custom structs as keys, we need the following interface

struct UID{
    //int x = 0;
    char x[16];
    struct HashFct{
        //std::size_t operator() (UID u) const { return std::hash<int>{}(u.x); }   // note: this needs to be const!!!!!
        std::size_t operator() (UID u) const { return *(std::size_t*)((u.x)); }   // note: this needs to be const!!!!!
    };
};
bool operator== (UID x1, UID x2){ return x1.x == x2.x; }



int main(){
    auto mm = std::unordered_map<UID, float, UID::HashFct>();
    auto u = UID{3};
    mm[u] = 1.0;

    std::cout << mm[u] << "\n";
}