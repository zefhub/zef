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