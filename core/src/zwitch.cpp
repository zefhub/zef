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

#include "zwitch.h"
#include "graph.h"

namespace zefDB {
    Zwitch zwitch{};
    std::chrono::steady_clock::time_point time_start_of_process = std::chrono::steady_clock::now();

    std::ostream& operator<< (std::ostream& o, Zwitch zw) {
        o << "{" << std::endl;
        for(auto item : zw.as_dict()) {
            o << "\t\"" << item.first << "\": " << item.second << std::endl;
        }
        o << "}" << std::endl;
        return o;
    }
}
