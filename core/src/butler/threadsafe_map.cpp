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

#include "butler/threadsafe_map.h"

namespace zefDB {
    namespace hidden {
        void zef_enum_bidirectional_map::disp() {
            print("------------- zef_enum_bidirectional_map ---------------");
            print("    ------------- indx_to_enum_name ---------------");
            for (auto el : indx_to_string_pair)
                print(to_str(el.first) + ":     " + el.second.first + "." + el.second.second);

            print("    ------------- enum_name_to_indx ---------------");
            for (auto el : enum_name_to_indx)
                print( el.first + ": " + to_str(el.second));
            print("    *************************************************");
        }


        enum_indx zef_enum_bidirectional_map::generate_unused_random_number() {
            static std::random_device rd;  //Will be used to obtain a seed for the random number engine
            static std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
            static std::uniform_int_distribution<enum_indx> dis(constants::compiled_aet_types_max_indx, std::numeric_limits<enum_indx>::max());
            enum_indx ran_initial = dis(gen);
            enum_indx indx_candidate = ran_initial - (ran_initial % 16);
            return contains(indx_candidate) ? generate_unused_random_number() : indx_candidate;
        }


        // enum_indx zef_enum_bidirectional_map::insert(string_pair sp) {
        //     enum_indx new_indx = generate_unused_random_number_in_enum_reserved_range();
        //     enum_name_to_indx[sp.first + "." + sp.second] = new_indx;
        //     indx_to_string_pair[new_indx] = sp;
        //     return new_indx;
        // }

        void zef_enum_bidirectional_map::insert(enum_indx new_indx, string_pair sp) {            
            enum_name_to_indx[sp.first + "." + sp.second] = new_indx;
            indx_to_string_pair[new_indx] = sp;            
        }

        string_pair zef_enum_bidirectional_map::at(enum_indx indx_key) {
            try {                 
                return indx_to_string_pair.at(indx_key);
            }
            catch (...) {
                std::cout << "indx_key out of range: " << indx_key << std::endl;
                throw std::runtime_error("key not found in zef_enum_bidirectional_map");
            }
        }

        enum_indx zef_enum_bidirectional_map::at(const string_pair& sp) {
            try { return enum_name_to_indx.at(sp.first + "." + sp.second); }
            catch (...) {
                std::cout << "string key not found: " << sp.first + "." + sp.second << std::endl;
                throw std::runtime_error("string pair key not found in zef_enum_bidirectional_map");
            }
        }

        // enum_indx zef_enum_bidirectional_map::get_or_insert_and_get_enum_number(const string_pair& name_pair) {
        //     auto found_val_it = enum_name_to_indx.find(name_pair.first + "." + name_pair.second);  // if the key has been added previously, it will have been found
        //     if (found_val_it != enum_name_to_indx.end()) return found_val_it->second;
        //     else return insert(name_pair);
        // }

        bool zef_enum_bidirectional_map::contains(enum_indx indx_key) {
            return zefDB::contains(indx_to_string_pair, indx_key);
        }

        bool zef_enum_bidirectional_map::contains(const string_pair& name_pair) {
            return zefDB::contains(enum_name_to_indx, name_pair.first + "." + name_pair.second);
        }

        size_t zef_enum_bidirectional_map::size() {
            return indx_to_string_pair.size();
        }
    

        std::vector<std::tuple<enum_indx, std::string, std::string>> zef_enum_bidirectional_map::all_entries_as_list() {
            // tasks::apply_immediate_updates_from_zm();
            return ranges::views::all(indx_to_string_pair)
                | ranges::views::transform([](const auto& x)->std::tuple<enum_indx, std::string, std::string> { return { x.first, x.second.first, x.second.second }; })
                | ranges::to<std::vector>
                | ranges::actions::sort([](auto& x1, auto& x2) { return std::get<1>(x1) < std::get<1>(x2); });
        }

        std::vector<std::string> zef_enum_bidirectional_map::all_enum_types() {
            return ranges::views::all(indx_to_string_pair)
                | ranges::views::transform([](const auto& x)->std::string { return { x.second.first }; })
                | ranges::to<std::vector>
                | ranges::actions::sort
                | ranges::actions::unique;
        }

        std::vector<std::string> zef_enum_bidirectional_map::all_enum_values(const std::string & needle) {
            return ranges::views::all(indx_to_string_pair)
                | ranges::views::transform([](const auto& x)->string_pair { return { x.second.first, x.second.second }; })
                | ranges::views::filter([&needle](const auto& x)->bool { return x.first == needle; })
                | ranges::views::transform([](const auto& x)->std::string { return x.second; })
                | ranges::to<std::vector>;
        }

        std::vector<enum_indx> zef_enum_bidirectional_map::all_indices() {
            return ranges::views::all(indx_to_string_pair)
                | ranges::views::transform([](const auto& x)->enum_indx { return { x.first }; })
                | ranges::to<std::vector>;
        }






    }
}
