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

#include "zefDB.h"
#include <malloc.h>

using namespace zefDB;

int main(void) {
    {
        Butler::initialise_butler();

        {
            auto butler = Butler::get_butler();

            // // Graph g{"danny-async-test"};
            // // Graph g{"fkg-iff-0293"};
            // // Graph g{};
            // Graph g{true};
            // std::cerr << "Graph uid is: " << (g | uid) << std::endl;

            // ZefRef z = instantiate(AET.Int, g);
            // auto my_func = [](ZefRef z) {
            //     std::cerr << "In the callback!" << std::endl;
            //     std::this_thread::sleep_for(std::chrono::seconds(2));
            // };
            // g | subscribe[my_func];

            // std::cerr << "Before assigning" << std::endl;
            // {
            //     Transaction ctx{g, false};
            //     // Transaction ctx{g};
            //     z <= 5;
            // }
            // std::cerr << "After assigning" << std::endl;

            // std::cerr << "Before assigning2" << std::endl;
            // {
            //     // Transaction ctx{g};
            //     Transaction ctx{g, false};
            //     z <= 2;
            // }
            // std::cerr << "After assigning2" << std::endl;

            // std::cerr << std::endl;
            // std::cerr << std::endl;
            // std::cerr << std::endl;
            // std::this_thread::sleep_for(std::chrono::seconds(5));
            // // std::cerr << std::endl;
            // // std::cerr << std::endl;
            // // std::cerr << "Going to load again" << std::endl;
            // // Graph g2{"danny-async-test"};

            // // {
            // //     Transaction transaction{g};

            // //     std::string huge_string("1234567890");
            // //     for(int i = 0;i < 10;i++)
            // //         huge_string = huge_string + huge_string;
            // //     instantiate(AET.String, g) <= huge_string;

            // //     std::cerr << "instantiated string" << std::endl;

            // //     // for (int i = 0; i < 10000; i++)
            // //     // for (int i = 0; i < 1000; i++)
            // //     for (int i = 0; i < 5; i++)
            // //         instantiate(ET.Machine, g);
            // //     std::cerr << "instantiated" << std::endl;
            // // }
            // // // Read in all strings just to access all pages
            // // for (auto it : g | instances[now][AET.String]) {
            // //     auto temp = *(it | value.String);
            // //     std::cerr << temp.substr(0,10) << std::endl;
            // // }
            
            // if(!verification::verify_graph_double_linking(g)) {
            //     std::cerr << "Verification failed!" << std::endl;
            // }

            // // Can't sync just yet!
            // // sync(g, true);
            // // std::cerr << "synced" << std::endl;



            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            Graph g{"danny2"};
            std::cerr << "Graph uid is: " << (g | uid) << std::endl;
            // malloc_stats();

            // std::string blobs(zefDB::internals::get_uids_as_bytes(g, 42, g.my_graph_data().read_head));
            // std::string uids;
            // uids = zefDB::internals::get_uids_as_bytes(g, 42, g.my_graph_data().read_head);

            // malloc_stats();
            {
            char temp;
            std::cerr << "Waiting for keypress" << std::endl;
            std::cin >> temp;
            }



            std::optional<Graph> gs[10];
            for (auto & g2 : gs) {
                g2 = clone(g);
                std::this_thread::sleep_for(std::chrono::milliseconds(3000));
                malloc_stats();
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));

            char temp;
            std::cerr << "Waiting for keypress" << std::endl;
            std::cin >> temp;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
        malloc_stats();
        char temp;
        std::cerr << "Waiting for keypress after hopefully destructing all" << std::endl;
        std::cin >> temp;
        std::cerr << "Stopping butler" << std::endl;
        Butler::stop_butler();
    }
    // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    // Stuff
    std::cerr << "End of main" << std::endl;
}
