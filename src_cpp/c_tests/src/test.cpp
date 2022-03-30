#include "zefDB.h"

#include <iostream>
#include <thread>
#include <chrono>
#include <cassert>

#include <valgrind/callgrind.h>

using namespace zefDB;

int main(int argc, char ** argv) {
    // if(argc < 3){
    //     std::cerr << "Need an arg for num ents to create" << std::endl;
    //     return 1;
    // }
    // int n_entities = std::stoi(argv[1]);
    // int n_batches = std::stoi(argv[2]);

    // {
    // py::scoped_interpreter guard;
    // py::module_ mod = py::module_::import("zefdb");
    // py::gil_scoped_release release;

    // // std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // Graph g;

    // // std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // using clock = std::chrono::steady_clock;
    // using time = std::chrono::time_point<clock>;

    // time start = clock::now();
    // CALLGRIND_START_INSTRUMENTATION;
    // CALLGRIND_TOGGLE_COLLECT;
    // for(int batch = 0; batch < n_batches; batch++) {
    //     Transaction t{g};
    //     for(int i = 0; i < n_entities; i++) {
    //         auto z = instantiate(ET.Machine, g);
    //         // std::cerr << z << std::endl;
    //     }
    // }
    // CALLGRIND_TOGGLE_COLLECT;
    // CALLGRIND_STOP_INSTRUMENTATION;

    // time end = clock::now();

    // std::cout << "Time was " << double(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count()) / 1e6 << " ms" << std::endl;

    // std::cerr << "Final blob size is: " << g.my_graph_data().write_head.load() << std::endl;

    // // std::this_thread::sleep_for(std::chrono::milliseconds(60000));
    // // std::cerr << "Shutting down network manually first" << std::endl;
    // // auto butler = Butler::get_butler();
    // // butler->network.stop_running();

    // // std::cerr << "Shutting down butler" << std::endl;
    // // Butler::stop_butler();
    // std::cerr << "At end of pybind scope" << std::endl;
    // }
    // std::cerr << "After stopping pybind" << std::endl;

    {
    py::scoped_interpreter guard;
    py::module_ mod = py::module_::import("zef");
    py::gil_scoped_release release;

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    Graph g("c");

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    Graph g2{true};
    // Graph g2{};
    auto z2 = instantiate(ET.Machine, g2);
    tag(z2, "qweqwe");
    // sync(g2);

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));


    }
}
    