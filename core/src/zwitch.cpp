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
