
#include "include/msgqueue.h"
#include "include/communication.h"

#include <iostream>
#include <thread>
#include <chrono>
#include <cassert>

using namespace zefDB::Butler;
using json = nlohmann::json;
int main(void) {
    zefDB::communication::PersistentConnection conn{"http://localhost:5001"};
    conn.start_running();
    std::cerr << "conn created" << std::endl;

    conn.wait_for_connected(std::chrono::seconds(10));
    std::cerr << "conn connected" << std::endl;

    if(conn.connected) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        // for (auto msg : {"a","B","c","d","e","g"}) {
        //     conn.send(msg);
        // }
        auto msg = zefDB::communication::prepare_ZH_message({
                {"msg_type", "subscribe_to_graph"},
                {"protocol_type", "ZEFHUB"},
                {"graph_uid_or_tag", "danny-async-test"},
                {"task_uid", "123"},
            });
        std::cerr << msg << std::endl;
        std::cerr << zefDB::communication::decompress_zstd(msg) << std::endl;
        conn.send(msg);
                    
        std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    }

    return 0;
}
