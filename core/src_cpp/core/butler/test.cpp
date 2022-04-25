
#include "include/msgqueue.h"
#include "zefDB.h"

#include <iostream>
#include <thread>
#include <chrono>
#include <cassert>

using namespace zefDB::Butler;
struct MSG {
    int n;
    int i;
    int val;
};

MessageQueue<MSG> queue;


constexpr int loop_n = 10000;
// constexpr int loop_n = 0;
constexpr int num_threads = 50;
// constexpr int num_threads = 4;
// constexpr int num_threads = 1;

void random_jobs(int n) {
    for (int i = 0; i < loop_n; i++) {
        int val = i % n;
        // std::cout << std::this_thread::get_id() << ": pushing " << val << std::endl;
        MSG * msg = new MSG{n, i, val};
        queue.push(msg);
    }
}

void reader() {
    int num_read = 0;
    MSG * msg = nullptr;
    while(num_read < loop_n*num_threads) {
        // std::this_thread::sleep_for(std::chrono::milliseconds(20) );
        
        while(queue.pop_any(msg)) {
            num_read++;
            if (msg->i % msg->n != msg->val)
                std::cerr << "Something went wrong" << std::endl;
            delete msg;
        }

        // std::cout << "Reader ran out: " << num_read << std::endl;
    }
    std::cout << "End of reader: " << num_read << std::endl;
}

int main(void) {

    std::cout << sizeof(std::atomic_int) << std::endl;
    std::cout << sizeof(int) << std::endl;

    std::cout << std::thread::id() << std::endl;
    // std::jthread t_worker[num_threads];
    std::thread t_worker[num_threads];
    for (int n = 0; n < num_threads; n++) {
        // t_worker[n] = std::jthread(random_jobs, n+5);
        t_worker[n] = std::thread(random_jobs, n+5);
    }
    // std::jthread t_reader(reader);
    std::thread t_reader(reader);

    t_reader.join();
    for (auto & t : t_worker) {
        t.join();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    std::cout << queue.num_messages << std::endl;
    for (auto & slot : queue.slots) {
        std::cout << slot << std::endl;
    }
    
    return 0;
}
