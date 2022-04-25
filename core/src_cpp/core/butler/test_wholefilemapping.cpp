#include "mmap.h"
#include "append_structures.h"
// #include "zefDB.h"

#include <iostream>
#include <thread>
#include <chrono>
#include <cassert>
#include <future>

using namespace zefDB;
    
int main(void) {

    for(int i = 0 ; i < 100 ; i++)
        std::cerr << std::string(make_random_uid()) << std::endl;
        

    int fd1 = open("danny1.txt", O_CREAT|O_TRUNC|O_RDWR, 0644);
    int fd2 = open("danny2.txt", O_CREAT|O_TRUNC|O_RDWR, 0644);
    int fd3 = open("danny3.txt", O_CREAT|O_TRUNC|O_RDWR, 0644);
    int fd4 = open("danny4.txt", O_CREAT|O_TRUNC|O_RDWR, 0644);

    size_t head1=0, head2=0, head3=0, head4=0;
    // std::cerr << fd1 << fd2 << fd3 << fd4 << std::endl;
    // MMap::WholeFileMapping<char> map(fd1, &head1);
    // MMap::WholeFileMapping<char> map2(fd2, &head2);
    // MMap::WholeFileMapping<char> map3(fd3, &head3);
    MMap::WholeFileMapping<AppendOnlySet<int>> map_ao(fd4, &head4);

    {
        auto p = map_ao.get_writer();
        p->append(5, p.ensure_func());
        std::cerr << p->contains(5) << std::endl;
        std::cerr << p->contains(7) << std::endl;
        p->append(9, p.ensure_func());
        p->append(5, p.ensure_func());
        p->append(50, p.ensure_func());
        p->append(500, p.ensure_func());
        p->append(5, p.ensure_func());
        p->append(1, p.ensure_func());

        std::cerr << std::endl;
        std::cerr << std::endl;
        std::cerr << p->size << std::endl;
        std::cerr << std::endl;
        std::cerr << std::endl;
    }

    {
        auto p = map_ao.get();
        std::cerr << p->contains(5) << std::endl;
        std::cerr << p->contains(7) << std::endl;
        std::cerr << p->size << std::endl;

        std::cerr << std::endl;
        std::cerr << std::endl;
        std::cerr << std::endl;
        for(const auto & item : *p) {
            std::cerr << item << std::endl;
        }

        std::vector<int> vec(p->begin(), p->end());
        for(auto & item : vec) {
            std::cerr << item << std::endl;
        }
    }
        

    // auto print = [&]() { 
    //     std::cerr << "Ptrs are " << map.map.ptr.load() << " " << map2.map.ptr.load() << " " << map3.map.ptr.load() << std::endl;
    // };

    // {
    //     auto p = map.get_writer();
    //     p.ensure_head(100);
    //     (*p) = 'a';

    //     p = map2.get_writer();
    //     p.ensure_head(100);
    //     (*p) = 'b';
    //     strcpy(&(*p), "asdf239487asdfjl234987asdfjk23498sdfkhn3489 the end");

    //     p = map3.get_writer();
    //     p.ensure_head(100);
    //     (*p) = 'c';

    //     char tmp[] = "this is some long text";
    //     std::cerr << strlen(tmp) << std::endl;
    //     memcpy(&(*p), tmp, strlen(tmp)+1);
    // }
    // print();

    // auto ensure_func = [](auto * map, int sec) {
    //     std::this_thread::sleep_for(std::chrono::seconds(1));
    //     auto p = map->get_writer();
    //     std::this_thread::sleep_for(std::chrono::seconds(sec));
    //     std::cerr << "Async about to ensure" << std::endl;
    //     p.ensure_head(1024*1024*1024);
    //     // p.ensure_head(1024);
    //     // p.ensure_head(1024*1024);
    //     std::cerr << "Async done ensure" << std::endl;
    // };
    // auto f = std::async(ensure_func, &map, 1);
    // auto f2 = std::async(ensure_func, &map2, 2);
    // // auto f3 = std::async(ensure_func, &map3, 3);
    // print(); 
    // std::cerr << "Main waiting with lock" << std::endl;

    // print(); 
    // {
    //     auto p = map.get();
    //     char * temp = &(*p);
    //     temp[2] = 6;
    //     std::this_thread::sleep_for(std::chrono::seconds(5));
    // }
    // print(); 
    // std::cerr << "Main done waiting" << std::endl;
    // print(); 

    // auto p = map.get();
    // auto p2 = map2.get();
    // auto p3 = map3.get();
    // std::cerr << "Final values are: " << std::endl;
    // std::cerr << (*p) << std::endl;
    // std::cerr << (*p2) << std::endl;
    // std::cerr << (*p3) << std::endl;
    // print(); 

    // close(fd1);
    // close(fd2);
    // close(fd3);
    
    return 0;
}
