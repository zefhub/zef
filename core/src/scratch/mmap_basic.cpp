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

#include<iostream>
#include<sys/mman.h>
#include<stdio.h>
#include <fcntl.h>    /* For O_RDWR */
#include<stdlib.h>
#include<sys/stat.h>
#include<unistd.h>
#include<string>
#include <string.h>  // memcpy

constexpr auto print = [](const auto& x)->void { std::cout << x << std::endl; };
constexpr auto printt = [](const auto& v)->void { for (auto& el : v) std::cout << el << ", "; std::cout << std::endl; };





int main(int argc, char** argv){
    // int fd = open("/home/ulf/ulfs_mmap_demo1.txt", O_RDONLY, S_IRUSR | S_IWUSR);  // read only
    int fd = open("/home/ulf/ulfs_mmap_demo1.txt", O_RDWR, S_IRUSR | S_IWUSR);  // read only
    struct stat sb;

    if(fstat(fd, &sb) == -1){
        perror("could not get file size.\n");
    }
    print("file size is");
    print(sb.st_size);

    // void* file_in_memory = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    // void* file_in_memory = mmap(NULL, sb.st_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);   // with MAP_PRIVATE the changes are not written back to the system and the file
    
    // void* preferred_address = (void*)(0x7fffabcd);
    void* preferred_address = (void*)(0x7fff2cde7000);
    //auto ptr_prefer = static_cast<char*>(preferred_address);
    print("preferred address");
    print(preferred_address);

    void* file_in_memory = mmap(preferred_address, 50000, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    //void* file_in_memory = mmap(preferred_address, 50, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);   // the changes to the memory are not mapped back to the file and to other processes
    
    
    print("assigned at ");
    print(file_in_memory);

    auto ptrr = static_cast<char*>(file_in_memory); 
    memcpy(ptrr, "Naomi is a very hungry cat!!!!", 20);

    // memcpy(static_cast<char*>(file_in_memory), "Luna is a very hungry cat!!!!", 50);
    print(static_cast<char*>(file_in_memory));

    for(int ct = 0; ct<100; ct++)
        ptrr[ct] = 'z';

    close(fd);
    return 0;
}

